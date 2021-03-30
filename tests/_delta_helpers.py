"""
Reusable test helpers for stolen, pairing, registration and golden list import tests.

Copyright (c) 2018-2021 Qualcomm Technologies, Inc.

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
limitations in the disclaimer below) provided that the following conditions are met:

- Redistributions of source code must retain the above copyright notice, this list of conditions and the following
  disclaimer.
- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
  disclaimer in the documentation and/or other materials provided with the distribution.
- Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
  products derived from this software without specific prior written permission.
- The origin of this software must not be misrepresented; you must not claim that you wrote the original software.
  If you use this software in a product, an acknowledgment is required by displaying the trademark/logo as per the
  details provided here: https://www.qualcomm.com/documents/dirbs-logo-and-brand-guidelines
- Altered source versions must be plainly marked as such, and must not be misrepresented as being the original
  software.
- This notice may not be removed or altered from any source distribution.

NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
"""
from datetime import datetime
import csv
import zipfile

from click.testing import CliRunner

from _helpers import insert_into_table, fetch_tbl_rows, imeis_md5_hashing_uuid, logger_stream_contents
from dirbs.cli.importer import cli as dirbs_import_cli


def historic_table_insert_params_from_dict(db_conn, importer_name, imei_norm_to_insert_list):
    """Helper function to populate historic table.

    Inserts values into the historic table correspondent to the current importer, given the importer name and the
    values to insert.
    i.e. importer_name=pairing_list; imei_norm_to_insert_list=['12345678901234','22345678901234']
    """
    # generate historic table name
    tbl_name = 'historic_{0}'.format(importer_name)
    # generate metadata dict for the current importer
    importer_metadata = importer_to_fields_dict[importer_name]
    normalized_imei_field_name = importer_metadata['normalized_imei_field_name']
    extra_field_names = importer_metadata['extra_field_names']
    # generate historic table field names list; end_date is already null by default.
    field_names_list = [normalized_imei_field_name]
    supports_imei_sharding = importer_metadata['supports_imei_sharding']
    if supports_imei_sharding:
        field_names_list.append('virt_imei_shard')
        with db_conn.cursor() as cursor:
            cursor.execute("""SELECT imei_norm, calc_virt_imei_shard(imei_norm) AS virt_imei_shard
                                FROM (SELECT UNNEST(%s)) imei_list(imei_norm)""",
                           [imei_norm_to_insert_list])
            virt_imei_shard_values = {res.imei_norm: res.virt_imei_shard for res in cursor}
    field_names_list.extend(extra_field_names)
    field_names_list.append('start_date')

    # add a default value for extra field if there is any
    extra_field_values = []
    extra_field_to_values = {'make': '',
                             'model': '',
                             'status': '',
                             'imsi': '11111111111111',
                             'msisdn': '',
                             'reporting_date': '20160420',
                             'model_number': '',
                             'brand_name': '',
                             'device_type': '',
                             'radio_interface': '',
                             'device_id': '123'}
    for fn in extra_field_names:
        assert fn in extra_field_to_values.keys()
        extra_field_values.append(extra_field_to_values[fn])

    # generate historic table field values list
    values_list = []
    for i in imei_norm_to_insert_list:
        values_row = [i]
        if supports_imei_sharding:
            values_row.append(virt_imei_shard_values[i])
        values_row.extend(extra_field_values)
        values_row.append('20160420')
        values_list.append(tuple(values_row))

    insert_into_table(db_conn, field_names_list, values_list, tbl_name)


def write_import_csv(tmpdir, importer_name, csv_imei_change_type_tuples, delta_import=True):
    """Helper function to create a csv input file.

    Takes in input the importer name to get metadata for csv header and a dict to map change_type to the imei_norm.
    i.e. csv_imei_change_type_tuples = [('52345678901234', 'remove'), ('22345678901234', 'add')];
         importer_name = pairing_list
    """
    valid_csv_import_data_file_name = 'sample_import_list.csv'
    valid_csv_import_data_file = str(tmpdir.join(valid_csv_import_data_file_name))
    with open(valid_csv_import_data_file, 'w') as csvfile:
        # generate metadata dict for the current importer
        importer_metadata = importer_to_fields_dict[importer_name]
        csv_imei_field_name = importer_metadata['csv_imei_field_name']
        extra_field_names = importer_metadata['extra_field_names']
        # add a default value for extra field if there is any

        # generate csv header (header is a list of fieldnames) i.e. [imei, imsi, change_type]
        csv_field_name_list = [csv_imei_field_name]
        csv_field_name_list.extend(extra_field_names)
        if delta_import:
            csv_field_name_list.append('change_type')
        # generate a list of csv rows to be written
        rows_to_write_list = []
        for imei_norm, change_type in csv_imei_change_type_tuples:
            row_dict = {csv_imei_field_name: imei_norm}
            extra_field_to_values = {'make': '',
                                     'model': '',
                                     'status': '',
                                     'imsi': '11111111111111',
                                     'msisdn': '2322211122112',
                                     'reporting_date': '20160420',
                                     'model_number': '',
                                     'brand_name': '',
                                     'device_type': '',
                                     'radio_interface': '',
                                     'device_id': '123'
                                     }
            for e in extra_field_names:
                row_dict.update({e: extra_field_to_values[e]})
            # in full_import mode change_type is Null
            if change_type:
                assert delta_import
                row_dict.update({'change_type': change_type})
            rows_to_write_list.append(row_dict)
            print(rows_to_write_list)
        # write into csv
        writer = csv.DictWriter(csvfile, fieldnames=csv_field_name_list)
        writer.writeheader()
        for d in rows_to_write_list:
            writer.writerow(d)

    valid_zip_import_data_file_path = str(tmpdir.join('sample_import_list.zip'))
    with zipfile.ZipFile(valid_zip_import_data_file_path, 'w') as valid_csv_import_data_file_zfile:
        valid_csv_import_data_file_zfile.write(valid_csv_import_data_file,
                                               valid_csv_import_data_file_name)
    return valid_zip_import_data_file_path


# importers common metadata
importer_to_fields_dict = {

    'golden_list': {'csv_imei_field_name': 'golden_imei',
                    'normalized_imei_field_name': 'hashed_imei_norm',
                    'extra_field_names': [],
                    'supports_imei_sharding': False},

    'pairing_list': {'csv_imei_field_name': 'imei',
                     'normalized_imei_field_name': 'imei_norm',
                     'extra_field_names': ['imsi', 'msisdn'],
                     'supports_imei_sharding': True},

    'stolen_list': {'csv_imei_field_name': 'imei',
                    'normalized_imei_field_name': 'imei_norm',
                    'extra_field_names': ['reporting_date', 'status'],
                    'supports_imei_sharding': True},

    'registration_list': {'csv_imei_field_name': 'approved_imei',
                          'normalized_imei_field_name': 'imei_norm',
                          'extra_field_names': ['make', 'model', 'status',
                                                'model_number', 'brand_name',
                                                'device_type', 'radio_interface', 'device_id'],
                          'supports_imei_sharding': True}
}


def delta_list_import_common(db_conn, mocked_config, tmpdir, importer_name, historic_tbl_name, logger):
    """Common code to test delta list import.

    1)  Verify import failure without using delta option because delta file pre-validation fails due to change_type col
    2)  Verify import succeeds using delta option
    2a) Verify that all record added have end_date None
    2b) Verify that record with imei_norm '52345678901234' has been removed and end_date set to not None.
    """
    imei_one = '12345678901234'
    imei_two = '22345678901234'
    imei_five = '52345678901234'
    imei_norm_pk_name = 'imei_norm'
    if importer_name == 'golden_list':
        imei_one = imeis_md5_hashing_uuid(imei_one, convert_to_uuid=True)
        imei_two = imeis_md5_hashing_uuid(imei_two, convert_to_uuid=True)
        imei_five = imeis_md5_hashing_uuid(imei_five, convert_to_uuid=True)
        imei_norm_pk_name = 'hashed_imei_norm'
    # populate historic_tbl
    imei_norm_to_insert_list = [imei_five]
    # write into csv
    csv_imei_change_type_tuples = [('52345678901234', 'remove'), ('12345678901234', 'add'),
                                   ('22345678901234', 'add')]
    # populate table
    historic_table_insert_params_from_dict(db_conn, importer_name, imei_norm_to_insert_list)
    # write csv
    valid_zip_import_data_file_path = write_import_csv(tmpdir, importer_name, csv_imei_change_type_tuples)
    runner = CliRunner()
    # Test part 1) Verify import failure without delta option
    result = runner.invoke(dirbs_import_cli, [importer_name, valid_zip_import_data_file_path],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1
    assert "Pre-validation failed: b\'Error:   " \
           'Metadata header, cannot find the column headers - change_type' in logger_stream_contents(logger)

    # Test part 2) Verify import success using delta option
    result = runner.invoke(dirbs_import_cli, [importer_name, '--delta', '--disable-delta-adds-check',
                                              valid_zip_import_data_file_path], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    count, res = fetch_tbl_rows(historic_tbl_name, db_conn)
    assert count == 3
    expected_output = {imei_one,
                       imei_two,
                       imei_five}
    imei_norm_set = {getattr(r, imei_norm_pk_name) for r in res}
    assert imei_norm_set == expected_output
    # Test part 2a
    assert all([(r.end_date is None) for r in res if getattr(r, imei_norm_pk_name) not in imei_five])
    # Test part 2b
    assert all([(r.end_date is not None) for r in res if getattr(r, imei_norm_pk_name) in imei_five])


def full_list_import_common(tmpdir, db_conn, mocked_config, importer_name, historic_tbl_name, delta_import=False):
    """Common code to test full list import.

    Import '52345678901234', '32345678901234' in db, with start_date '20160420' and end_date null.
    Import a full list containing '12345678901234', '22345678901234', '32345678901234'.
    1) Verify that 52345678901234 is removed(set end_date to not job date) because is only in db.
    2) Verify that 32345678901234 is ignored and remains the same (start_date '20160420') because is in both
    db and list.
    3) Verify that 12345678901234 and 22345678901234 are added with start date different from '20160420'
    and end date null because are in current list and not in db.
    """
    imei_one = '12345678901234'
    imei_two = '22345678901234'
    imei_three = '32345678901234'
    imei_five = '52345678901234'
    imei_norm_pk_name = 'imei_norm'
    if importer_name == 'golden_list':
        imei_one = imeis_md5_hashing_uuid(imei_one, convert_to_uuid=True)
        imei_two = imeis_md5_hashing_uuid(imei_two, convert_to_uuid=True)
        imei_three = imeis_md5_hashing_uuid(imei_three, convert_to_uuid=True)
        imei_five = imeis_md5_hashing_uuid(imei_five, convert_to_uuid=True)
        imei_norm_pk_name = 'hashed_imei_norm'

    # populate historic_tbl
    imei_norm_to_insert_list = [imei_five,
                                imei_three]
    # write into csv
    csv_imei_change_type_tuples = [('12345678901234', None), ('22345678901234', None),
                                   ('32345678901234', None)]
    # populate historic table
    historic_table_insert_params_from_dict(db_conn, importer_name, imei_norm_to_insert_list)
    # write csv input file
    valid_zip_import_data_file_path = write_import_csv(tmpdir, importer_name, csv_imei_change_type_tuples,
                                                       delta_import=delta_import)
    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, [importer_name, valid_zip_import_data_file_path],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    count, res = fetch_tbl_rows(historic_tbl_name, db_conn)
    assert count == 4
    expected_output = {imei_one,
                       imei_two,
                       imei_three,
                       imei_five}
    imei_norm_set = {getattr(r, imei_norm_pk_name) for r in res}
    assert imei_norm_set == expected_output
    date_time_before_import = datetime(2016, 4, 20)
    # Test part 1
    assert all([(r.end_date is not None) for r in res if getattr(r, imei_norm_pk_name) in imei_five])
    # Test part 2 and 3
    assert all([(r.end_date is None) for r in res if getattr(r, imei_norm_pk_name) in (imei_one, imei_two,
                                                                                       imei_three)])
    assert all([(r.start_date != date_time_before_import) for r in res if getattr(r, imei_norm_pk_name)
                in (imei_one, imei_two)])
    assert all([(r.start_date == date_time_before_import) for r in res if getattr(r, imei_norm_pk_name)
                not in (imei_one, imei_two)])


def multiple_changes_check_common(logger, mocked_config, tmpdir, importer_name):
    """Common code to verify that is not possible to add and remove a record at the same time."""
    # write into csv
    csv_imei_change_type_tuples = [('12345678901234', 'add'), ('12345678901234', 'remove')]
    valid_zip_import_data_file_path = write_import_csv(tmpdir, importer_name, csv_imei_change_type_tuples)
    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, [importer_name, '--delta', valid_zip_import_data_file_path],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1
    imei_norm_expected = '12345678901234'
    imei_norm_pk = 'imei_norm'
    if importer_name == 'golden_list':
        imei_norm_expected = imeis_md5_hashing_uuid(imei_norm_expected, convert_to_uuid=True)
        imei_norm_pk = 'hashed_imei_norm'
    assert 'Same record cannot be added or removed at the same time in delta list. ' \
           'Failing rows: {imei_norm_pk}: {imei_norm_expected}'.format(imei_norm_pk=imei_norm_pk,
                                                                       imei_norm_expected=imei_norm_expected) \
           in logger_stream_contents(logger)


def delta_remove_check_and_disable_option_common(db_conn, historic_tbl_name, tmpdir, mocked_config, logger,
                                                 importer_name):
    """Common code to verify delta remove check and CLI option to disable it.

    1) Verify that import fails if the check is enabled and the record to remove is not in db.
    2) Verify '--disable-delta-removes-check' CLI option and verify that, if disabled, the import succeeds.
    """
    # write into csv
    csv_imei_change_type_tuples = [('12345678901243', 'remove')]
    # Test part 1)
    valid_zip_import_data_file_path = write_import_csv(tmpdir, importer_name, csv_imei_change_type_tuples)
    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, [importer_name, '--delta', valid_zip_import_data_file_path],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1
    imei_norm_expected = '12345678901243'
    imei_norm_pk = 'imei_norm'
    if importer_name == 'golden_list':
        imei_norm_expected = imeis_md5_hashing_uuid(imei_norm_expected, convert_to_uuid=True)
        imei_norm_pk = 'hashed_imei_norm'
    assert 'Failed remove delta validation check. Cannot remove records not in db. ' \
           'Failing rows: {imei_norm_pk}: {imei_norm_expected}'.format(imei_norm_pk=imei_norm_pk,
                                                                       imei_norm_expected=imei_norm_expected) \
           in logger_stream_contents(logger)
    # Test part 2)
    result = runner.invoke(dirbs_import_cli, [importer_name, '--delta', '--disable-delta-removes-check',
                                              valid_zip_import_data_file_path], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    # historic_table rows
    count, res = fetch_tbl_rows(historic_tbl_name, db_conn)
    assert count == 0


def delta_add_check_and_disable_option_common(db_conn, tmpdir, mocked_config, logger, importer_name,
                                              historic_tbl_name):
    """Common code to verify delta add check and CLI option to disable check.

    1) Verify that import fails if the check is enabled and the record to add is already in db.
    2) Verify '--disable-delta-adds-check' CLI option and verify that, if disabled, the import succeeds.
    """
    imei_norm_one = '12345678901243'
    imei_norm_three = '32345678901234'
    imei_norm_pk = 'imei_norm'
    if importer_name == 'golden_list':
        imei_norm_one = imeis_md5_hashing_uuid(imei_norm_one, convert_to_uuid=True)
        imei_norm_three = imeis_md5_hashing_uuid(imei_norm_three, convert_to_uuid=True)
        imei_norm_pk = 'hashed_imei_norm'
    # populate historic_tbl
    imei_norm_to_insert_list = [imei_norm_one, imei_norm_three]
    # write into csv
    csv_imei_change_type_tuples = [('12345678901243', 'add')]
    # Test part 1)
    historic_table_insert_params_from_dict(db_conn, importer_name, imei_norm_to_insert_list)
    valid_zip_import_data_file_path = write_import_csv(tmpdir, importer_name, csv_imei_change_type_tuples)
    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, [importer_name, '--delta', valid_zip_import_data_file_path],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1
    assert 'Failed add delta validation check. Cannot add item that is already in db. ' \
           'Failing rows: {imei_norm_pk}: {imei_norm}'.format(imei_norm_pk=imei_norm_pk,
                                                              imei_norm=imei_norm_one) \
           in logger_stream_contents(logger)
    # historic_table rows
    count, res = fetch_tbl_rows(historic_tbl_name, db_conn)
    # only 2 rows that were inserted manually at the beginning of the test
    assert count == 2
    # Test part 2)
    result = runner.invoke(dirbs_import_cli, [importer_name, '--delta', '--disable-delta-adds-check',
                                              valid_zip_import_data_file_path], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    # historic_table rows
    count, res = fetch_tbl_rows(historic_tbl_name, db_conn)
    # only 2 rows that were inserted manually at the beginning of the test
    assert count == 2


def delta_add_same_entries_common(db_conn, tmpdir, mocked_config, importer_name, historic_tbl_name):
    """Common code to verify that is possible to add in delta list.

    An info message is generated and only the first row gets inserted into historic table.
    """
    # write into csv
    csv_imei_change_type_tuples = [('12345678901243', 'add'), ('12345678901243', 'add')]
    valid_zip_import_data_file_path = write_import_csv(tmpdir, importer_name, csv_imei_change_type_tuples)
    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, [importer_name, '--delta', valid_zip_import_data_file_path],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    count, res = fetch_tbl_rows(historic_tbl_name, db_conn)
    assert count == 1
    imei_one = '12345678901243'
    imei_norm_pk_name = 'imei_norm'
    if importer_name == 'golden_list':
        imei_one = imeis_md5_hashing_uuid(imei_one, convert_to_uuid=True)
        imei_norm_pk_name = 'hashed_imei_norm'
    imei_norm_set = {getattr(r, imei_norm_pk_name) for r in res}
    assert imei_norm_set == {imei_one}


def row_count_stats_common(postgres, db_conn, tmpdir, mocked_config, logger, importer_name, historic_tbl_name):
    """Test Depot not available yet. Verify output stats for CLI import command."""
    # Part 1) populate import_table and verify before import row count
    # Part 2) import file containing one duplicate (same row) and verify that staging_row_count value includes
    # duplicates and import_table_new_row_count doesn't.
    imei_norm_one = '12345678901243'
    imei_norm_two = '22345678901243'
    imei_norm_three = '32345678901234'
    imei_norm_four = '42345678901243'
    imei_norm_five = '52345678901243'
    imei_norm_pk = 'imei_norm'
    if importer_name == 'golden_list':
        imei_norm_one = imeis_md5_hashing_uuid(imei_norm_one, convert_to_uuid=True)
        imei_norm_two = imeis_md5_hashing_uuid(imei_norm_two, convert_to_uuid=True)
        imei_norm_three = imeis_md5_hashing_uuid(imei_norm_three, convert_to_uuid=True)
        imei_norm_four = imeis_md5_hashing_uuid(imei_norm_four, convert_to_uuid=True)
        imei_norm_five = imeis_md5_hashing_uuid(imei_norm_five, convert_to_uuid=True)
        imei_norm_pk = 'hashed_imei_norm'
    # populate historic_tbl
    imei_norm_to_insert_list = [imei_norm_one, imei_norm_three]
    # write into csv
    csv_imei_change_type_tuples = [('12345678901243', 'remove'),
                                   ('22345678901243', 'add'),
                                   ('22345678901243', 'add'),
                                   ('42345678901243', 'add'),
                                   ('52345678901243', 'add')]
    # Test part 1)
    historic_table_insert_params_from_dict(db_conn, importer_name, imei_norm_to_insert_list)
    valid_zip_import_data_file_path = write_import_csv(tmpdir, importer_name, csv_imei_change_type_tuples)
    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, [importer_name, '--delta', valid_zip_import_data_file_path],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    count, res = fetch_tbl_rows(historic_tbl_name, db_conn)
    assert count == 5
    imei_norm_set = {getattr(r, imei_norm_pk) for r in res}
    assert imei_norm_set == {imei_norm_one, imei_norm_two, imei_norm_three, imei_norm_four, imei_norm_five}
    # Test  Part 1)
    assert 'Rows in table prior to import: 2' in logger_stream_contents(logger)
    # Test  Part 2) - self.staging_row_count
    assert 'Rows supplied in delta input file: 5' in logger_stream_contents(logger)
    # Test  Part 2) - import_table_new_row_count=rows_before + rows_inserted - rows_deleted
    assert 'Rows in table after import: 4 (3 new, 0 updated, 1 removed)' in logger_stream_contents(logger)


def historic_threshold_config_common(postgres, db_conn, tmpdir, mocked_config, logger, importer_name,
                                     historic_tbl_name, monkeypatch):
    """Test Depot not available yet. Verify that historic treeshold is configurable by yaml."""
    historic_threshold_to_importers = {'pairing_list': 'pairing_threshold_config',
                                       'stolen_list': 'stolen_threshold_config',
                                       'registration_list': 'import_threshold_config',
                                       'golden_list': 'golden_threshold_config'}

    hist_thr = historic_threshold_to_importers.get(importer_name, '')
    monkeypatch.setattr(getattr(mocked_config, hist_thr, ''), 'import_size_variation_absolute', 0.3, raising=False)
    monkeypatch.setattr(getattr(mocked_config, hist_thr, ''), 'import_size_variation_percent', 0.3, raising=False)

    valid_zip_import_data_file_path = write_import_csv(tmpdir, importer_name, [('22345678901243', 'add')])
    runner = CliRunner()
    runner.invoke(dirbs_import_cli, [importer_name, '--delta', valid_zip_import_data_file_path],
                  obj={'APP_CONFIG': mocked_config})

    with db_conn, db_conn.cursor() as cur:
        cur.execute("""SELECT extra_metadata->'historic_size_variation_max_pct' AS historic_size_variation_max_pct,
                              extra_metadata->'historic_size_variation_max_abs' AS historic_size_variation_max_abs
                         FROM job_metadata
                        WHERE command = \'dirbs-import\'""")  # noqa Q444
        res = cur.fetchall()
        assert len(res) == 1
        res[0].historic_size_variation_max_pct == 0.3
        res[0].historic_size_variation_max_abs == 0.3
