"""
Stolen data import unit tests.

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

from os import path
import zipfile
from datetime import datetime

import pytest
from click.testing import CliRunner

from dirbs.importer.stolen_list_importer import StolenListImporter
from dirbs.cli.importer import cli as dirbs_import_cli
from _fixtures import *  # noqa: F403, F401
from _helpers import get_importer, expect_success, expect_failure, data_file_to_test, logger_stream_contents, \
    fetch_tbl_rows
from _importer_params import StolenListParams
from _delta_helpers import full_list_import_common, multiple_changes_check_common, \
    delta_remove_check_and_disable_option_common, write_import_csv, \
    delta_add_check_and_disable_option_common, delta_add_same_entries_common, delta_list_import_common, \
    historic_table_insert_params_from_dict, row_count_stats_common, historic_threshold_config_common


def test_cli_stolen_list_importer(postgres, db_conn, tmpdir, mocked_config, logger):
    """Test Depot not available yet.

    Verify that the CLI import command for Stolen List is working properly.
    """
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/stolen_list')
    valid_csv_stolen_list_data_file_name = 'sample_stolen_import_list_anonymized.csv'
    valid_csv_stolen_list_data_file = path.join(data_dir, valid_csv_stolen_list_data_file_name)

    # create a zip file inside a temp dir
    valid_zip_operator_data_file_path = str(tmpdir.join('sample_stolen_import_list_anonymized.zip'))
    with zipfile.ZipFile(valid_zip_operator_data_file_path, 'w') as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is the
        # archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_stolen_list_data_file, valid_csv_stolen_list_data_file_name)

    runner = CliRunner()  # noqa
    result = runner.invoke(dirbs_import_cli, ['stolen_list', valid_zip_operator_data_file_path],
                           obj={'APP_CONFIG': mocked_config})

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT imei_norm FROM stolen_list ORDER BY imei_norm')
        result_list = [res.imei_norm for res in cursor]

    assert result.exit_code == 0
    assert result_list == ['10000000000000', '10000000000001', '10000000000002',
                           '10000000000003', '10000000000004', '10000000000005']


# DIRBS-335
@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='stolen_list_missingheader.csv')],
                         indirect=True)
def test_missing_header(stolen_list_importer, logger, db_conn):
    """Test Depot ID 96588/2.

    Verify that the Stolen List data is not imported if a header column is missing.
    """
    expect_failure(stolen_list_importer, exc_message='cannot find the column headers - , 642222222222222, imei, '
                                                     'reporting_date, status')


# DIRBS-457
@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='sample_stolen_list_normalize.csv')],
                         indirect=True)
def test_matching_normalisation(stolen_list_importer, logger, db_conn):
    """Test Depot ID 96596/10.

    Verify that IMEIs that normalize to the same value
    are successfully imported into the database.
    """
    expect_success(stolen_list_importer, 5, db_conn, logger)


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='sample_stolen_list.csv')],
                         indirect=True)
def test_simple_import(stolen_list_importer, logger, db_conn):
    """Test Depot ID 96591/5.

    Verify that valid stolen data can be successfully imported into the database.
    """
    expect_success(stolen_list_importer, 20, db_conn, logger)


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(content='imei,reporting_date,status\n'
                                                   '622222222222222,20160426,  \n'
                                                   '122222222222223,20160425,  ')],
                         indirect=True)
def test_status_optional(stolen_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that status whitespace-only is converted to null.
    """
    expect_success(stolen_list_importer, 2, db_conn, logger)

    with db_conn, db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm, status FROM historic_stolen_list ORDER BY imei_norm')
        res = cur.fetchall()

    assert len(res) == 2
    stolen_list_rows = {(r.imei_norm, r.status) for r in res}
    assert stolen_list_rows == {('12222222222222', None), ('62222222222222', None)}


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(content='imei,reporting_date,status\n'
                                                   '622222222222222,20160426,\n'
                                                   '122222222222223,20160425,')],
                         indirect=True)
def test_reporting_date_optional(stolen_list_importer, logger, db_conn, mocked_config, metadata_db_conn,
                                 tmpdir, mocked_statsd):
    """Test Depot ID not known yet.

    Verify that valid stolen data can be successfully imported into the database with optional field reporting_date.
    Verify in case of two IMEIs with different reporting_date, only the one with min(reporting_date) is imported.
    """
    # Basically, the decorator @optional in the stolen_list schema for the field reporting-date, allows whitespaces.
    # If the field was text and we imported a row containing a reporting-date whitespace-only,
    # the row would have been imported with withespaces into the db.
    # In this case the reporting-date field is of type date so,
    # when we import a whitespace data of type text into a date type column, we get an Error Type
    # valid imei, reporting_date
    expect_success(stolen_list_importer, 2, db_conn, logger)

    with db_conn, db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm, reporting_date FROM historic_stolen_list ORDER BY imei_norm')
        res = cur.fetchall()

    assert len(res) == 2
    stolen_list_rows = {(r.imei_norm, r.reporting_date) for r in res}
    assert stolen_list_rows == {('12222222222222', datetime.date(datetime(2016, 4, 25))),
                                ('62222222222222', datetime.date(datetime(2016, 4, 26)))}

    # Verify in case of two IMEIs with different reporting_date, duplicate check will raise an exception.
    with get_importer(StolenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      StolenListParams(perform_historic_check=False,
                                       content='imei,reporting_date,status\n'
                                               '01234567891234,20160401,\n'
                                               '01234567891234,20160402,')) as imp:
        expect_failure(imp, 'Conflicting rows check failed (1 rows with same primary key and conflicting data)')
        assert "Found 1 conflicting row(s) with primary key (\'imei_norm\',): (\'01234567891234\',)" \
               in logger_stream_contents(logger)

    # valid imei, reporting_date whitespace only -  error reporting_date type is date not string
    with get_importer(StolenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      StolenListParams(content='imei,reporting_date,status\n'
                                               '622222222222222,    ,\n'
                                               '122222222222223,20160425,')) as imp:
        expect_failure(imp,
                       exc_message="Pre-validation failed: b\'Error:   "
                                   'regex("^(20[0-9]{2}((0[13578]|1[02])31|(01|0[3-9]|1[0-2])(29|30)|'
                                   '(0[1-9]|1[0-2])(0[1-9]|1[0-9]|2[0-8]))|20([02468][048]|[13579][26])0229)?$") '
                                   'fails for line: 1, column: reporting_date, value: "    "\\nFAIL')

    db_conn.commit()

    with db_conn, db_conn.cursor() as cur:
        cur.execute('TRUNCATE historic_stolen_list')

    # valid imei, reporting_date empty - type null
    with get_importer(StolenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      StolenListParams(content='imei,reporting_date,status\n'
                                               '722222222222222,,\n'
                                               '122222222222223,20160425,')) as imp:
        expect_success(imp, 2, db_conn, logger)

    with db_conn, db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm, reporting_date FROM historic_stolen_list ORDER BY imei_norm')
        res = cur.fetchall()

    assert len(res) == 2
    stolen_list_rows = {(r.imei_norm, r.reporting_date) for r in res}
    assert stolen_list_rows == {('12222222222222', datetime.date(datetime(2016, 4, 25))),
                                ('72222222222222', None)}


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='sample_stolen_list_duplicate.csv')],
                         indirect=True)
def test_duplicate_check_fails(stolen_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that rows with same imei and reporting_date are not considered duplicates.
    """
    expect_success(stolen_list_importer, 20, db_conn, logger)


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='sample_stolen_list_duplicate.csv')],
                         indirect=True)
def test_duplicate_check_override(stolen_list_importer, db_conn, logger):
    """Test Depot ID not known yet.

    Verify that it fails to import an empty file after importing a non empty file.
    """
    expect_success(stolen_list_importer, 20, db_conn, logger)


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='sample_stolen_list_v1.csv')],
                         indirect=True)
def test_repeat_import(stolen_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn, mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that valid stolen list data can be successfully imported into the database
    when repeating the import of the same file.
    """
    expect_success(stolen_list_importer, 21, db_conn, logger)
    with get_importer(StolenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      StolenListParams(filename='sample_stolen_list_v1.csv')) as imp:
        expect_success(imp, 21, db_conn, logger)


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='sample_stolen_list.csv')],
                         indirect=True)
def test_historical_check_percentage_fails(stolen_list_importer, logger, mocked_statsd, db_conn, mocked_config,
                                           metadata_db_conn, tmpdir):
    """Test Depot ID 96593/7.

    Verify that a local stolen data containing 9 rows fails to be imported after having imported a 20
    rows file because Historical stolen list check is greater than 25% drop in import size;.
    """
    expect_success(stolen_list_importer, 20, db_conn, logger)

    with get_importer(StolenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      StolenListParams(filename='sample_stolen_list_historicalcheck.csv',
                                       import_size_variation_percent=mocked_config.stolen_threshold_config.
                                       import_size_variation_percent,
                                       import_size_variation_absolute=mocked_config.stolen_threshold_config.
                                       import_size_variation_absolute)) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename=data_file_to_test(100,
                                                                      imei_custom_header='imei,reporting_date,'
                                                                                         'status',
                                                                      imei_imsi=False))],
                         indirect=True)
def test_historical_check_percentage_succeeds(stolen_list_importer, logger, mocked_statsd, db_conn, mocked_config,
                                              metadata_db_conn, tmpdir):
    """Test Depot ID not known yet.

    Verify that a local stolen data is successfully imported after having imported two files where the
    second file has 80% size of the first one and the threshold value is 75.
    """
    expect_success(stolen_list_importer, 100, db_conn, logger)

    with get_importer(StolenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      StolenListParams(filename=data_file_to_test(80, imei_custom_header='imei,reporting_date,'
                                                                                         'status',
                                                                  imei_imsi=False),
                                       import_size_variation_percent=mocked_config.stolen_threshold_config.
                                       import_size_variation_percent,
                                       import_size_variation_absolute=mocked_config.stolen_threshold_config.
                                       import_size_variation_absolute)) as imp:
        expect_success(imp, 80, db_conn, logger)


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='sample_stolen_list.csv')],
                         indirect=True)
def test_historical_check_empty(stolen_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that it fails to import an empty file after importing a non empty file.
    """
    expect_success(stolen_list_importer, 20, db_conn, logger)

    with get_importer(StolenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      StolenListParams(filename='empty_stolenlist_historical_check.csv')) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='stolen_list_incorrectfiletype.txt')],
                         indirect=True)
def test_invalid_file_type(stolen_list_importer):
    """Test Depot ID 96587/1.

    Verify that Stolen List data is not imported if the filename format is invalid.
    """
    expect_failure(stolen_list_importer, exc_message='Wrong suffix')


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='stolen_list_hexpound.csv')],
                         indirect=True)
def test_malformed_imeis(stolen_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                         mocked_config, tmpdir):
    """Test Depot ID 95690/4.

    Verify that the Stolen List data file is accepted
    and imported if the data contains IMEIs with #, and *.
    """
    expect_success(stolen_list_importer, 20, db_conn, logger)

    # attempting to import stolen list file containing symbol not allowed '%'.
    with get_importer(StolenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      StolenListParams(filename='stolen_list_hexpound_bad_symbol.csv')) as imp:
        expect_failure(imp, exc_message='regex("^[0-9A-Fa-f\\\\*\\\\#]{1,16}$") '
                                        'fails for line: 1, column: imei, value: '
                                        '"62%222222222222"\\nFAIL')


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='sample_stolen_list_normalize.csv')],
                         indirect=True)
def test_fields_normalised(stolen_list_importer, logger, db_conn):
    """Test Depot ID 96592/6.

    Verify that the data was imported into the database and normalized per the following criteria:
    If the first 14 characters of the IMEI are digits, the normalised IMEI is the first 14 characters.
    If the imei does not start with 14 leading digits, no normalisation is done and we just copy the imei
    value to the imei_norm column.
    """
    expect_success(stolen_list_importer, 5, db_conn, logger)
    with db_conn.cursor() as cursor:
        cursor.execute('SELECT imei_norm FROM stolen_list ORDER BY imei_norm')
        res = {x.imei_norm for x in cursor.fetchall()}
        assert len(res) == 5
        assert res == {'642222#2222222', '6433333*3333333', '64444444444444', '64555555555555', '64666666666666'}


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='sample_stolen_list.csv')],
                         indirect=True)
def test_override_historical_check(stolen_list_importer, logger, mocked_statsd, db_conn,
                                   metadata_db_conn, mocked_config, tmpdir):
    """Test Depot ID 96594/8.

    Verify that the user can override  historical checks when importing Stolen List data.
    """
    expect_success(stolen_list_importer, 20, db_conn, logger)
    with get_importer(StolenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      StolenListParams(filename='sample_stolen_list_historicalcheck.csv',
                                       perform_historic_check=False)) as imp:
        expect_success(imp, 9, db_conn, logger)


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(content='imei,reporting_date,status\n'
                                                   '622222222222222,20160420,\n'
                                                   '122222222222223,20160420,')],
                         indirect=True)
def test_update_existing_record(stolen_list_importer, logger, db_conn, mocked_config, metadata_db_conn,
                                tmpdir, mocked_statsd):
    """Test update existing record."""
    # populate historic table
    expect_success(stolen_list_importer, 2, db_conn, logger)

    # Verify that the update consisted in add + remove:
    # total rows in db = 4
    # two rows are added
    # new rows have end_date set to null and new reporting date '20160425'
    # existing rows have end_date set to job start time and old reporting date '20160420'
    with get_importer(StolenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      StolenListParams(content='imei,reporting_date,status\n'
                                               '62222222222222,20160425,\n'
                                               '122222222222223,20160425,')) as imp:
        expect_success(imp, 2, db_conn, logger)

    with db_conn, db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm, reporting_date, end_date FROM historic_stolen_list')
        res = cur.fetchall()
    assert len(res) == 4
    stolen_list_rows = {(r.imei_norm, r.reporting_date) for r in res}
    assert stolen_list_rows == {('12222222222222', datetime.date(datetime(2016, 4, 25))),
                                ('62222222222222', datetime.date(datetime(2016, 4, 25))),
                                ('12222222222222', datetime.date(datetime(2016, 4, 20))),
                                ('62222222222222', datetime.date(datetime(2016, 4, 20)))}
    # existing rows (with rep_date 20160420) will have end_date not null
    assert all([r.end_date is not None for r in res if r.reporting_date == datetime.date(datetime(2016, 4, 20))])
    # viceversa added rows (with rep_date 20160425) will have end_date null
    assert all([r.end_date is None for r in res if r.reporting_date == datetime.date(datetime(2016, 4, 25))])


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(content='imei,reporting_date,status,change_type\n'
                                                   '12345678901234,20000930,,ADD',
                                           delta=True,
                                           perform_delta_updates_check=False)],
                         indirect=True)
def test_delta_file_prevalidation(stolen_list_importer):
    """Test Depot not available yet.

    Test pre-validation schemas.
    """
    # change_type must be lower case
    expect_failure(stolen_list_importer,
                   exc_message='regex("^(add|remove|update)$") fails for line: 1, column: change_type, value: "ADD"')


# used by all the following import tests
importer_name = 'stolen_list'
historic_tbl_name = 'historic_stolen_list'


def test_delta_list_import(db_conn, mocked_config, tmpdir, logger):
    """Test Depot not available yet. See _helpers::delta_list_import_common for doc."""
    delta_list_import_common(db_conn, mocked_config, tmpdir, importer_name, historic_tbl_name, logger)


def test_full_list_import(tmpdir, db_conn, mocked_config):
    """Test Depot not available yet. See _helpers::test_full_list_import_common for doc."""
    full_list_import_common(tmpdir, db_conn, mocked_config, importer_name, historic_tbl_name)


def test_multiple_changes_check(postgres, logger, mocked_config, tmpdir):
    """Test Depot not available yet. See _helpers::test_multiple_changes_check_common for doc."""
    multiple_changes_check_common(logger, mocked_config, tmpdir, importer_name)


def test_delta_remove_check_and_disable_option(postgres, db_conn, tmpdir, mocked_config, logger):
    """Test Depot not available yet. See _helpers::delta_remove_check_and_disable_option_common for doc."""
    delta_remove_check_and_disable_option_common(db_conn, historic_tbl_name, tmpdir, mocked_config, logger,
                                                 importer_name)


def test_delta_add_check_and_disable_option(db_conn, tmpdir, mocked_config, logger):
    """Test Depot not available yet. See _helpers::test_delta_add_check_and_disable_option_common for doc."""
    delta_add_check_and_disable_option_common(db_conn, tmpdir, mocked_config, logger, importer_name, historic_tbl_name)


def test_delta_add_same_entries(postgres, db_conn, tmpdir, mocked_config):
    """Test Depot not available yet. See _helpers::delta_add_same_entries_common for doc."""
    delta_add_same_entries_common(db_conn, tmpdir, mocked_config, importer_name, historic_tbl_name)


def test_row_count_stats(postgres, db_conn, tmpdir, mocked_config, logger):
    """Test Depot not available yet. See _helpers::row_count_stats_common for doc."""
    row_count_stats_common(postgres, db_conn, tmpdir, mocked_config, logger, importer_name, historic_tbl_name)


def test_delta_update_check_and_disable_option(db_conn, tmpdir, mocked_config, logger):
    """Test Depot not available yet. Verify delta update check and CLI option to disable it.

    1) Verify that import fails if the check is enabled and the record to remove is not in db.
    2) Verify '--disable-delta-updates-check' CLI option and verify that, if disabled, the import succeeds.
    3) Verify that per each row updated a new row is added (and the existing row end_date is not null anymore)
    to historic_stolen_list table.
    """
    # Test part 1)
    csv_imei_change_type_tuples = [('22345678901234', 'update')]
    valid_zip_import_data_file_path = write_import_csv(tmpdir, importer_name, csv_imei_change_type_tuples)
    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, ['stolen_list', '--delta', '--disable-delta-removes-check',
                                              valid_zip_import_data_file_path],
                           obj={'APP_CONFIG': mocked_config}, catch_exceptions=False)
    assert result.exit_code == 1
    assert 'Failed update delta validation check. ' \
           'Cannot update records not in db. Failing rows: imei_norm: 22345678901234' in logger_stream_contents(logger)

    # Test part 2)
    result = runner.invoke(dirbs_import_cli, ['stolen_list', '--delta', '--disable-delta-updates-check',
                                              valid_zip_import_data_file_path],
                           catch_exceptions=False, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    # Test part 3)
    imei_one = '12345678901234'
    # populate historic_tbl
    imei_norm_to_insert_list = [imei_one]
    # write into csv
    csv_imei_change_type_tuples = [('12345678901234', 'update')]
    # populate table
    historic_table_insert_params_from_dict(db_conn, importer_name, imei_norm_to_insert_list)
    # write csv
    valid_zip_import_data_file_path = write_import_csv(tmpdir, importer_name, csv_imei_change_type_tuples)
    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, [importer_name, '--delta', valid_zip_import_data_file_path],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    count, res = fetch_tbl_rows(historic_tbl_name, db_conn)
    assert count == 3
    # verifying that there are 3 rows in historic_stolen_list table
    # one row for 22345678901234 added in step 1 and two rows for imei_norm 12345678901234 that has been updated.
    assert [r.imei_norm for r in res if r.end_date is None and r.imei_norm != '22345678901234'] == ['12345678901234']
    assert [r.imei_norm for r in res
            if r.end_date is not None and r.imei_norm != '22345678901234'] == ['12345678901234']


def test_historic_threshold_config_cli(postgres, db_conn, tmpdir, mocked_config, logger, monkeypatch):
    """Test Depot not available yet. See _helpers::historic_threshold_config_common for doc."""
    historic_threshold_config_common(postgres, db_conn, tmpdir, mocked_config, logger, importer_name,
                                     historic_tbl_name, monkeypatch)
