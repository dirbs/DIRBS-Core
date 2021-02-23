"""
Operator data import unit tests.

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
import datetime

import pytest
from click.testing import CliRunner

from dirbs.cli.importer import cli as dirbs_import_cli
from dirbs.config.region import OperatorConfig
from dirbs.importer.operator_data_importer import OperatorDataImporter
from _helpers import get_importer, expect_success, expect_failure, logger_stream_contents
from _fixtures import *  # noqa: F403, F401
from _importer_params import OperatorDataParams, GSMADataParams


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_clean_20160701_20160731.csv',
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             perform_unclean_checks=False,
                             extract=False)],
                         indirect=True)
def test_operator_staging_data_with_invalid_data_flags_function(operator_data_importer, db_conn, logger):
    """Test Depot ID not known yet.

    Verify that is possible to find and debug invalid MNO data.
    A function operator_staging_data_with_invalid_data_flags can be used to list the invalid
    rows during a particular import. This requires that the import was run with the --no-cleanup option
    so that the staging table is persisted.
    The simply run operator_staging_data_with_invalid_data_flags(<import_id>);
    to list all import data with columns saying whether that row failed the NULL IMEI, IMSI, MSISDN checks,
    """
    # invalid row
    # date,imei,imsi,msisdn
    # 20160705,35555555555,41111111111111,22100222222222
    expect_success(operator_data_importer, 20, db_conn, logger)
    run_id = operator_data_importer.import_id
    with db_conn.cursor() as cursor:
        cursor.execute("""SELECT DISTINCT imei_norm
                            FROM operator_staging_data_with_invalid_data_flags(%s)
                           WHERE (is_null_imei
                              OR is_unclean_imei);""", [run_id])

        res = [(x.imei_norm) for x in cursor.fetchall()]
        assert res == ['35555555555']


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_clean3_20160701_20160731.csv',
                             operator='operator1',
                             perform_region_checks=False,
                             perform_null_checks=False,
                             perform_home_network_check=False,
                             extract=False)],
                         indirect=True)
def test_monthly_network_triplets_with_invalid_data_flags_view(operator_data_importer, logger, db_conn):
    """Test Depot not known yet.

    Verify that is possible to find and debug invalid MNO data.
    A view monthly_network_triplets_with_invalid_data_flags can be used to find invalid data that has been imported.
    This can be done on a per-operator, per_month basis.
    """
    # invalid triplets
    # imei_norm, imsi, msisdn, triplet_year, triplet_month, operator_id, is_null_imei, is_unclean_imei
    # [(None, '44444444444444', '22100444444444', 2016, 7, 'operator1', True, False),
    # (None, '41111111111111', '22100222222222', 2016, 7, 'operator1', True, False)]
    expect_success(operator_data_importer, 20, db_conn, logger)
    with db_conn.cursor() as cursor:
        cursor.execute("""SELECT imei_norm, imsi, msisdn
                            FROM monthly_network_triplets_with_invalid_data_flags
                           WHERE operator_id = 'operator1'
                             AND triplet_month = 7
                             AND triplet_year = 2016
                             AND (is_null_imei
                              OR is_unclean_imei);""")

        res = {(x.imei_norm, x.imsi, x.msisdn) for x in cursor.fetchall()}
        assert res == {(None, '44444444444444', '22100444444444'), (None, '41111111111111', '22100222222222')}


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_clean3_20160701_20160731.csv',
                             operator='operator1',
                             perform_region_checks=False,
                             perform_null_checks=False,
                             perform_home_network_check=False,
                             extract=False)],
                         indirect=True)
def test_daily_per_mno_hll_sketches(operator_data_importer, logger, db_conn):
    """Test Depot not known yet.

    Verify that is possible to use hll to count distinct identifiers.
    """
    expect_success(operator_data_importer, 20, db_conn, logger)
    with db_conn.cursor() as cursor:
        cursor.execute("""SELECT #hll_union_agg(triplet_hll) AS triplet_count,
                                 #hll_union_agg(imei_hll) AS imei_count,
                                 #hll_union_agg(imsi_hll) AS imsi_count,
                                 #hll_union_agg(msisdn_hll) AS msisdn_count,
                                 #hll_union_agg(imei_imsis_hll) AS imei_imsis_count,
                                 #hll_union_agg(imei_msisdns_hll) AS imei_msisdns_count,
                                 #hll_union_agg(imsi_msisdns_hll) AS imsi_msisdns_count
                            FROM daily_per_mno_hll_sketches""")
        res = {(x.triplet_count, x.imei_count, x.imsi_count, x.msisdn_count, x.imei_imsis_count,
                x.imei_msisdns_count, x.imsi_msisdns_count) for x in cursor.fetchall()}
        assert res == {(18, 15, 19, 19, 18, 18, 20)}


def test_operator_id_lower_case(mocked_config, logger, mocked_statsd, db_conn,
                                metadata_db_conn, tmpdir, postgres, monkeypatch):
    """Test Depot ID not known yet.

    Verify that the operator_id is always converted to lowercase either by get_importer helper function or
    cli _validate_operator_id (cli importers calls make_data_importer which uses mcc_mcn pair
    from OperatorConfig class).
    If the operator given as input param is not already lower case and has been converted
    verify that a warning message is generated.
    """
    # Step 1 check that operator_id is changed to lower case using get_importer helper function with a proper warning
    operator_dict = {'name': 'First Operator', 'id': 'OPERATOR1', 'mcc_mnc_pairs': [{'mnc': '01', 'mcc': '111'}]}
    oc = OperatorConfig(ignore_env=True, **operator_dict)
    monkeypatch.setattr(mocked_config.region_config, 'operators', [oc])

    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_clean_20160701_20160731.csv',
                          operator='OPERator1',
                          perform_region_checks=False,
                          perform_home_network_check=False,
                          perform_unclean_checks=False,
                          extract=False)) as new_imp:
        expect_success(new_imp, 20, db_conn, logger)

    assert 'OPERator1 has been changed to lower case: operator1' in logger_stream_contents(logger)

    # Step 2 check that operator_id is changed to lower case using cli importer with a proper warning
    runner = CliRunner()  # noqa

    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/operator')
    valid_csv_operator_data_file_name = 'operator1_20160701_20160731.csv'
    valid_csv_operator_data_file = path.join(data_dir, valid_csv_operator_data_file_name)

    # create a zip file inside a temp dir
    valid_zip_operator_data_file_path = \
        str(tmpdir.join('operator1_20160701_20160731.zip'))
    with zipfile.ZipFile(valid_zip_operator_data_file_path, 'w') as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is the
        # archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_operator_data_file, valid_csv_operator_data_file_name)

    db_conn.commit()
    op = {'id': 'OPERATOR1', 'name': 'First op', 'mcc_mnc_pairs': [{'mcc': '111', 'mnc': '01'}]}
    op_list = [OperatorConfig(ignore_env=True, **op)]
    monkeypatch.setattr(mocked_config.region_config, 'operators', op_list)
    result = runner.invoke(dirbs_import_cli, ['operator', 'Operator1', '--disable-rat-import',
                                              '--disable-region-check', '--disable-home-check',
                                              valid_zip_operator_data_file_path],
                           obj={'APP_CONFIG': mocked_config})

    assert result.exit_code == 0
    assert 'Operator1 has been changed to lower case: operator1' in logger_stream_contents(logger)

    # Step 3 OPERATOR1 comes from mocked_config_by_yaml, cli importer will use mcc_mcn pairs from config
    assert 'OPERATOR1 has been changed to lower case: operator1' in logger_stream_contents(logger)


def test_existing_operator_id_changed_and_db_not_empty(db_conn, logger, metadata_db_conn, mocked_config,
                                                       tmpdir, postgres, mocked_statsd):
    """Test Depot ID not known yet.

    Verify that there is no software exception during operator report if
    existing operator name gets changed and database not empty.
    """
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/operator')
    valid_csv_operator_data_file_name = 'operator1_20160701_20160731.csv'
    valid_csv_operator_data_file = path.join(data_dir, valid_csv_operator_data_file_name)

    # create a zip file inside a temp dir
    valid_zip_operator_data_file_path = \
        str(tmpdir.join('operator1_20160701_20160731.zip'))
    with zipfile.ZipFile(valid_zip_operator_data_file_path, 'w') as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is the
        # archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_operator_data_file, valid_csv_operator_data_file_name)

    runner = CliRunner()  # noqa
    result = runner.invoke(dirbs_import_cli, ['operator', 'OPErator1', '--disable-rat-import',
                                              '--disable-region-check', '--disable-home-check',
                                              valid_zip_operator_data_file_path],
                           obj={'APP_CONFIG': mocked_config})

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM network_imeis')
        assert result.exit_code == 0
        assert cursor.fetchone().count == 16

        result = runner.invoke(dirbs_import_cli, ['operator', 'operator1', '--disable-rat-import',
                                                  '--disable-region-check', '--disable-home-check',
                                                  valid_zip_operator_data_file_path],
                               obj={'APP_CONFIG': mocked_config})

        cursor.execute('SELECT COUNT(*) FROM network_imeis')
        assert result.exit_code == 0
        assert cursor.fetchone().count == 16


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn,rat\n'
                                     '20161101, 01376803870943,123456789012345,123456789012345,101',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             perform_leading_zero_check=False,
                             null_imei_threshold=0.8,
                             null_imsi_threshold=1,
                             null_msisdn_threshold=1,
                             null_rat_threshold=0.25,
                             perform_rat_import=True)],
                         indirect=True)
def test_postprocess_opt_fields_null(operator_data_importer, metadata_db_conn, logger, db_conn,
                                     mocked_config, tmpdir, mocked_statsd):
    """Test Depot ID not known yet.

    Verify that normalized IMEI, IMSI, MSISDN and RAT fields, decorated as optional,
    are trimmed and converted to null if blank in the postprocess phase.
    """
    # Step 1: expect prevalidator to fail to import IMEI with white-space at the start
    # Step 2: check that field are trimmed and if empty converted to null
    # data input file content containing blank fields:
    # 20160702,101322226982806,11101400135251,22300825684694,
    # 20160704,302666663700263,11101803062043,  ,102
    # 20160704,402777773700263,  ,22300049781840," "
    # 20160704,  ,11101803062043,22300049781840,001

    # Step 1:
    expect_failure(operator_data_importer,
                   exc_message='regex("^[0-9A-Fa-f\\\\*\\\\#]{1,16}$") fails '
                               'for line: 1, column: imei, value: " 01376803870943"\\nFAIL')
    # Step 2:
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_blank_optional_fields_20160701_20160731.csv',
                          perform_unclean_checks=False,
                          perform_region_checks=False,
                          perform_home_network_check=False,
                          perform_leading_zero_check=False,
                          null_imei_threshold=0.8,
                          null_imsi_threshold=1,
                          null_msisdn_threshold=1,
                          null_rat_threshold=0.50,
                          null_threshold=1.0,
                          perform_rat_import=True)) as new_imp:

        expect_success(new_imp, 4, db_conn, logger)

    with db_conn.cursor() as cursor:
        cursor.execute("""SELECT imei_norm, imsi, msisdn, seen_rat_bitmask AS rat
                            FROM network_imeis AS si
                            JOIN operator_data AS od
                           USING (imei_norm)
                        ORDER BY imei_norm, imsi;""")
        result = [(x.imei_norm, x.imsi, x.msisdn, x.rat) for x in cursor.fetchall()]
        assert result == [('10132222698280', '11101400135251', '22300825684694', None),
                          ('30266666370026', '11101803062043', None, 256),
                          ('40277777370026', None, '22300049781840', None)]


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_invalid_imei_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
def test_invalid_char_imei(operator_data_importer, logger):
    """Test Depot ID 96619/2.

    Verify that operator data is checked for invalid IMEI(s) and is not
    imported into the database.
    """
    expect_failure(operator_data_importer,
                   exc_message='regex("^[0-9A-Fa-f\\\\*\\\\#]{1,16}$") fails for line: '
                               '1, column: imei, value: "InvaldIMEI"\\nFAIL')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_largeimsi_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
def test_invalid_length_imsi(operator_data_importer):
    """Test Depot ID 96631/14.

    Verify that the operator data file is rejected if
    the file contains an IMSI>15 digits.
    """
    expect_failure(operator_data_importer,
                   exc_message='regex("^[0-9]{1,15}$") fails for line: 1, column: imsi, '
                               'value: "333333333333333333"\\nFAIL')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_invalid_imsi_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
def test_invalid_char_imsi(operator_data_importer):
    """Test Depot ID 96618/1.

    Verify that operator data is checked for invalid IMSI(s) and
    is not imported into the database.
    """
    expect_failure(operator_data_importer,
                   exc_message='regex("^[0-9]{1,15}$") fails for line: 2, column: imsi, value: "&*^&*^*(&^"\\nFAIL')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_20160701_20160731.csv',
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_valid_import(operator_data_importer, logger, db_conn):
    """Test Depot ID 96597/1.

    Verify that valid Operator Data can be successfully
    imported into the database.
    """
    expect_success(operator_data_importer, 19, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_invalid_msisdn_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
def test_invalid_msisdn_one(operator_data_importer):
    """Test Depot ID 96620/3.

    Verify that operator data is checked for
    invalid MSISDN(s) and is not imported into the database.
    """
    expect_failure(operator_data_importer,
                   exc_message='regex("^[0-9]{1,15}$") fails for line: 10, column: msisdn, value: '
                               '"InvalidMSIDN"\\nFAIL')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_invalid_msisdn2_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
def test_invalid_msisdn_two(operator_data_importer):
    """Test Depot ID 96620/3.

    Verify that operator data is checked for
    invalid MSISDN(s) and is not imported into the database.
    """
    expect_failure(operator_data_importer,
                   exc_message='regex("^[0-9]{1,15}$") fails for line: 10, column: msisdn, '
                               'value: "313113414AA1143"\\nFAIL')


# Right now duplicates count against the clean threshold
@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='detect_duplicate_20160203_20160203.csv',
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_detect_duplicates(operator_data_importer, logger, db_conn):
    """Test Depot ID 96633/16.

    Verify that if duplicate records exist in
    the operator data file, that only one record gets written to the database.
    """
    expect_success(operator_data_importer, 8, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_contains_null_imsis_msisdns_20160701_20160731.csv',
                             perform_null_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_detect_collisions(operator_data_importer, mocked_config, logger, mocked_statsd, db_conn,
                           metadata_db_conn, tmpdir):
    """Test Depot ID 96598/2.

    Verify that valid duplicate operator data is not imported into the database.
    """
    expect_success(operator_data_importer, 102, db_conn, logger)
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_contains_null_imsis_msisdns_20160701_20160731.csv',
                          perform_null_checks=False,
                          perform_region_checks=False,
                          perform_home_network_check=False)) as new_imp:
        expect_success(new_imp, 102, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_InvalidDate_20151224_20151225.csv',
                             extract=False)],
                         indirect=True)
def test_check_valid_dates_narrow_dates(operator_data_importer):
    """Test Depot ID 96601/5.

    Verify that Operator Data is not imported if the
    filename date is too narrow.
    """
    expect_failure(operator_data_importer,
                   exc_message='99 records are outside the date range supplied by the filename')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_malformeddate_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
def test_check_valid_dates_malformed_dates(operator_data_importer):
    """Test Depot ID 96601/5.

    Verify malformed date in the record data column.
    """
    expect_failure(operator_data_importer,
                   exc_message='fails for line: 2, column: date, value: "2016070"\\nFAIL')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_invaliddate_olderdatedrecords_20170101_20170131.csv',
                             extract=False)],
                         indirect=True)
def test_check_valid_dates_older_dated_records(operator_data_importer):
    """Test Depot ID 96787/24.

    Verify that Operator Data is not imported if the filename date range
    differs from the dates in the .CSV data file.  In this scenario, the records in the data file
    contain dates that are older than the filename date.
    """
    expect_failure(operator_data_importer,
                   exc_message='4 records are outside the date range supplied by the filename')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_invaliddate_20160401_20160131.csv',
                             extract=False)],
                         indirect=True)
def test_check_valid_dates_start_date_ahead(operator_data_importer):
    """Test Depot ID 96786/23.

    Verify that Operator Data is not imported if
    the filename date range differs from the dates in the .CSV data file.
    """
    expect_failure(operator_data_importer,
                   exc_message='Invalid filename - start date later than end date')


def test_check_valid_dates_end_date_future(db_conn, metadata_db_conn, mocked_config, logger, mocked_statsd, tmpdir):
    """Test Depot ID TBD.

    Verify that an operator data import fails if the end date is later that the system current
    date.
    """
    here = path.abspath(path.dirname(__file__))
    base_file = path.join(here, 'unittest_data/operator/operator1_normalizeIMEI_20160701_20160731.csv')
    sd_str = '20160701'
    ed1 = datetime.date.today()
    ed2 = ed1 + datetime.timedelta(days=1)
    ed1_str = ed1.strftime('%Y%m%d')
    ed2_str = ed2.strftime('%Y%m%d')
    filename1 = path.join(str(tmpdir), 'operator1_{0}_{1}.csv'.format(sd_str, ed1_str))
    filename2 = path.join(str(tmpdir), 'operator1_{0}_{1}.csv'.format(sd_str, ed2_str))
    with open(base_file, 'r') as input_file:
        with open(filename1, 'w') as of:
            of.write(input_file.read())
        input_file.seek(0)
        with open(filename2, 'w') as of:
            of.write(input_file.read())

    # Importing first file should work since end date is equal to the current date
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename=filename1,
                          perform_unclean_checks=False,
                          perform_region_checks=False,
                          perform_home_network_check=False)) as operator_data_importer:
        expect_success(operator_data_importer, 8, db_conn, logger)

    # Importing the second file should fail since the end date is in the future by one day
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename=filename2,
                          perform_unclean_checks=False,
                          perform_region_checks=False,
                          perform_home_network_check=False)) as operator_data_importer:
        expect_failure(operator_data_importer, exc_message='End date on operator data dump file is in the future')


# DIRBS-125
@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_normalizeIMEI_20160701_20160731.csv',
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_imei_normalization(operator_data_importer, logger, db_conn):
    """Test Depot ID 96600/4.

    Verify that the operator data IMEI fields are normalized.
    """
    expect_success(operator_data_importer, 8, db_conn, logger)
    with db_conn.cursor() as cursor:
        cursor.execute(
            'SELECT imei_norm FROM operator_data ORDER BY imei_norm')

        assert cursor.fetchone()[0] == '22345678901233'
        assert cursor.fetchone()[0] == '22345678901234'
        assert cursor.fetchone()[0] == '22A456789012345'
        assert cursor.fetchone()[0] == '23456789012345'
        assert cursor.fetchone()[0] == '35120030203334'
        assert cursor.fetchone()[0] == '52003020863612'
        assert cursor.fetchone()[0] == 'A1234'
        assert cursor.fetchone()[0] == 'AA'


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_blankimei_20160701_20160731.csv',
                             null_imei_threshold=1,
                             null_imsi_threshold=1,
                             null_msisdn_threshold=1,
                             null_threshold=1,
                             unclean_imei_threshold=0.3,
                             unclean_imsi_threshold=0.17,
                             unclean_threshold=0.3,
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_null_imei_not_filtered_out(operator_data_importer, logger, db_conn):
    """Test Depot ID 96628/11.

    Verify that the operator data file is accepted and
    imported if the data has a blank IMEI.
    """
    expect_success(operator_data_importer, 6, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_blankimsi_20160701_20160731.csv',
                             null_imei_threshold=1,
                             null_imsi_threshold=1,
                             null_msisdn_threshold=1,
                             null_threshold=1,
                             perform_null_checks=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_null_imsi_not_filtered_out(operator_data_importer, logger, db_conn):
    """Test Depot ID 96630/13.

    Verify that the operator data file is accepted and
    imported if the data has a blank IMSI.
    """
    expect_success(operator_data_importer, 6, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_blankimei_20160701_20160731.csv',
                             null_imei_threshold=0.2,
                             unclean_threshold=0.3,
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_null_imei_threshold_exceeded(operator_data_importer, logger):
    """Test Depot ID not known yet.

    Verify that the operator data file is not imported if the number of
    data has with blank IMEI is greater than null_imei_threshold.
    """
    # _data_length = 6
    # failing_null_check = 2
    # ratio = failing_null_check / self._data_length = 0.33
    # _null_imei_threshold = 0.2
    expect_failure(operator_data_importer,
                   exc_message='Failed NULL IMEI data threshold check, limit is: 0.20 and imported data has: 0.33')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_blankimsi_20160701_20160731.csv',
                             null_imei_threshold=0.2,
                             null_imsi_threshold=0.2,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_null_imsi_threshold_exceeded(operator_data_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that the operator data file is not imported if the number of
    data has with blank IMSI is greater than null_imsi_threshold.
    """
    # _data_length = 6
    # failing_null_check = 2
    # ratio = failing_null_check / self._data_length = 0.33
    # _null_imsi_threshold = 0.2
    expect_failure(operator_data_importer,
                   exc_message='Failed NULL IMSI data threshold check, limit is: 0.20 and imported data has: 0.33')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_blank_msisdn_20160701_20160731.csv',
                             null_imei_threshold=0.2,
                             null_imsi_threshold=0.2,
                             null_msisdn_threshold=0.2,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_null_msisdn_threshold_exceeded(operator_data_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that the operator data file is not imported if the number of
    data has with blank MSISDN is greater than null_msisdn_threshold.
    """
    # _data_length = 6
    # failing_null_check = 2
    # ratio = failing_null_check / self._data_length = 0.33
    # _null_msisdn_threshold = 0.2
    expect_failure(operator_data_importer,
                   exc_message='Failed NULL MSISDN data threshold check, limit is: 0.20 and imported data has: 0.33')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_blank_msisdn_20160701_20160731.csv',
                             null_imei_threshold=0.2,
                             null_imsi_threshold=0.2,
                             null_msisdn_threshold=0.35,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_null_combined_threshold_exceeded(operator_data_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that the operator data file is not imported if the number of
    data has with blank IMEI, IMSI or MSISDN is greater than null_threshold.
    """
    # _data_length = 6
    # failing_null_check = 2
    # ratio = failing_null_check / self._data_length = 0.33
    # _null_msisdn_threshold = 0.2
    expect_failure(operator_data_importer,
                   exc_message='Failed NULL data (combined) threshold check, '
                               'limit is: 0.05 and imported data has: 0.33')


def test_null_threshold_not_exceeded(logger, mocked_statsd, db_conn, metadata_db_conn, mocked_config, tmpdir):
    """Test Depot ID 96605/9.

    Verify that Operator data is checked for null entries and
    is successfully imported into the DB if the Null threshold is not exceeded.
    """
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_null2_20160701_20160730.csv',
                          null_imei_threshold=0.2,
                          null_imsi_threshold=0.2,
                          null_msisdn_threshold=0.2,
                          null_threshold=0.12,
                          perform_unclean_checks=False,
                          perform_region_checks=False,
                          perform_home_network_check=False)) as operator_data_importer:
        expect_success(operator_data_importer, 18, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_clean2_20160701_20160731.csv',
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             extract=False)],
                         indirect=True)
def test_unclean_threshold_not_exceeded(operator_data_importer, logger, db_conn):
    """Test Depot ID 96607/11.

    Verify that Operator data is checked for "unclean data” and is imported into the
    DB if the unclean threshold is not exceeded.
    """
    expect_success(operator_data_importer, 20, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_clean_20160701_20160731.csv',
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             unclean_imei_threshold=0.04,
                             extract=False)],
                         indirect=True)
def test_unclean_imei_threshold_exceeded(operator_data_importer):
    """Test Depot ID 96606/10.

    Verify that Operator data is checked for "unclean data” and is
    not imported into the DB if the unclean threshold is exceeded.
    """
    expect_failure(operator_data_importer, exc_message='Failed unclean IMEI data threshold check, '
                                                       'limit is: 0.04 and imported data has: 0.05')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_clean_20160701_20160731.csv',
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             unclean_imsi_threshold=0.04,
                             extract=False)],
                         indirect=True)
def test_unclean_imsi_threshold_exceeded(operator_data_importer):
    """Test Depot ID 96606/10.

    Verify that Operator data is checked for "unclean data” and is
    not imported into the DB if the unclean threshold is exceeded.
    """
    expect_failure(operator_data_importer, exc_message='Failed unclean IMSI data threshold check, '
                                                       'limit is: 0.04 and imported data has: 0.05')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_clean_20160701_20160731.csv',
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             extract=False)],
                         indirect=True)
def test_unclean_combined_threshold_exceeded(operator_data_importer):
    """Test Depot ID 96606/10.

    Verify that Operator data is checked for "unclean data” and is
    not imported into the DB if the unclean threshold is exceeded.
    """
    expect_failure(operator_data_importer, exc_message='Failed unclean data (combined) threshold check, '
                                                       'limit is: 0.05 and imported data has: 0.1')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_region_check_20160701_20160731.csv',
                             extract=False,
                             cc=['22'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '22'}, {'mcc': '311', 'mnc': '22'}])],
                         indirect=True)
def test_region_imsi_threshold_exceeded(operator_data_importer):
    """Test Depot ID 96608/12.

    Verify that Operator data is checked for “Region Data” and is not imported into
    the DB if the Region threshold is exceeded. Bad region msisdn.
    """
    # input data file 10 rows - 2 bad rows
    # 2 bad IMSI: 31008868818888, 41222222222222 with mcc not in ('111', '311').
    # region_threshold = 0.1
    expect_failure(operator_data_importer,
                   exc_message='Failed out-of-region IMSI data threshold check, limit is: 0.10 '
                               'and imported data has: 0.20')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_region_check_20160701_20160731.csv',
                             extract=False,
                             cc=['22'],
                             out_of_region_imsi_threshold=0.2,
                             out_of_region_msisdn_threshold=0.05,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '22'}, {'mcc': '311', 'mnc': '22'}])],
                         indirect=True)
def test_region_msisdn_threshold_exceeded(operator_data_importer):
    """Test Depot ID 96608/12.

    Verify that Operator data is checked for “Region Data” and is not imported into
    the DB if the Region threshold is exceeded. Bad region msisdn.
    """
    # input data file 10 rows - 1 bad rows
    # 1 bad MSISDN: 23109999999999 with invalid cc=23
    # region_threshold = 0.1
    expect_failure(operator_data_importer,
                   exc_message='Failed out-of-region MSISDN data threshold check, limit is: 0.05 '
                               'and imported data has: 0.10')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_region_check_20160701_20160731.csv',
                             extract=False,
                             cc=['22'],
                             out_of_region_imsi_threshold=0.2,
                             out_of_region_msisdn_threshold=0.1,
                             out_of_region_threshold=0.25,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '22'}, {'mcc': '311', 'mnc': '22'}])],
                         indirect=True)
def test_region_combined_threshold_exceeded(operator_data_importer):
    """Test Depot ID 96608/12.

    Verify that Operator data is checked for “Region Data” and is not imported into
    the DB if the Region threshold is exceeded. Bad region msisdn.
    """
    # input data file 10 rows - 3 bad rows
    # 1 bad MSISDN: 23109999999999 with invalid cc=23
    # 2 bad IMSI: 31008868818888, 41222222222222 with mcc not in ('111', '311').
    # region_threshold = 0.25 for combined
    expect_failure(operator_data_importer,
                   exc_message='Failed out-of-region data (combined) threshold check, limit is: 0.25 '
                               'and imported data has: 0.30')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_region_check_20160701_20160731.csv',
                             out_of_region_imsi_threshold=0.3,
                             out_of_region_msisdn_threshold=0.3,
                             out_of_region_threshold=0.3,
                             perform_home_network_check=False,
                             extract=False,
                             cc=['22'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '22'}, {'mcc': '311', 'mnc': '22'}])],
                         indirect=True)
def test_region_threshold_not_exceeded(operator_data_importer, logger, db_conn):
    """Test Depot ID 96609/13.

    Verify that Operator data is checked for “Region Data” and is
    imported into the DB if the Region threshold is not exceeded.
    """
    # input data file 10 rows - 3 bad rows
    # 2 bad IMSI: 31008868818888, 41222222222222 with mcc not in ('111', '311')
    # 1 bad MSISDN: 23109999999999 with invalid cc=23
    # region_threshold = 0.4
    expect_success(operator_data_importer, 10, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_home_check_exceeded_20160701_20160731.csv',
                             perform_region_checks=False,
                             non_home_network_threshold=0.1,
                             extract=False,
                             cc=['%'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '33'}])],
                         indirect=True)
def test_home_threshold_exceeded(operator_data_importer):
    """Test Depot ID 96608/12.

    Verify that Operator data is checked for “Home Data” and is not imported into
    the DB if the Home threshold is exceeded.
    """
    # Input data file contains 10 rows.
    # 3 rows contain bad IMSI:
    # 20160703, 344444444444444, 11145338688188, 22050011111111
    # 20160710, 356666666666666, 41222222222222, 22200333331333
    # 20160722, 359999999999999, 31145555555555, 22100555555555
    # for the pair {'mcc':'111', 'mnc':'33'}
    # default non_home_network_threshold= 0.2. Override non_home_network_threshold= 0.1
    expect_failure(operator_data_importer,
                   exc_message='Failed non-home network IMSI data threshold check, '
                               'limit is: 0.10 and imported data has: 0.30')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_home_check_exceeded_20160701_20160731.csv',
                             perform_region_checks=False,
                             non_home_network_threshold=0.3,
                             extract=False,
                             cc=['%'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '33'}])],
                         indirect=True)
def test_home_threshold_not_exceeded(db_conn, logger, operator_data_importer):
    """Test Depot ID 96608/12.

    Verify that Operator data is imported if the home network threshold is not exceeded
    """
    # Input data file contains 10 rows.
    # 3 rows contain bad IMSI:
    # 20160703, 344444444444444, 11145338688188, 22050011111111
    # 20160710, 356666666666666, 41222222222222, 22200333331333
    # 20160722, 359999999999999, 31145555555555, 22100555555555
    # for the pair {'mcc':'111', 'mnc':'33'}
    # default non_home_network_threshold= 0.2. Override non_home_network_threshold= 0.3
    expect_success(operator_data_importer, 10, db_conn, logger)


# DIRBS-335
@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_mixedcaseheaders_20160701_20160731.csv',
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_header_case_insensitivity(operator_data_importer, logger, db_conn):
    """Test Depot ID 96626/9.

    Verify that the operator data file is accepted
    and imported if the headers have mixed cases.
    """
    expect_success(operator_data_importer, 6, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_imeizerocheck1_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_small_imeicheck_2016.txt')],
                         indirect=True)
def test_detect_missing_leading_zero_one(operator_data_importer,
                                         gsma_tac_db_importer,
                                         logger,
                                         db_conn,
                                         tmpdir):
    """Test Depot ID 96603/7.

    Verify that Operator Data is not imported if the Leading Zero Check Fails.
    """
    gsma_tac_db_importer.import_data()
    expect_failure(operator_data_importer, exc_message='Failed leading zero check')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_imeizerocheck2_20160701_20160731.csv',
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '08'}],
                             extract=False)],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_small_imeicheck_2016.txt')],
                         indirect=True)
def test_detect_missing_leading_zero_two(operator_data_importer,
                                         gsma_tac_db_importer,
                                         mocked_config,
                                         logger,
                                         mocked_statsd,
                                         db_conn,
                                         metadata_db_conn,
                                         tmpdir):
    """Test Depot ID 96603/7.

    Verify that Operator Data is not imported if the Leading Zero Check Fails.
    """
    expect_failure(operator_data_importer, exc_message='Failed leading zero check')

    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_imeizerocheck3_20160701_20160731.csv',
                          perform_region_checks=False,
                          perform_home_network_check=False,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '08'}],
                          extract=False)) as new_imp:
        expect_success(new_imp, 6, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_20160701_20160731.csv',
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1')],
                         indirect=True)
def test_avg_distinct_imei(postgres, operator_data_importer, mocked_config, logger, mocked_statsd, db_conn,
                           metadata_db_conn, tmpdir):
    """Test Depot ID 96612/16.

    Verify that operator data is not imported when it fails to meet
    the historical checks.
    """
    expect_success(operator_data_importer, 19, db_conn, logger)

    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_historicalcheck_20161001_20161031.csv',
                          perform_region_checks=False,
                          perform_home_network_check=False,
                          operator='operator1')
                      ) as new_imp:
        expect_failure(new_imp, exc_message='Failed IMEI per day historic check')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_20160701_20160731.csv',
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1')],
                         indirect=True)
def test_avg_distinct_imsi(postgres, operator_data_importer, mocked_config, logger, mocked_statsd, db_conn,
                           metadata_db_conn, tmpdir):
    """Test Depot ID 96613/17.

    Verify that operator data is not imported when it fails to
    meet the historical checks.
    """
    expect_success(operator_data_importer, 19, db_conn, logger)

    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_historicalcheck_imsi_20161001_20161031.csv',
                          operator='operator1',
                          perform_region_checks=False,
                          perform_home_network_check=False,
                          historic_imei_threshold=0.0)) as new_imp:
        expect_failure(new_imp, exc_message='Failed IMSI per day historic check')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_20160701_20160731.csv',
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1')],
                         indirect=True)
def test_avg_distinct_msisdn(postgres, operator_data_importer, mocked_config, logger, mocked_statsd, db_conn,
                             metadata_db_conn, tmpdir):
    """Test Depot ID 96614/18.

    Verify that operator data is not imported when it
    fails to meet the historical checks.
    """
    expect_success(operator_data_importer, 19, db_conn, logger)

    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_historicalcheck_msisdn_20161001_20161031.csv',
                          historic_msisdn_threshold=0.6,
                          perform_region_checks=False,
                          perform_home_network_check=False,
                          historic_imei_threshold=0.0,
                          historic_imsi_threshold=0.0,
                          operator='operator1')) as new_imp:
        expect_failure(new_imp, exc_message='Failed MSISDN per day historic check')


def test_invalid_zip_upload(postgres, mocked_config):
    """Test Depot ID 96772/18.

    Test to check CSV file is not accepted as input from CLI.
    """
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/operator')
    valid_csv_operator_data_file_name = 'Foo_Wireless_20160101_20160331.csv'
    valid_csv_operator_data_file = path.join(data_dir, valid_csv_operator_data_file_name)

    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, ['operator', '--disable-clean-check',
                                              '--disable-region-check', 'operator1',
                                              valid_csv_operator_data_file],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exception
    assert 'does not have the correct file extension (.zip)' in result.output
    assert result.exit_code != 0


def test_cli_operator_importer(postgres, db_conn, mocked_config, logger, tmpdir):
    """Test Depot not available yet.

    Verify that the CLI import command for operator data is working properly.
    """
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/operator')
    valid_csv_operator_data_file_name = 'sample_operator_import_operator1_anonymized_20160701_20160731.csv'
    valid_csv_operator_data_file = path.join(data_dir, valid_csv_operator_data_file_name)

    # create a zip file inside a temp dir
    valid_zip_operator_data_file_path = \
        str(tmpdir.join('sample_operator_import_operator1_anonymized_20160701_20160731.zip'))
    with zipfile.ZipFile(valid_zip_operator_data_file_path, 'w') as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is the
        # archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_operator_data_file, valid_csv_operator_data_file_name)

    runner = CliRunner()  # noqa
    result = runner.invoke(dirbs_import_cli, ['operator', 'operator1', '--disable-rat-import',
                                              valid_zip_operator_data_file_path],
                           obj={'APP_CONFIG': mocked_config})

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT imei_norm FROM network_imeis ORDER BY imei_norm')
        res = [res.imei_norm for res in cursor.fetchall()]
        assert result.exit_code == 0
        assert res == ['38826033698280', '38847733370026']


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_invalidfilename.csv',
                             extract=False)],
                         indirect=True)
def test_invalid_filename_format(operator_data_importer):
    """Test Depot ID 96602/6.

    Verify that Operator Data is not imported if the filename format is invalid.
    """
    expect_failure(operator_data_importer,
                   exc_message='Invalid filename - must be in format <operator_id>_<YYYYMMDD>_<YYYYMMDD>.zip')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161122,01376803870943,123456789012345,123456789012345\n'
                                     '20161122,64220297727231,123456789012345,123456789012345\n'
                                     '20161121,64220299727231,125456789012345,123456789012345\n'
                                     '20161121,64220498727231,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
def test_last_seen_date_network_imeis(operator_data_importer, mocked_config, logger, mocked_statsd, db_conn,
                                      metadata_db_conn, tmpdir):
    """Verify that we store last_seen date for each IMEI in the network_imeis table."""
    # check that network_imeis contains 4 records with greatest date (20161122)
    expect_success(operator_data_importer, 4, db_conn, logger)

    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          content='date,imei,imsi,msisdn\n'
                                  '20161121,01376803870943,123456789012345,123456789012345\n'
                                  '20161121,64220297727231,123456789012345,123456789012345\n'
                                  '20161122,64220299727231,125456789012345,123456789012345\n'
                                  '20161122,64220498727231,123456789012345,123456789012345',
                          extract=False,
                          perform_unclean_checks=False,
                          perform_region_checks=False,
                          perform_home_network_check=False,
                          operator='operator1'
                      )) as new_imp:
        expect_success(new_imp, 8, db_conn, logger)

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT imei_norm, last_seen FROM network_imeis ORDER BY imei_norm')
        res = [(x.imei_norm, x.last_seen) for x in cursor.fetchall()]

    assert res == [('01376803870943', datetime.date(2016, 11, 22)),
                   ('64220297727231', datetime.date(2016, 11, 22)),
                   ('64220299727231', datetime.date(2016, 11, 22)),
                   ('64220498727231', datetime.date(2016, 11, 22))]


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_null_3_20160701_20160730.csv',
                             unclean_imsi_threshold=0.2,
                             unclean_threshold=0.2,
                             out_of_region_threshold=0.3,
                             perform_null_checks=False,
                             extract=False)],
                         indirect=True)
def test_check_override(operator_data_importer, mocked_config, logger, mocked_statsd, db_conn,
                        metadata_db_conn, tmpdir):
    """Test Depot ID 96610/14.

    Verify that the operator can override Null, Clean, Region Check,
    Leading Zero Check and historical checks when importing Operator Data files.
    """
    expect_success(operator_data_importer, 20, db_conn, logger)

    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_clean_20160701_20160731.csv',
                          unclean_threshold=0.1,
                          out_of_region_threshold=0.1,
                          cc=['22'],
                          mcc_mnc_pairs=[{'mcc': '%', 'mnc': '%'}],
                          perform_unclean_checks=False,
                          perform_historic_checks=False,
                          extract=False)) as new_imp:
        expect_success(new_imp, 40, db_conn, logger)

    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_region_20160701_20160731.csv',
                          unclean_threshold=0.1,
                          out_of_region_threshold=0.1,
                          perform_historic_checks=False,
                          cc=['22'],
                          mcc_mnc_pairs=[{'mcc': '%', 'mnc': '%'}],
                          extract=False)) as new_imp:
        expect_success(new_imp, 60, db_conn, logger)

    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_imeizerocheck1_20160701_20160731.csv',
                          unclean_imsi_threshold=0.2,
                          unclean_threshold=0.2,
                          out_of_region_threshold=0.1,
                          perform_historic_checks=False,
                          cc=['22'],
                          mcc_mnc_pairs=[{'mcc': '%', 'mnc': '%'}],
                          perform_leading_zero_check=False,
                          extract=False)) as new_imp:
        expect_success(new_imp, 66, db_conn, logger)

    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_20160701_20160731.csv',
                          unclean_threshold=0.1,
                          out_of_region_threshold=0.1,
                          perform_historic_checks=False,
                          cc=['22'],
                          mcc_mnc_pairs=[{'mcc': '%', 'mnc': '%'}],
                          extract=False)) as new_imp:
        expect_success(new_imp, 68, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_pipedelimited_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
def test_data_file_incorrect_delimiter(operator_data_importer):
    """Test Depot ID 96621/4.

    Verify that the operator data file is rejected
    and not imported if it is not "," delimited.
    """
    expect_failure(operator_data_importer,
                   exc_message='Metadata header, cannot find the column headers - imei, msisdn, date, '
                               'date|imei|imsi|msisdn, imsi - .\\nFAIL')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='no_csv_file_20160203_20160203.txt',
                             extract=False)],
                         indirect=True)
def test_non_csv_file(operator_data_importer):
    """Test Depot ID 96622/5.

    Verify that the operator data file is rejected and not imported if it is a .txt file.
    """
    expect_failure(operator_data_importer, exc_message='Wrong suffix for passed file')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_extracolumns_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
def test_incorrect_data_column(operator_data_importer):
    """Test Depot ID 96623/6.

    Verify that the operator data file is rejected and not
    imported if it does not contain the correct column data for each row.
    """
    expect_failure(operator_data_importer, exc_message='Expected @totalColumns of 4 and found 5 on line 1\\nFAIL')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_misorderedheaders_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
def test_data_headers_wrong_order(operator_data_importer):
    """Test Depot ID 96624/7.

    Verify that the operator data file is rejected and not imported
    if it the headers are in the wrong order.
    """
    expect_failure(operator_data_importer,
                   exc_message='Metadata header, cannot find the column headers -  - .\\nFAIL')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_missingheader_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
def test_missing_headers(operator_data_importer, logger):
    """Test Depot ID 96625/8.

    Verify that the operator data file is rejected and not imported if
    a header column is missing.
    """
    expect_failure(operator_data_importer,
                   exc_message='Metadata header, cannot find the column headers - imsi - .\\nFAIL')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_hexpound_20160701_20160731.csv',
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             extract=False)],
                         indirect=True)
def test_data_imei_invalid(operator_data_importer, db_conn, logger):
    """Test Depot ID 96629/12.

    Verify that the operator data file is accepted and imported
    if the data contains IMEIs with #, and *.
    """
    expect_success(operator_data_importer, 6, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='detect_duplicate_20160203_20160203.csv',
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             extract=False)],
                         indirect=True)
def test_duplicate_record(operator_data_importer, db_conn, logger):
    """Test Depot ID 96633/16.

    Verify that if duplicate records exist in the operator data file,
    that only one record gets written to the database.
    """
    expect_success(operator_data_importer, 8, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_with_rat_info_20160701_20160731.csv',
                             unclean_threshold=0.5,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             perform_rat_import=True)],
                         indirect=True)
def test_rat_import(operator_data_importer, logger, db_conn):
    """Test Depot ID unknown.

    Verify that the operator data RAT fields are imported correctly.
    """
    expect_success(operator_data_importer, 9, db_conn, logger)
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT seen_rat_bitmask FROM network_imeis WHERE imei_norm = \'01132222698280\'")
        assert cursor.fetchone()[0] == int('00000000000000000000000000000010', 2)

        cursor.execute("SELECT seen_rat_bitmask FROM network_imeis WHERE imei_norm = \'41255555638746\'")
        assert cursor.fetchone()[0] == int('00000000000000000001000010010000', 2)

        cursor.execute("SELECT seen_rat_bitmask FROM network_imeis WHERE imei_norm = \'41266666370026\'")
        assert cursor.fetchone()[0] == int('00000000000000000001100100011000', 2)

        cursor.execute("SELECT seen_rat_bitmask FROM network_imeis WHERE imei_norm = \'41277777370026\'")
        assert cursor.fetchone()[0] == int('00000000000000000000100000000000', 2)

        cursor.execute("SELECT seen_rat_bitmask FROM network_imeis WHERE imei_norm = \'41288888370026\'")
        assert cursor.fetchone()[0] == int('00000000000000000000001010000000', 2)

        cursor.execute("SELECT seen_rat_bitmask FROM network_imeis WHERE imei_norm = \'41299999370026\'")
        assert cursor.fetchone()[0] == int('00000000000000000000000000010000', 2)

        cursor.execute("SELECT seen_rat_bitmask FROM network_imeis WHERE imei_norm = \'41233333638746\'")
        assert cursor.fetchone()[0] == int('00000000000000000001000010010000', 2)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_invalid_rat_code_20160701_20160731.csv',
                             extract=False,
                             perform_rat_import=True)],
                         indirect=True)
def test_invalid_rat_code(operator_data_importer):
    """Test Depot ID unknown.

    Verify that the operator data file is rejected if
    the file contains an invalid RAT code outside the spec ranges.
    """
    expect_failure(operator_data_importer,
                   exc_message=' regex("^(00[1-7]|10[1-5])(\\\\|(00[1-7]|10[1-5]))*$") fails for line: 1, '
                               'column: rat, value: "123"\\nFAIL')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_invalid_rat_length_20160701_20160731.csv',
                             extract=False,
                             perform_rat_import=True)],
                         indirect=True)
def test_invalid_rat_length(operator_data_importer):
    """Test Depot ID unknown.

    Verify that the operator data file is rejected if
    the file contains an invalid RAT code length.
    """
    expect_failure(operator_data_importer,
                   exc_message=' regex("^(00[1-7]|10[1-5])(\\\\|(00[1-7]|10[1-5]))*$") fails for line: 1, '
                               'column: rat, value: "1234"\\nFAIL')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_20160701_20160731.csv',
                             extract=False,
                             out_of_region_imsi_threshold=1.0,
                             perform_home_network_check=False,
                             perform_msisdn_import=False)],
                         indirect=True)
def test_disable_msisdn_import(operator_data_importer, logger, db_conn):
    """Test Depot ID unknown.

    Verify that the MSISDN columns is not imported if MSISDN import is disabled.
    """
    expect_success(operator_data_importer, 19, db_conn, logger)

    # MSISDN should be NULL in every row
    with db_conn.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM operator_data WHERE msisdn IS NULL')
        result = cursor.fetchone()
        assert result[0] == 19


def test_disable_auto_analyze_check(mocked_config, logger, mocked_statsd, db_conn,
                                    metadata_db_conn, tmpdir, postgres, monkeypatch):
    """Verify that the disable-auto-analyze check disables the analyze command on associated tables."""
    # default case where auto analyze is enabled and no warning message appears
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='operator1_clean_20160701_20160731.csv',
                          operator='operator1',
                          perform_region_checks=False,
                          perform_home_network_check=False,
                          perform_unclean_checks=False,
                          extract=False)) as new_imp:
        expect_success(new_imp, 20, db_conn, logger)
    assert 'Skipping auto analyze of associated historic tables...' not in logger_stream_contents(logger)

    # disable-auto-analyze case where warning is streamed out
    runner = CliRunner()
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/operator')
    valid_csv_operator_data_file_name = 'operator1_20160701_20160731.csv'
    valid_csv_operator_data_file = path.join(data_dir, valid_csv_operator_data_file_name)

    # create a zip file inside a temp dir
    valid_zip_operator_data_file_path = \
        str(tmpdir.join('operator1_20160701_20160731.zip'))
    with zipfile.ZipFile(valid_zip_operator_data_file_path, 'w') as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is the
        # archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_operator_data_file, valid_csv_operator_data_file_name)

    db_conn.commit()
    result = runner.invoke(dirbs_import_cli, ['operator', 'operator1', '--disable-rat-import',
                                              '--disable-region-check', '--disable-home-check',
                                              '--disable-auto-analyze',
                                              valid_zip_operator_data_file_path],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    assert 'Skipping auto analyze of associated historic tables...' in logger_stream_contents(logger)
