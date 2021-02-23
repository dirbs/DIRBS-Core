"""
Pairing list data import unit tests.

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

import pytest
from click.testing import CliRunner

from dirbs.importer.pairing_list_importer import PairingListImporter
from dirbs.cli.importer import cli as dirbs_import_cli
from _fixtures import *  # noqa: F403, F401
from _importer_params import PairListParams
from _helpers import get_importer, expect_success, expect_failure, data_file_to_test
from _delta_helpers import multiple_changes_check_common, \
    delta_remove_check_and_disable_option_common, \
    delta_add_same_entries_common, historic_threshold_config_common


def test_cli_pairing_list_importer(postgres, db_conn, tmpdir, mocked_config, logger):
    """Test Depot not available yet.

    Verify that the CLI import command for GSMA is working properly.
    """
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/pairing_list')
    valid_csv_pairing_list_data_file_name = 'sample_pairing_import_list_anonymized.csv'
    valid_csv_pairing_list_data_file = path.join(data_dir, valid_csv_pairing_list_data_file_name)

    # create a zip file inside a temp dir
    valid_zip_operator_data_file_path = str(tmpdir.join('sample_pairing_import_list_anonymized.zip'))
    with zipfile.ZipFile(valid_zip_operator_data_file_path, 'w') as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is the
        # archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_pairing_list_data_file,
                                                 valid_csv_pairing_list_data_file_name)

    runner = CliRunner()  # noqa
    result = runner.invoke(dirbs_import_cli, ['pairing_list', valid_zip_operator_data_file_path],
                           obj={'APP_CONFIG': mocked_config})

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT imei_norm, imsi FROM pairing_list ORDER BY imei_norm, imsi')
        res = {(res.imsi, res.imei_norm) for res in cursor.fetchall()}

    assert result.exit_code == 0
    assert len(res) == 3
    assert res == {('11108080805796', '35362602204562'),
                   ('11108951160476', '35412003020863'),
                   ('11108864102404', '35671206060150')}


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='pairing_list_insensitive_header.csv')],
                         indirect=True)
def test_case_insensitive_headers(pairing_list_importer, logger, db_conn):
    """Test Depot ID 96773/9.

    Verify that the pairing list file is accepted and imported if the headers have mixed cases.
    """
    expect_success(pairing_list_importer, 1, db_conn, logger)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='pairing_list_duplicate_record.csv')],
                         indirect=True)
def test_duplicate_check_override(pairing_list_importer, logger, db_conn):
    """Test Depot ID 96774/10.

    Verify that if duplicate records exist in the pairing list file,
    that only one record gets written to the database.
    """
    expect_success(pairing_list_importer, 1, db_conn, logger)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='pairing_list_duplicate_imei.csv')],
                         indirect=True)
def test_duplicate_imei(pairing_list_importer, logger, db_conn):
    """Test Depot ID 96775/1.

    Verify that if records with duplicate imeis exist in the pairing list file,
    that both records get written to the database.
    """
    expect_success(pairing_list_importer, 2, db_conn, logger)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='pairing_list_duplicate_imei_norm.csv')],
                         indirect=True)
def test_duplicate_imei_norm(pairing_list_importer, logger, db_conn):
    """Test Depot ID 96776/12.

    Verify that if records with duplicate normalized imeis exist in the pairing list file,
    that both records get written to the database.
    """
    expect_success(pairing_list_importer, 2, db_conn, logger)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='pairing_list_duplicate_imsi.csv')],
                         indirect=True)
def test_duplicate_imsi(pairing_list_importer, logger, db_conn):
    """Test Depot ID to be provided.

    Verify that if records with duplicate IMSI exist in the pairing list file,
    that both records get written to the database.
    """
    expect_success(pairing_list_importer, 2, db_conn, logger)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='pairing_list_empty_file.csv')],
                         indirect=True)
def test_empty_file(pairing_list_importer, logger, db_conn):
    """Test Depot ID 96779/15.

    Verify that an empty pairing list file cannot
    be imported into the database.
    """
    expect_success(pairing_list_importer, 0, db_conn, logger)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='pairing_list_empty_file_noheaders.csv')],
                         indirect=True)
def test_empty_file_no_headers(pairing_list_importer, logger):
    """Test Depot ID 96777/17.

    Verify that an empty pairing list file cannot be imported into the database.
    """
    expect_failure(pairing_list_importer, exc_message='metadata file is empty but '
                                                      'should contain at least a header\\nFAIL')


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='sample_pairinglist_invalid_imei.csv')],
                         indirect=True)
def test_invalid_imei_char(pairing_list_importer, logger):
    """Test Depot ID 96616/2.

    Verify that Pairing List data is checked for invalid IMEI(s) and is not
    imported into the database.
    """
    expect_failure(pairing_list_importer, exc_message='regex("^[0-9A-Fa-f\\\\*\\\\#]{1,16}$") fails '
                                                      'for line: 1, column: imei, value: '
                                                      '"InvalidIMEI"\\nFAIL')


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='sample_pairinglist_invalid_imsi.csv')],
                         indirect=True)
def test_invalid_imsi_char(pairing_list_importer, logger):
    """Test Depot ID 96617/3.

    Verify that Pairing List data is checked for invalid
    IMSI(s) and is not imported into the database.
    """
    expect_failure(pairing_list_importer, exc_message='regex("^[0-9]{1,15}$") fails for line: 1, '
                                                      'column: imsi, value: "&*^&*^*(&^"\\nFAIL')


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='pairing_list_missing_imei_column.csv')],
                         indirect=True)
def test_missing_imei_column(pairing_list_importer, logger):
    """Test Depot ID 96780/16.

    Verify that validation fails if the pairing list file is missing the IMEI Column.
    """
    expect_failure(pairing_list_importer)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='pairing_list_missing_imsi_column.csv')],
                         indirect=True)
def test_missing_imsi_column(pairing_list_importer, logger):
    """Test Depot ID 96782/17.

    Verify that validation fails if the pairing list file is missing the IMSI Column.
    """
    expect_failure(pairing_list_importer)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='pairing_list_out_of_order_columns.csv')],
                         indirect=True)
def test_out_of_order_columns(pairing_list_importer, logger):
    """Test Depot ID 96784/18.

    Verify that the pairing list file is rejected and not
    imported if it the headers are in the wrong order.
    """
    expect_failure(pairing_list_importer)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='sample_pairinglist.csv')],
                         indirect=True)
def test_normal_sample(pairing_list_importer, logger, db_conn):
    """Test Depot ID 96615/1.

    Verify that valid Paring List Data can be successfully imported into the database.
    """
    expect_success(pairing_list_importer, 5, db_conn, logger)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='sample_pairinglist.csv')],
                         indirect=True)
def test_historical_check_empty(pairing_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                mocked_config, tmpdir):
    """Test Depot ID 96662/5.

    Verify that a the pairing list data is not imported if it fails the historical check.
    """
    expect_success(pairing_list_importer, 5, db_conn, logger)
    # attempting to import empty pairing list file after having successfully imported 99 rows.
    with get_importer(PairingListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      PairListParams(filename='sample_pairinglist_historial_check.csv')) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='sample_pairinglist.csv')],
                         indirect=True)
def test_historical_check_percentage(pairing_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                     mocked_config, tmpdir):
    """Test Depot ID 96662/5.

    Verify that pairing list data is not imported if it fails the historical check.
    """
    expect_success(pairing_list_importer, 5, db_conn, logger)

    # size increased, the importer succeeds.
    with get_importer(PairingListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      PairListParams(filename='sample_pairing_list_large.csv',
                                     import_size_variation_percent=mocked_config.pairing_threshold_config.
                                     import_size_variation_percent,
                                     import_size_variation_absolute=mocked_config.pairing_threshold_config.
                                     import_size_variation_absolute)) as imp:
        expect_success(imp, 99, db_conn, logger)

    # importing file with drop in size greater than 5%
    with get_importer(PairingListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      PairListParams(filename=data_file_to_test(90, imei_imsi_msisdn=True),
                                     import_size_variation_percent=mocked_config.pairing_threshold_config.
                                     import_size_variation_percent,
                                     import_size_variation_absolute=mocked_config.pairing_threshold_config.
                                     import_size_variation_absolute)) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')

    # importing file with drop in size less than 5%
    with get_importer(PairingListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      PairListParams(filename=data_file_to_test(95, imei_imsi_msisdn=True),
                                     import_size_variation_percent=mocked_config.pairing_threshold_config.
                                     import_size_variation_percent,
                                     import_size_variation_absolute=mocked_config.pairing_threshold_config.
                                     import_size_variation_absolute)) as imp:
        expect_success(imp, 95, db_conn, logger)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='sample_pairinglist.csv')],
                         indirect=True)
def test_override_historical_check(pairing_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                   mocked_config, tmpdir):
    """Test Depot ID 96661/4.

    Verify that the user can override historical checks when importing pairing list data.
    """
    expect_success(pairing_list_importer, 5, db_conn, logger)

    with get_importer(PairingListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      PairListParams(filename='sample_pairinglist_historial_check.csv',
                                     perform_historic_check=False)) as imp:
        expect_success(imp, 0, db_conn, logger)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename=data_file_to_test(1000, imei_imsi_msisdn=True))],
                         indirect=True)
def test_historical_check_1000(pairing_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                               mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Historical pairing list check failed greater than 5% drop in import size 1000000 down to 98000.
    """
    expect_success(pairing_list_importer, 1000, db_conn, logger)
    with get_importer(PairingListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      PairListParams(filename=data_file_to_test(900, imei_imsi_msisdn=True))) as imp:
        expect_failure(imp)


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(content='imei,imsi,msisdn\n'
                                                 '123456789123456,,454444444444678')],
                         indirect=True)
def test_empty_imsi(pairing_list_importer):
    """Test Depot not known yet.

    Verify that Pairing List data is checked for empty IMSI(s) and is not
    imported into the database. This test has been added to verify that imsi column doesn't need to be trimmed because
    empty IMSI column will be refused by the validator.
    """
    expect_failure(pairing_list_importer, exc_message="Pre-validation failed: b\'Error:   "
                                                      'regex("^[0-9]{1,15}$") fails for line: 1, column: imsi, '
                                                      'value: ""\\nFAIL')


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(content='imei,imsi,msisdn,change_type\n'
                                                 '12345678901234,11111111111111,222222222222225,add\n'
                                                 '22345678901234,21111111111111,222222222222226,update',
                                         delta=True)],
                         indirect=True)
def test_delta_file_prevalidation(logger, db_conn, metadata_db_conn, mocked_config,
                                  tmpdir, mocked_statsd, pairing_list_importer):
    """Test Depot not available yet.

    Test pre-validation schemas.
    """
    # update change-type is allowed only for stolen
    expect_failure(pairing_list_importer,
                   exc_message='Pre-validation failed: b\'Error:   regex("^(add|remove)$") fails for line: 2, '
                               'column: change_type, value: "update"\\nFAIL')
    # change_type must be lower case
    with get_importer(PairingListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      PairListParams(content='imei,imsi,msisdn,change_type\n'
                                             '12345678901234,11111111111111,222222222222222,ADD',
                                     delta=True)) as imp:
        expect_failure(imp, exc_message='Pre-validation failed: b\'Error:   regex("^(add|remove)$") fails for line: '
                                        '1, column: change_type, value: "ADD"\\nFAIL')


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(filename='sample_pairinglist.csv')],
                         indirect=True)
def test_repeat_import(pairing_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                       mocked_config, tmpdir):
    """Test Depot not known yet. Test same import doesn't affect db."""
    expect_success(pairing_list_importer, 5, db_conn, logger)

    with get_importer(PairingListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      PairListParams(filename='sample_pairinglist.csv')) as imp:
        expect_success(imp, 5, db_conn, logger)


# used by all the following import tests
importer_name = 'pairing_list'
historic_tbl_name = 'historic_{0}'.format(importer_name)


# def test_delta_list_import(db_conn, mocked_config, tmpdir, logger):
#     """Test Depot not available yet. See _helpers::delta_list_import_common for doc."""
#     delta_list_import_common(db_conn, mocked_config, tmpdir, importer_name, historic_tbl_name, logger)


# def test_full_list_import(tmpdir, db_conn, mocked_config):
#     """Test Depot not available yet. See _helpers::test_full_list_import_common for doc."""
#     full_list_import_common(tmpdir, db_conn, mocked_config, importer_name, historic_tbl_name)


def test_multiple_changes_check(postgres, logger, mocked_config, tmpdir):
    """Test Depot not available yet. See _helpers::test_multiple_changes_check_common for doc."""
    multiple_changes_check_common(logger, mocked_config, tmpdir, importer_name)


def test_delta_remove_check_and_disable_option(postgres, db_conn, tmpdir, mocked_config, logger):
    """Test Depot not available yet. See _helpers::delta_remove_check_and_disable_option_common for doc."""
    delta_remove_check_and_disable_option_common(db_conn, historic_tbl_name, tmpdir, mocked_config, logger,
                                                 importer_name)


# def test_delta_add_check_and_disable_option(db_conn, tmpdir, mocked_config, logger):
#     """Test Depot not available yet. See _helpers::test_delta_add_check_and_disable_option_common for doc."""
#     delta_add_check_and_disable_option_common(db_conn, tmpdir, mocked_config, logger,
#                                               importer_name, historic_tbl_name)


def test_delta_add_same_entries(postgres, db_conn, tmpdir, mocked_config):
    """Test Depot not available yet. See _helpers::delta_add_same_entries_common for doc."""
    delta_add_same_entries_common(db_conn, tmpdir, mocked_config, importer_name, historic_tbl_name)


# def test_row_count_stats(postgres, db_conn, tmpdir, mocked_config, logger):
#     """Test Depot not available yet. See _helpers::row_count_stats_common for doc."""
#     row_count_stats_common(postgres, db_conn, tmpdir, mocked_config, logger, importer_name, historic_tbl_name)


def test_historic_threshold_config_cli(postgres, db_conn, tmpdir, mocked_config, logger, monkeypatch):
    """Test Depot not available yet. See _helpers::historic_threshold_config_common for doc."""
    historic_threshold_config_common(postgres, db_conn, tmpdir, mocked_config, logger, importer_name,
                                     historic_tbl_name, monkeypatch)
