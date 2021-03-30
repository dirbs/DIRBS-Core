"""
Subscribers list data import unit tests.

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

from dirbs.importer.subscriber_reg_list_importer import SubscribersListImporter
from dirbs.cli.importer import cli as dirbs_import_cli
from _fixtures import *  # noqa: F403, F401
from _importer_params import SubscribersListParams
from _helpers import get_importer, expect_success, expect_failure


def test_cli_subscribers_list_importer(postgres, db_conn, tmpdir, mocked_config, logger):
    """Verify that the CLI import command for subscribers list is working properly."""
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/subscriber_reg_list')
    valid_csv_subscribers_list_data_file_name = 'sample_subscribers_import_list_anonymized.csv'
    valid_csv_subscribers_list_data_file = path.join(data_dir, valid_csv_subscribers_list_data_file_name)

    # create a zip file inside temp dir
    valid_zip_subscribers_data_file_path = str(tmpdir.join('sample_subscribers_import_list_anonymized.zip'))
    with zipfile.ZipFile(valid_zip_subscribers_data_file_path, 'w') as valid_csv_data_zfile:
        valid_csv_data_zfile.write(valid_csv_subscribers_list_data_file,
                                   valid_csv_subscribers_list_data_file_name)

    cli_runner = CliRunner()
    result = cli_runner.invoke(dirbs_import_cli, ['subscribers_registration_list',
                                                  valid_zip_subscribers_data_file_path],
                               obj={'APP_CONFIG': mocked_config})

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT uid, imsi FROM subscribers_registration_list ORDER BY uid, imsi')  # noqa: Q440
        res = {(res.uid, res.imsi) for res in cursor.fetchall()}

    assert result.exit_code == 0
    assert len(res) == 3
    assert res == {('3536260220456285', '11108080805796'),
                   ('3541200302086361', '11108951160476'),
                   ('3567120606015081', '11108864102404')}


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='subscribers_list_insensitive_header.csv')],
                         indirect=True)
def test_case_insensitive_headers(subscribers_list_importer, logger, db_conn):
    """Verify that the subscribers list file is accepted and imported if the headers have mixed cases."""
    expect_success(subscribers_list_importer, 1, db_conn, logger)


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='subscribers_list_duplicate_record.csv')],
                         indirect=True)
def test_duplicate_check_override(subscribers_list_importer, logger, db_conn):
    """Verify that if duplicate record exists in the file, only one record gets written to the db."""
    expect_success(subscribers_list_importer, 1, db_conn, logger)


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='subscribers_list_duplicate_uid.csv')],
                         indirect=True)
def test_duplicate_uid(subscribers_list_importer, logger, db_conn):
    """Verify that if duplicate uid exists with different imsi, the both records gets written to the db."""
    expect_success(subscribers_list_importer, 2, db_conn, logger)


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='subscribers_list_duplicate_imsi.csv')],
                         indirect=True)
def test_duplicate_imsi(subscribers_list_importer, logger, db_conn):
    """Verify that if duplicate imsi exists with the two different uids the import fails.

    This to be taken care on the subsystem level as core doesn't know if anyone allows different uids with same imsi
    or not.
    """
    expect_success(subscribers_list_importer, 2, db_conn, logger)


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='subscribers_list_empty_file.csv')],
                         indirect=True)
def test_empty_file(subscribers_list_importer, logger, db_conn):
    """Verify that an empty file can be imported into the database."""
    expect_success(subscribers_list_importer, 0, db_conn, logger)


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='subscribers_list_empty_file_noheaders.csv')],
                         indirect=True)
def test_empty_file_no_header(subscribers_list_importer, logger):
    """Verify that an empty file with no headers cannot be imported to the database."""
    expect_failure(subscribers_list_importer, exc_message='metadata file is empty but '
                                                          'should contain at least a header\\nFAIL')


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='subscribers_list_invalid_uid.csv')],
                         indirect=True)
def test_invalid_uid_val(subscribers_list_importer, logger):
    """Verify that the subscribers list data is checked for invalid uid and is not imported."""
    expect_failure(subscribers_list_importer, exc_message='regex("^[0-9A-Za-z\\\\-\\\\\\\\]{1,20}$") fails '
                                                          'for line: 1, column: uid, value: "2333)3333&3333%3"\\nFAIL')


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='subscribers_list_invalid_imsi.csv')],
                         indirect=True)
def test_invalid_imsi_val(subscribers_list_importer, logger):
    """Verify that the subsribers list data is checked for invalid IMSI(s) and is not imported into the db."""
    expect_failure(subscribers_list_importer, exc_message='regex("^[0-9]{1,15}$") fails for line: 1, '
                                                          'column: imsi, value: "&*^&*^*(&^"\\nFAIL')


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='subscribers_list_missing_uid_column.csv')],
                         indirect=True)
def test_missing_uid_column(subscribers_list_importer, logger):
    """Verify that validation fails if the pairing list file is missing the UID column."""
    expect_failure(subscribers_list_importer)


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='subscribers_list_missing_imsi_column.csv')],
                         indirect=True)
def test_missing_imsi_column(subscribers_list_importer, logger):
    """Verify that validation fails if the pairing list file is missing the IMSI column."""
    expect_failure(subscribers_list_importer)


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='subscribers_list_out_of_order_columns.csv')],
                         indirect=True)
def test_out_of_order_column(subscribers_list_importer, logger):
    """Verify that the pairing list file is rejected and not imported if it the headers are in the wrong order."""
    expect_failure(subscribers_list_importer)


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='sample_subscriberslist.csv')],
                         indirect=True)
def test_normal_sample(subscribers_list_importer, logger, db_conn):
    """Varify that valid subscribers list data can be successfully imported into the database."""
    expect_success(subscribers_list_importer, 5, db_conn, logger)


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='sample_subscriberslist.csv')],
                         indirect=True)
def test_historical_check_empty(subscribers_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                mocked_config, tmpdir):
    """Verify that subscribers list data is not imported if it fails historical check."""
    expect_success(subscribers_list_importer, 5, db_conn, logger)

    # attempting to import empty subscribers list
    with get_importer(SubscribersListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      SubscribersListParams(filename='sample_subscriberslist_historial_check.csv')) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='sample_subscriberslist.csv')],
                         indirect=True)
def test_historical_check_percentage(subscribers_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                     mocked_config, tmpdir):
    """Verify that the subscribers list data is not imported if it fails the historical check."""
    expect_success(subscribers_list_importer, 5, db_conn, logger)

    # size increased, importer succeeds
    with get_importer(SubscribersListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      SubscribersListParams(filename='sample_subscribers_list_large.csv',
                                            import_size_variation_percent=mocked_config.subscribers_threshold_config.
                                            import_size_variation_percent,
                                            import_size_variation_absolute=mocked_config.subscribers_threshold_config.
                                            import_size_variation_absolute
                                            )) as imp:
        expect_success(imp, 99, db_conn, logger)

    # importing file with drop in size greater then 5%
    with get_importer(SubscribersListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      SubscribersListParams(filename='sample_subscribers_list_large_drop_5_percent_greater.csv',
                                            import_size_variation_percent=mocked_config.subscribers_threshold_config.
                                            import_size_variation_percent,
                                            import_size_variation_absolute=mocked_config.subscribers_threshold_config.
                                            import_size_variation_absolute
                                            )) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')

    # importing file with drop in size greater then 5%
    with get_importer(SubscribersListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      SubscribersListParams(filename='sample_subscribers_list_large_drop_5_percent_less.csv',
                                            import_size_variation_percent=mocked_config.subscribers_threshold_config.
                                            import_size_variation_percent,
                                            import_size_variation_absolute=mocked_config.subscribers_threshold_config.
                                            import_size_variation_absolute
                                            )) as imp:
        expect_success(imp, 95, db_conn, logger)


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='sample_subscriberslist.csv')],
                         indirect=True)
def test_override_historical_check(subscribers_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                   mocked_config, tmpdir):
    """Verify that the user can override historical checks when importing subscribers list data."""
    expect_success(subscribers_list_importer, 5, db_conn, logger)

    with get_importer(SubscribersListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      SubscribersListParams(filename='sample_subscriberslist_historial_check.csv',
                                            perform_historic_check=False)) as imp:
        expect_success(imp, 0, db_conn, logger)


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(content='uid,imsi\n'
                                                        ',12345678901234')],
                         indirect=True)
def test_empty_uid(subscribers_list_importer):
    """Verify that the Subscriber list data is checked for empty UID and is not imported."""
    expect_failure(subscribers_list_importer, exc_message='regex("^[0-9A-Za-z\\\\-\\\\\\\\]{1,20}$") fails '
                                                          'for line: 1, column: uid, value: ""\\nFAIL')


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(content='uid,imsi\n'
                                                        '123456789012345,')],
                         indirect=True)
def test_empty_imsi(subscribers_list_importer):
    """Verify that the Subscribers list data is check for empty IMSI(s) and is not imported."""
    expect_failure(subscribers_list_importer, exc_message="Pre-validation failed: b\'Error:   "
                                                          'regex("^[0-9]{1,15}$") fails for line: 1, column: imsi, '
                                                          'value: ""\\nFAIL')


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(content='uid,imsi,change_type\n'
                                                        '12345678901234,11111111111111,add\n'
                                                        '22345678901234,21111111111111,update',
                                                delta=True)],
                         indirect=True)
def test_delta_file_prevalidation(logger, db_conn, metadata_db_conn, mocked_config,
                                  tmpdir, mocked_statsd, subscribers_list_importer):
    """Test delta file pre-validation schemas."""
    # update change-type is not allowed here
    expect_failure(subscribers_list_importer,
                   exc_message='Pre-validation failed: b\'Error:   regex("^(add|remove)$") fails for line: 2, '
                               'column: change_type, value: "update"\\nFAIL')

    # change-type must be lower case
    with get_importer(SubscribersListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      SubscribersListParams(content='uid,imsi,change_type\n'
                                                    '12345678901234,11111111111111,ADD',
                                            delta=True)) as imp:
        expect_failure(imp, exc_message='Pre-validation failed: b\'Error:   regex("^(add|remove)$") fails for line: '
                                        '1, column: change_type, value: "ADD"\\nFAIL')


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(filename='sample_subscriberslist.csv')],
                         indirect=True)
def test_repeat_import(subscribers_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                       mocked_config, tmpdir):
    """Verify that same import doesn't affect db."""
    expect_success(subscribers_list_importer, 5, db_conn, logger)

    # importing same file
    with get_importer(SubscribersListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      SubscribersListParams(filename='sample_subscriberslist.csv')) as imp:
        expect_success(imp, 5, db_conn, logger)
