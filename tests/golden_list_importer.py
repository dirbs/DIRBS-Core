"""
Golden data import unit tests.

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

from dirbs.importer.golden_list_importer import GoldenListImporter
from dirbs.cli.importer import cli as dirbs_import_cli
from _importer_params import GoldenListParams
from _fixtures import *  # noqa: F403, F401
from _helpers import get_importer, expect_success, expect_failure, data_file_to_test, imeis_md5_hashing_uuid
from _delta_helpers import full_list_import_common, multiple_changes_check_common, \
    delta_remove_check_and_disable_option_common, delta_add_check_and_disable_option_common, \
    delta_add_same_entries_common, delta_list_import_common, row_count_stats_common, historic_threshold_config_common


def test_cli_golden_list_importer(postgres, db_conn, tmpdir, mocked_config, logger):
    """Test Depot not available yet.

    Verify that the CLI import command for GSMA is working properly.
    """
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/golden_list')
    valid_csv_golden_list_data_file_name = 'sample_golden_import_list_anonymized.csv'
    valid_csv_golden_list_data_file = path.join(data_dir, valid_csv_golden_list_data_file_name)

    # create a zip file inside a temp dir
    valid_zip_operator_data_file_path = str(tmpdir.join('sample_golden_import_list_anonymized.zip'))
    with zipfile.ZipFile(valid_zip_operator_data_file_path, 'w') as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is the
        # archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_golden_list_data_file, valid_csv_golden_list_data_file_name)

    # Run dirbs-report
    runner = CliRunner()  # noqa
    result = runner.invoke(dirbs_import_cli, ['golden_list', valid_zip_operator_data_file_path],
                           obj={'APP_CONFIG': mocked_config})

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT hashed_imei_norm FROM golden_list ORDER BY hashed_imei_norm')
        result_list = [res.hashed_imei_norm for res in cursor]

    assert result.exit_code == 0
    assert result_list == ['14d7f294-462f-1847-bb3c-f2f271308684', '17186a62-8378-e3d2-7f17-0adefd81a1aa',
                           '5aa286a1-c3e6-cb88-ba86-047fba391f29', '7b00c461-1cac-79cd-b391-3a03ca5423b9',
                           'a6728112-3cfc-025a-3b99-5301f1ffe5ba', 'de9aa2e0-8df1-958b-bd0e-3bd9a4482476']


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(filename='golden_list_missingheader.csv')],
                         indirect=True)
def test_missing_header(golden_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that the Golden List data is not imported if a header column is missing.
    """
    expect_failure(golden_list_importer, exc_message='Metadata header, cannot find the column headers - '
                                                     'golden_imei, 642222222222222 - .\\nFAIL')


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(filename='sample_golden_list_normalize.csv')],
                         indirect=True)
def test_matching_normalisation(golden_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that IMEIs that normalize to the same value
    are successfully imported into the database.
    """
    expect_success(golden_list_importer, 5, db_conn, logger)


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(filename='sample_golden_list.csv')],
                         indirect=True)
def test_simple_import(golden_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that valid golden data can be successfully imported into the database.
    """
    expect_success(golden_list_importer, 20, db_conn, logger)


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(filename='sample_golden_list_duplicate.csv')],
                         indirect=True)
def test_duplicate_check_fails(golden_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that it doesn't fail to import file with same rows.
    """
    expect_success(golden_list_importer, 20, db_conn, logger)


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(filename='sample_golden_list_duplicate.csv')],
                         indirect=True)
def test_duplicate_check_override(golden_list_importer, db_conn, logger):
    """Test Depot ID not known yet.

    Verify that it fails to import an empty file after importing a non empty file.
    """
    expect_success(golden_list_importer, 20, db_conn, logger)


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(filename='sample_golden_list_v1.csv')],
                         indirect=True)
def test_repeat_import(golden_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn, mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that valid golden list data can be successfully imported into the database
    when repeating the import of the same file.
    """
    expect_success(golden_list_importer, 21, db_conn, logger)
    with get_importer(GoldenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GoldenListParams(filename='sample_golden_list_v1.csv')) as imp:
        expect_success(imp, 21, db_conn, logger)


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(filename='sample_golden_list.csv')],
                         indirect=True)
def test_historical_check_percentage_fails(golden_list_importer, logger, mocked_statsd,
                                           db_conn, metadata_db_conn, mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that a local golden data containing 9 rows fails to be imported after having imported a 20
    rows file because Historical golden list check is greater than 25% drop in import size;.
    """
    expect_success(golden_list_importer, 20, db_conn, logger)

    with get_importer(GoldenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GoldenListParams(filename='sample_golden_list_historicalcheck.csv',
                                       import_size_variation_percent=mocked_config.golden_threshold_config.
                                       import_size_variation_percent,
                                       import_size_variation_absolute=mocked_config.golden_threshold_config.
                                       import_size_variation_absolute)) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(filename=data_file_to_test(100, imei_imsi=False,
                                                                      imei_custom_header='golden_imei'))],
                         indirect=True)
def test_historical_check_percentage_succeeds(golden_list_importer, logger, mocked_statsd,
                                              db_conn, metadata_db_conn, mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that a local golden data is successfully imported after having imported two files where the
    second file has 80% size of the first one and the threshold value is 75.
    """
    expect_success(golden_list_importer, 100, db_conn, logger)

    with get_importer(GoldenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GoldenListParams(filename=data_file_to_test(80, imei_imsi=False,
                                                                  imei_custom_header='golden_imei'),
                                       import_size_variation_percent=mocked_config.golden_threshold_config.
                                       import_size_variation_percent,
                                       import_size_variation_absolute=mocked_config.golden_threshold_config.
                                       import_size_variation_absolute)) as imp:
        expect_success(imp, 80, db_conn, logger)


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(filename='sample_golden_list.csv')],
                         indirect=True)
def test_historical_check_empty(golden_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that it fails to import an empty file after importing a non empty file.
    """
    expect_success(golden_list_importer, 20, db_conn, logger)

    with get_importer(GoldenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GoldenListParams(filename='empty_goldenlist_historical_check.csv')) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(filename='golden_list_incorrectfiletype.txt')],
                         indirect=True)
def test_invalid_file_type(golden_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that Golden List data is not imported if the filename format is invalid.
    """
    expect_failure(golden_list_importer, exc_message='Wrong suffix')


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(filename='golden_list_hexpound.csv')],
                         indirect=True)
def test_malformed_imeis(golden_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                         mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that the Golden List data file is accepted
    and imported if the data contains IMEIs with #, and *.
    """
    expect_success(golden_list_importer, 20, db_conn, logger)

    # attempting to import golden list file containing symbol not allowed '%'.
    with get_importer(GoldenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GoldenListParams(filename='golden_list_hexpound_bad_symbol.csv')) as imp:
        expect_failure(imp, exc_message='regex("^[0-9A-Fa-f\\\\*\\\\#]{1,16}$") '
                                        'fails for line: 1, column: golden_imei, value: '
                                        '"62%222222222222"\\nFAIL')


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(content='GOLDEN_IMEI\n'
                                                   '64220204327947\n'
                                                   '12875502464321\n')],
                         indirect=True)
def test_import_data_hashing_option(golden_list_importer, db_conn, metadata_db_conn, mocked_config,
                                    logger, tmpdir, mocked_statsd):
    """Test Depot ID not known yet.

    Verify option to import either hashed or unhashed data.
    """
    # Step 1 import unhashed data
    # Step 2 import already hashed data
    imei_one_uuid = imeis_md5_hashing_uuid('64220204327947', convert_to_uuid=True)
    imei_two_uuid = imeis_md5_hashing_uuid('12875502464321', convert_to_uuid=True)

    # Step 1
    expect_success(golden_list_importer, 2, db_conn, logger)

    with db_conn.cursor() as cur:
        cur.execute('SELECT hashed_imei_norm FROM golden_list ORDER BY hashed_imei_norm')
        res = {x.hashed_imei_norm for x in cur.fetchall()}
        assert len(res) == 2

    assert res == {imei_one_uuid, imei_two_uuid}

    # Step 2
    imei_one_hashed = imeis_md5_hashing_uuid('64220204327947', convert_to_uuid=False)
    imei_two_hashed = imeis_md5_hashing_uuid('12875502464321', convert_to_uuid=False)

    with get_importer(GoldenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GoldenListParams(content='GOLDEN_IMEI\n{0}\n{1}\n'.format(imei_one_hashed, imei_two_hashed),
                                       prehashed_input_data=True)) as imp:
        expect_success(imp, 2, db_conn, logger)

    with db_conn.cursor() as cur:
        cur.execute('SELECT hashed_imei_norm FROM golden_list ORDER BY hashed_imei_norm')
        res = {x.hashed_imei_norm for x in cur.fetchall()}

    assert len(res) == 2
    assert res == {imei_one_uuid, imei_two_uuid}


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(content='GOLDEN_IMEI\n'
                                                   '64320204327947aa\n'
                                                   '14a20204327947\n')],
                         indirect=True)
def test_fields_normalised(golden_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that the data was imported hashed into the database after having been normalized per the
    following criteria:
    If the first 14 characters of the IMEI are digits, the normalised IMEI is the first 14 characters.
    If the imei does not start with 14 leading digits, no normalisation is done and we just copy the imei
    upper-case value to the imei_norm column.
    """
    expect_success(golden_list_importer, 2, db_conn, logger)

    first_imei_couple_normalized_uuid = imeis_md5_hashing_uuid('64320204327947', convert_to_uuid=True)
    second_imei_couple_normalized_uuid = imeis_md5_hashing_uuid('14A20204327947', convert_to_uuid=True)

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT DISTINCT hashed_imei_norm FROM golden_list ORDER BY hashed_imei_norm')
        res = [x.hashed_imei_norm for x in cursor.fetchall()]
        assert res == [first_imei_couple_normalized_uuid, second_imei_couple_normalized_uuid]


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(filename='sample_golden_list.csv')],
                         indirect=True)
def test_override_historical_check(golden_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                   mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that the user can override  historical checks when importing Golden List data.
    """
    expect_success(golden_list_importer, 20, db_conn, logger)
    with get_importer(GoldenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GoldenListParams(filename='sample_golden_list_historicalcheck.csv',
                                       perform_historic_check=False)) as imp:
        expect_success(imp, 9, db_conn, logger)


@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(content='golden_imei,change_type\n'
                                                   '12345678901234,add\n'
                                                   '22345678901234,update',
                                           delta=True)],
                         indirect=True)
def test_delta_file_prevalidation(logger, db_conn, metadata_db_conn, mocked_config,
                                  tmpdir, mocked_statsd, golden_list_importer):
    """Test Depot not available yet.

    Test pre-validation schemas.
    """
    # update change-type is allowed only for stolen
    expect_failure(golden_list_importer,
                   exc_message='Pre-validation failed: b\'Error:   regex("^(add|remove)$") fails for line: 2, '
                               'column: change_type, value: "update"\\nFAIL')
    # change_type must be lower case
    with get_importer(GoldenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GoldenListParams(content='golden_imei,change_type\n'
                                               '12345678901234,ADD',
                                       delta=True)) as imp:
        expect_failure(imp,
                       exc_message='Pre-validation failed: b\'Error:   regex("^(add|remove)$") fails for line: '
                                   '1, column: change_type, value: "ADD"\\nFAIL')


# used by all the following import tests
historic_tbl_name = 'historic_golden_list'
importer_name = 'golden_list'


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


def test_historic_threshold_config_cli(postgres, db_conn, tmpdir, mocked_config, logger, monkeypatch):
    """Test Depot not available yet. See _helpers::historic_threshold_config_common for doc."""
    historic_threshold_config_common(postgres, db_conn, tmpdir, mocked_config, logger, importer_name,
                                     historic_tbl_name, monkeypatch)
