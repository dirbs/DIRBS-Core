"""
Registration list unit tests.

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

from dirbs.importer.registration_list_importer import RegistrationListImporter
from dirbs.cli.importer import cli as dirbs_import_cli
from _importer_params import RegistrationListParams
from _fixtures import *  # noqa: F403, F401
from _helpers import get_importer, expect_success, expect_failure, data_file_to_test
from _delta_helpers import full_list_import_common, multiple_changes_check_common, \
    delta_remove_check_and_disable_option_common, delta_add_check_and_disable_option_common, \
    delta_add_same_entries_common, delta_list_import_common, row_count_stats_common, historic_threshold_config_common


def test_cli_registration_list_importer(postgres, db_conn, tmpdir, mocked_config, logger):
    """Test Depot not available yet.

    Verify that the CLI import command for GSMA is working properly.
    """
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/registration_list')
    valid_csv_registration_list_data_file_name = 'sample_registration_list_import_anonymized.csv'
    valid_csv_registration_list_data_file = path.join(data_dir, valid_csv_registration_list_data_file_name)

    # create a zip file inside a temp dir
    valid_zip_gsma_data_file_path = str(tmpdir.join('sample_registration_list_import_anonymized.zip'))
    with zipfile.ZipFile(valid_zip_gsma_data_file_path, 'w') as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is
        # the archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_registration_list_data_file,
                                                 valid_csv_registration_list_data_file_name)

    runner = CliRunner()  # noqa
    result = runner.invoke(dirbs_import_cli, ['registration_list', valid_zip_gsma_data_file_path],
                           obj={'APP_CONFIG': mocked_config})

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT imei_norm FROM registration_list ORDER BY imei_norm')
        result_list = [res[0] for res in cursor.fetchall()]

    assert result.exit_code == 0
    assert result_list == ['10000000000000', '10000000000001', '10000000000002',
                           '10000000000003', '10000000000004', '10000000000005']


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='registration_list_missingheader.csv')],
                         indirect=True)
def test_header_case_insensitivity(registration_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that the Registration List data is not imported if a header column is missing.
    """
    expect_failure(registration_list_importer, exc_message='Metadata header, cannot find the column headers - '
                                                           '10000000000000, device_id, model, '
                                                           'device_type, model_number, '
                                                           'status, brand_name, make, approved_imei, '
                                                           'radio_interface - .\\nFAIL')


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list_normalize.csv')],
                         indirect=True)
def test_matching_normalisation(registration_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that IMEIs that normalize to the same value
    are successfully imported into the database.
    """
    expect_success(registration_list_importer, 5, db_conn, logger)


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list.csv')],
                         indirect=True)
def test_simple_import(registration_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that valid import data can be successfully imported into the database.
    """
    expect_success(registration_list_importer, 20, db_conn, logger)


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list_duplicate.csv')],
                         indirect=True)
def test_duplicate_check_fails(registration_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that it doesn't fail to import sam rows.
    """
    expect_success(registration_list_importer, 18, db_conn, logger)


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list_duplicate.csv')],
                         indirect=True)
def test_duplicate_check_override(registration_list_importer, db_conn, logger):
    """Test Depot ID not known yet.

    Verify that it succeeds to import a file containing duplicates if duplicate check is disabled.
    """
    # data file contains 21 rows and one duplicated IMEI
    expect_success(registration_list_importer, 18, db_conn, logger)


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list.csv')],
                         indirect=True)
def test_repeat_import(registration_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                       mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that valid registration_list data can be successfully imported into the database
    when repeating the import of the same file.
    """
    expect_success(registration_list_importer, 20, db_conn, logger)
    with get_importer(RegistrationListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      RegistrationListParams(filename='sample_registration_list.csv')) as imp:
        expect_success(imp, 20, db_conn, logger)


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list.csv')],
                         indirect=True)
def test_historical_check_percentage_fails(registration_list_importer, logger, mocked_statsd, db_conn,
                                           metadata_db_conn, mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that an import import data containing 9 rows fails to be imported after having imported a 20
    rows file because Historical registration_list check is greater than 25% drop in import size;.
    """
    expect_success(registration_list_importer, 20, db_conn, logger)

    with get_importer(RegistrationListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      RegistrationListParams(filename='sample_registration_list_historicalcheck.csv',
                                             import_size_variation_percent=mocked_config.import_threshold_config.
                                             import_size_variation_percent,
                                             import_size_variation_absolute=mocked_config.import_threshold_config.
                                             import_size_variation_absolute)) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename=data_file_to_test(100, imei_custom_header='approved_imei,'
                                                                                                    'make,model,'
                                                                                                    'status,'
                                                                                                    'model_number,'
                                                                                                    'brand_name,'
                                                                                                    'device_type,'
                                                                                                    'radio_interface,'
                                                                                                    'device_id')
                                                 )],
                         indirect=True)
def test_historical_check_percentage_succeeds(registration_list_importer, logger, mocked_statsd,
                                              db_conn, metadata_db_conn, mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that a local import data is successfully imported after having imported two files where the
    second file has 80% size of the first one and the threshold value is 75.
    """
    expect_success(registration_list_importer, 100, db_conn, logger)

    with get_importer(RegistrationListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      RegistrationListParams(filename=data_file_to_test(80,
                                                                        imei_custom_header='approved_imei,'
                                                                                           'make,model,'
                                                                                           'status,model_number,'
                                                                                           'brand_name,device_type,'
                                                                                           'radio_interface,'
                                                                                           'device_id'
                                                                        ),
                                             import_size_variation_percent=mocked_config.import_threshold_config.
                                             import_size_variation_percent,
                                             import_size_variation_absolute=mocked_config.import_threshold_config.
                                             import_size_variation_absolute)) as imp:
        expect_success(imp, 80, db_conn, logger)


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list.csv')],
                         indirect=True)
def test_historical_check_empty(registration_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that it fails to import an empty file after importing a non empty file.
    """
    expect_success(registration_list_importer, 20, db_conn, logger)

    with get_importer(RegistrationListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      RegistrationListParams(filename='empty_registration_list_historical_check.csv')) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='registration_list_incorrectfiletype.txt')],
                         indirect=True)
def test_invalid_file_type(registration_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that registration_list data is not imported if the filename format is invalid.
    """
    expect_failure(registration_list_importer, exc_message='Wrong suffix')


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='registration_list_hexpound.csv')],
                         indirect=True)
def test_malformed_imeis(registration_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                         mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that the registration_list data file is accepted
    and imported if the data contains IMEIs with #, and *.
    """
    expect_success(registration_list_importer, 20, db_conn, logger)

    # attempting to import registration_list file containing symbol not allowed '%'.
    with get_importer(RegistrationListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      RegistrationListParams(filename='registration_list_hexpound_bad_symbol.csv')) as imp:
        expect_failure(imp, exc_message='regex("^[0-9A-Fa-f\\\\*\\\\#]{1,16}$") '
                                        'fails for line: 1, column: approved_imei, value: '
                                        '"1000000%000000"\\nFAIL')


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list_normalize.csv')],
                         indirect=True)
def test_fields_normalised(registration_list_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that the data was imported into the database and normalized per the following criteria:
    If the first 14 characters of the IMEI are digits, the normalised IMEI is the first 14 characters.
    If the imei does not start with 14 leading digits, no normalisation is done and we just copy the imei
    value to the imei_norm column.
    """
    expect_success(registration_list_importer, 5, db_conn, logger)
    with db_conn.cursor() as cursor:
        cursor.execute('SELECT imei_norm FROM registration_list ORDER BY imei_norm')
        res = {x.imei_norm for x in cursor.fetchall()}
        assert res == {'1000011#000000', '1000012*000001', '10000130000002', '10000140000003', '10000150000004'}


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list.csv')],
                         indirect=True)
def test_override_historical_check(registration_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                   mocked_config, tmpdir):
    """Test Depot ID not known yet.

    Verify that the user can override  historical checks when importing registration_list data.
    """
    expect_success(registration_list_importer, 20, db_conn, logger)
    with get_importer(RegistrationListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      RegistrationListParams(filename='sample_registration_list_historicalcheck.csv',
                                             perform_historic_check=False)) as imp:
        expect_success(imp, 9, db_conn, logger)


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(content='approved_imei,make,model,status,'
                                                         'model_number,brand_name,device_type,'
                                                         'radio_interface,device_id\n'
                                                         '12345678901234,   ,   ,,,,,,123')],
                         indirect=True)
def test_optional_fields_whitespace(logger, db_conn, registration_list_importer):
    """Test Depot not available yet.

    Verified that if make and model columns are white-space only, they are trimmed and converted to null
    before storing into registration table.
    """
    expect_success(registration_list_importer, 1, db_conn, logger)
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute('SELECT imei_norm, make, model, status, model_number, brand_name, '
                       'device_type, radio_interface'
                       ' FROM registration_list ORDER BY imei_norm')
        assert [(x.imei_norm, x.make, x.model, x.status, x.model_number, x.brand_name,
                 x.device_type, x.radio_interface)
                for x in cursor.fetchall()] == [('12345678901234', None, None, None,
                                                 None, None, None, None)]


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(content='approved_imei,make,model,status,'
                                                         'model_number,brand_name,device_type,'
                                                         'radio_interface,device_id\n'
                                                         '12345678901234,,,,,,,,',
                                                 delta=False)],
                         indirect=True)
def test_device_id_required_field(logger, db_conn, metadata_db_conn, mocked_config,
                                  tmpdir, mocked_statsd, registration_list_importer):
    """Test Depot not available yet.

    Verify that the data in device_id is required.
    """
    with get_importer(RegistrationListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      RegistrationListParams(content='approved_imei,make,model,status,'
                                                     'model_number,brand_name,device_type,'
                                                     'radio_interface,device_id\n'
                                                     '12345678901234,,,,,,,,',
                                             delta=False)) as imp:
        expect_failure(imp,
                       exc_message='Pre-validation failed: b\'Error:   regex("[a-zA-Z0-9]+") fails for line: '
                                   '1, column: device_id, value: ""\\nFAIL')


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(content='approved_imei,make,model,status,'
                                                         'model_number,brand_name,device_type,'
                                                         'radio_interface,device_id,change_type\n'
                                                         '12345678901234,,,,,,,,123,add\n'
                                                         '22345678901234,,,,,,,,123,update',
                                                 delta=True)],
                         indirect=True)
def test_delta_file_prevalidation(logger, db_conn, metadata_db_conn, mocked_config,
                                  tmpdir, mocked_statsd, registration_list_importer):
    """Test Depot not available yet.

    Test pre-validation schemas.
    """
    # change_type must be lower case
    with get_importer(RegistrationListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      RegistrationListParams(content='approved_imei,make,model,status,'
                                                     'model_number,brand_name,device_type,'
                                                     'radio_interface,device_id,change_type\n'
                                                     '12345678901234,,,,,,,,123,ADD',
                                             delta=True)) as imp:
        expect_failure(imp,
                       exc_message='Pre-validation failed: b\'Error:   regex("^(add|remove|update)$") fails for line: '
                                   '1, column: change_type, value: "ADD"\\nFAIL')


# used by all the following import tests
importer_name = 'registration_list'
historic_tbl_name = 'historic_registration_list'


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
