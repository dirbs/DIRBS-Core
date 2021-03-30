"""
Config parsing unit tests.

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

import os

from click.testing import CliRunner
import pytest

from dirbs.cli.catalog import cli as dirbs_catalog_cli
from dirbs.config import CatalogConfig
from _fixtures import *    # noqa: F403, F401
from _helpers import zip_files_to_tmpdir, logger_stream_contents


def test_all_files_are_harvested(postgres, db_conn, tmpdir, logger, monkeypatch, mocked_config):
    """Test all input files are correctly harvested and cataloged."""
    files_to_zip = ['unittest_data/operator/operator1_with_rat_info_20160701_20160731.csv',
                    'unittest_data/gsma/sample_gsma_import_list_anonymized.txt',
                    'unittest_data/stolen_list/sample_stolen_list.csv',
                    'unittest_data/registration_list/sample_registration_list.csv',
                    'unittest_data/pairing_list/sample_pairinglist.csv',
                    'unittest_data/golden_list/sample_golden_list.csv']
    zip_files_to_tmpdir(files_to_zip, tmpdir)
    catalog_config_dict = {
        'prospectors': [
            {
                'file_type': 'operator',
                'paths': [str(tmpdir.join('operator1_with_rat_info_20160701_20160731.zip'))],
                'schema_filename': 'OperatorImportSchema_v2.csvs'
            },
            {
                'file_type': 'gsma_tac',
                'paths': [str(tmpdir.join('sample_gsma_import_list_anonymized.zip'))],
                'schema_filename': 'GSMASchema.csvs'
            },
            {
                'file_type': 'stolen_list',
                'paths': [str(tmpdir.join('sample_stolen_list.zip'))],
                'schema_filename': 'StolenListSchema.csvs'
            },
            {
                'file_type': 'pairing_list',
                'paths': [str(tmpdir.join('sample_pairinglist.zip'))],
                'schema_filename': 'PairingListSchema.csvs'
            },
            {
                'file_type': 'registration_list',
                'paths': [str(tmpdir.join('sample_registration_list.zip'))],
                'schema_filename': 'RegistrationListSchema.csvs'
            },
            {
                'file_type': 'golden_list',
                'paths': [str(tmpdir.join('sample_golden_list.zip'))],
                'schema_filename': 'GoldenListSchemaData.csvs'
            }
        ],
        'perform_prevalidation': True
    }

    catalog_config = CatalogConfig(ignore_env=True, **catalog_config_dict)
    monkeypatch.setattr(mocked_config, 'catalog_config', catalog_config)

    # Run dirbs-catalog using db args from the temp postgres instance
    runner = CliRunner()

    # Run dirbs-catalog using db args from the temp postgres instance
    runner = CliRunner()
    result = runner.invoke(dirbs_catalog_cli, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT * FROM data_catalog')
        res = [(res.filename, res.file_type, res.compressed_size_bytes, res.is_valid_zip,
                res.is_valid_format, res.extra_attributes)
               for res in cursor.fetchall()]
        assert ('operator1_with_rat_info_20160701_20160731.zip', 'operator', 797, True, True,
                {'filename_check': True}) in res
        assert ('sample_gsma_import_list_anonymized.zip', 'gsma_tac', 1083, True, True, {}) in res
        assert ('sample_stolen_list.zip', 'stolen_list', 529, True, True, {}) in res
        assert ('sample_registration_list.zip', 'registration_list', 919, True, True, {}) in res
        assert ('sample_pairinglist.zip', 'pairing_list', 399, True, True, {}) in res
        assert ('sample_golden_list.zip', 'golden_list', 474, True, True, {}) in res

    # Run dirbs-catalog again to verify that no new files are discovered
    result = runner.invoke(dirbs_catalog_cli, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    assert 'Data catalog is already up-to-date!' in logger_stream_contents(logger)


def test_file_specified_explicitly_is_cataloged_correctly(postgres, db_conn, tmpdir, mocked_config, monkeypatch):
    """Test that if file is specified explicitly; it is pre-validated using the correct schema."""
    files_to_zip = ['unittest_data/operator/operator1_with_rat_info_20160701_20160731.csv']
    zip_files_to_tmpdir(files_to_zip, tmpdir)
    catalog_config_dict = {
        'prospectors': [
            {
                'file_type': 'operator',
                'paths': [str(tmpdir)],
                'schema_filename': 'OperatorImportSchema.csvs'
            },
            {
                'file_type': 'operator',
                'paths': [str(tmpdir.join('operator1_with_rat_info_20160701_20160731.zip'))],
                'schema_filename': 'OperatorImportSchema_v2.csvs'
            }
        ],
        'perform_prevalidation': True
    }

    catalog_config = CatalogConfig(ignore_env=True, **catalog_config_dict)
    monkeypatch.setattr(mocked_config, 'catalog_config', catalog_config)
    # Run dirbs-catalog using db args from the temp postgres instance
    runner = CliRunner()
    result = runner.invoke(dirbs_catalog_cli, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT is_valid_format FROM data_catalog WHERE filename = '
                       "\'operator1_with_rat_info_20160701_20160731.zip\'")
        assert cursor.fetchone().is_valid_format


def test_non_zip_files_are_not_harvested(postgres, db_conn, tmpdir, mocker, mocked_config, monkeypatch):
    """Test non-zip files are not cataloged."""
    catalog_config_dict = {
        'prospectors': [
            {
                'file_type': 'operator',
                'paths': [str(tmpdir)],
                'schema_filename': 'OperatorImportSchema.csvs'
            },
        ],
        'perform_prevalidation': False
    }

    catalog_config = CatalogConfig(ignore_env=True, **catalog_config_dict)
    monkeypatch.setattr(mocked_config, 'catalog_config', catalog_config)

    # Mock os.listdir call to return the unzipped test file
    mocker.patch.object(os, 'listdir', new_callable=mocker.MagicMock(
        return_value=[os.path.abspath(os.path.dirname(__file__)),
                      'unittest_data/operator/operator1_with_rat_info_20160701_20160731.csv']))

    # Run dirbs-catalog using db args from the temp postgres instance
    runner = CliRunner()
    result = runner.invoke(dirbs_catalog_cli, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM data_catalog WHERE filename = '
                       "\'operator1_with_rat_info_20160701_20160731.csv\'")
        assert cursor.fetchone()[0] == 0


def test_perform_prevalidation_option(postgres, db_conn, tmpdir, monkeypatch, mocked_config):
    """Test pre-validation is not performed if option is turned off in the config."""
    files_to_zip = ['unittest_data/operator/operator1_with_rat_info_20160701_20160731.csv']
    zip_files_to_tmpdir(files_to_zip, tmpdir)
    catalog_config_dict = {
        'prospectors': [
            {
                'file_type': 'operator',
                'paths': [str(tmpdir.join('operator1_with_rat_info_20160701_20160731.zip'))],
                'schema_filename': 'OperatorImportSchema_v2.csvs'
            }
        ],
        'perform_prevalidation': False
    }

    catalog_config = CatalogConfig(ignore_env=True, **catalog_config_dict)
    monkeypatch.setattr(mocked_config, 'catalog_config', catalog_config)
    # Run dirbs-catalog using db args from the temp postgres instance
    runner = CliRunner()
    result = runner.invoke(dirbs_catalog_cli, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    # This test basically checks that when pre-validation is disabled, then it is skipped during catalog.
    # The is_valid_format field would be NULL is that scenario as tested below. The scenario with
    # pre-validation enabled is implicitly tested in test_all_files_are_harvested test case.
    with db_conn.cursor() as cursor:
        cursor.execute('SELECT is_valid_format FROM data_catalog WHERE filename = '
                       "\'operator1_with_rat_info_20160701_20160731.zip\'")
        assert cursor.fetchone().is_valid_format is None


def test_non_unique_path_failure(mocked_config, logger, tmpdir, monkeypatch):
    """Test if same path is defined two or more times; then config parsing fails."""
    # Set config file using the environment variable
    catalog_config_dict = {
        'prospectors': [
            {
                'file_type': 'operator',
                'paths': [str(tmpdir.join('operator.zip'))],
                'schema_filename': 'OperatorImportSchema_v2.csvs'
            },
            {
                'file_type': 'operator',
                'paths': [str(tmpdir.join('operator.zip')),
                          str(tmpdir.join('operator1.zip'))],
                'schema_filename': 'OperatorImportSchema_v2.csvs'
            }
        ],
        'perform_prevalidation': False
    }

    with pytest.raises(Exception) as ex:
        CatalogConfig(ignore_env=True, **catalog_config_dict)

    assert 'The paths specified in the catalog config are not globally unique' in str(ex)


def test_invalid_zip_file(postgres, db_conn, tmpdir, mocked_config, monkeypatch):
    """Test that invalid zip files are properly handled."""
    valid_csv_file_path = str(tmpdir.join('operator1_with_rat_info_20160701_20160731.csv'))
    invalid_file_zip_path = valid_csv_file_path[:-3] + 'zip'
    with open(valid_csv_file_path, 'w') as f:
        f.close()
    os.rename(valid_csv_file_path, invalid_file_zip_path)

    catalog_config_dict = {
        'prospectors': [
            {
                'file_type': 'operator',
                'paths': [str(tmpdir)],
                'schema_filename': 'OperatorImportSchema.csvs'
            },
            {
                'file_type': 'operator',
                'paths': [invalid_file_zip_path],
                'schema_filename': 'OperatorImportSchema_v2.csvs'
            }
        ],
        'perform_prevalidation': True
    }

    catalog_config = CatalogConfig(ignore_env=True, **catalog_config_dict)
    monkeypatch.setattr(mocked_config, 'catalog_config', catalog_config)
    # Run dirbs-catalog using db args from the temp postgres instance
    runner = CliRunner()
    result = runner.invoke(dirbs_catalog_cli, obj={'APP_CONFIG': mocked_config}, catch_exceptions=False)
    assert result.exit_code == 0
