"""
Barred List data import unit tests.

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

from _fixtures import *  # noqa: F403, F401
from _importer_params import BarredListParams
from _helpers import expect_failure, expect_success, get_importer, data_file_to_test
from dirbs.importer.barred_list_importer import BarredListImporter
from dirbs.cli.importer import cli as dirbs_import_cli


def test_cli_barred_list_importer(postgres, db_conn, tmpdir, mocked_config, logger):
    """Verify that the cli import command for barred list is working properly."""
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/barred_list')
    valid_data_file_name = 'sample_barred_import_list.csv'
    valid_data_file = path.join(data_dir, valid_data_file_name)

    # zipping file inside a temp dir
    valid_zip_file_path = str(tmpdir.join('sample_barred_import_list.zip'))
    with zipfile.ZipFile(valid_zip_file_path, 'w') as valid_zip_file:
        valid_zip_file.write(valid_data_file, valid_data_file_name)

    # invoke cli runner
    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, ['barred_list', valid_zip_file_path],
                           obj={'APP_CONFIG': mocked_config})

    # verify data in db
    with db_conn.cursor() as cursor:
        cursor.execute('SELECT imei_norm FROM barred_list ORDER BY imei_norm')
        result_list = [res.imei_norm for res in cursor]

    assert result.exit_code == 0
    assert result_list == ['10000110000006', '10000220000007', '10000330000008',
                           '10000440000009', '10000550000000', '10000660000001']


@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(filename='barred_list_missing_header.csv')],
                         indirect=True)
def test_missing_header(barred_list_importer, logger, db_conn):
    """Verify that the barred list data is not imported if a header column is missing."""
    expect_failure(barred_list_importer, exc_message='Metadata header, cannot find the column headers - imei, '
                                                     '642222222222222')


@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(filename='sample_barred_list_normalize.csv')],
                         indirect=True)
def test_matching_normalization(barred_list_importer, logger, db_conn):
    """Verify that IMEIs that normalize to the same value are successfully imported into the database."""
    expect_success(barred_list_importer, 5, db_conn, logger)


@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(filename='sample_barred_list.csv')],
                         indirect=True)
def test_simple_import(barred_list_importer, logger, db_conn):
    """Verify that the valid barred list data can be successfully imported into the db."""
    expect_success(barred_list_importer, 20, db_conn, logger)


@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(filename='sample_barred_list_v1.csv')],
                         indirect=True)
def test_repeat_import(barred_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn, mocked_config, tmpdir):
    """Verify that valid barred list data can be successfully imported into the database.

    when repeating the import of the same file.
    """
    expect_success(barred_list_importer, 21, db_conn, logger)
    with get_importer(BarredListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      BarredListParams(filename='sample_barred_list_v1.csv')) as imp:
        expect_success(imp, 21, db_conn, logger)


@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(filename='sample_barred_list.csv')],
                         indirect=True)
def test_historical_check_percent_fails(barred_list_importer, logger, mocked_statsd, db_conn, mocked_config,
                                        metadata_db_conn, tmpdir):
    """Verify that barred list import fails historical check."""
    expect_success(barred_list_importer, 20, db_conn, logger)
    with get_importer(BarredListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      BarredListParams(filename='sample_barred_list_historicalcheck.csv',
                                       import_size_variation_percent=mocked_config.barred_threshold_config.
                                       import_size_variation_percent,
                                       import_size_variation_absolute=mocked_config.barred_threshold_config.
                                       import_size_variation_absolute
                                       )) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')


@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(filename=data_file_to_test(100,
                                                                      imei_custom_header='imei',
                                                                      imei_imsi=False))],
                         indirect=True)
def test_historical_check_percent_succeeds(barred_list_importer, logger, mocked_statsd, db_conn, mocked_config,
                                           metadata_db_conn, tmpdir):
    """Verify that a local barred data is successfully imported.

    After having imported two files where the second file has 80% size of the first one and the threshold value is 75.
    """
    expect_success(barred_list_importer, 100, db_conn, logger)
    with get_importer(BarredListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      BarredListParams(filename=data_file_to_test(80, imei_custom_header='imei',
                                                                  imei_imsi=False),
                                       import_size_variation_percent=mocked_config.barred_threshold_config.
                                       import_size_variation_percent,
                                       import_size_variation_absolute=mocked_config.barred_threshold_config.
                                       import_size_variation_absolute)) as imp:
        expect_success(imp, 80, db_conn, logger)


@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(filename='sample_barred_list.csv')],
                         indirect=True)
def test_historical_check_empty(barred_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                mocked_config, tmpdir):
    """Verify that empty file import fails after importing a non empty file."""
    expect_success(barred_list_importer, 20, db_conn, logger)
    with get_importer(BarredListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      BarredListParams(filename='empty_barredlist_historical_check.csv')) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')


@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(filename='barred_list_incorrectfiletype.txt')],
                         indirect=True)
def test_invalid_file_type(barred_list_importer):
    """Verify that Barred List data is not imported if the filename format is invalid."""
    expect_failure(barred_list_importer, exc_message='Wrong suffix')


@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(filename='barred_list_hexpound.csv')],
                         indirect=True)
def test_malformed_imeis(barred_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                         mocked_config, tmpdir):
    """Verify that the barred list data file is accepted.

    And imported if the data contains IMEIs with #, and *.
    """
    expect_success(barred_list_importer, 20, db_conn, logger)
    # attempting to import stolen list file containing symbol not allowed '%'.
    with get_importer(BarredListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      BarredListParams(filename='barred_list_hexpound_bad_symbol.csv')) as imp:
        expect_failure(imp, exc_message='regex("^[0-9A-Fa-f\\\\*\\\\#]{1,16}$") '
                                        'fails for line: 1, column: imei, value: '
                                        '"62%222222222222"\\nFAIL')


@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(filename='sample_barred_list_normalize.csv')],
                         indirect=True)
def test_fields_normalized(barred_list_importer, logger, db_conn):
    """Verify that the data was imported into the database and normalized per the following criteria.

    If the first 14 characters of the IMEI are digits, the normalised IMEI is the first 14 characters.
    If the imei does not start with 14 leading digits, no normalisation is done and we just copy the imei
    value to the imei_norm column.
    """
    expect_success(barred_list_importer, 5, db_conn, logger)
    with db_conn.cursor() as cursor:
        cursor.execute('SELECT imei_norm FROM barred_list ORDER BY imei_norm')
        res = {x.imei_norm for x in cursor.fetchall()}
        assert len(res) == 5
        assert res == {'642002#2222220', '6431133*3333331', '64422444444444', '64533555555555', '64644666666666'}


@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(content='imei,change_type\n'
                                                   '12345678901234,ADD',
                                           delta=True,
                                           perform_delta_updates_check=False)],
                         indirect=True)
def test_delta_file_prevalidation(barred_list_importer):
    """Test pre-validation schema."""
    expect_failure(barred_list_importer,
                   exc_message='regex("^(add|remove)$") fails for line: 1, column: change_type, value: "ADD"')
