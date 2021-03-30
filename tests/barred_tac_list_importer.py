"""
Barred TAC List data import unit tests.

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
from _importer_params import BarredTacListParams
from _helpers import expect_failure, expect_success, get_importer
from dirbs.importer.barred_tac_list_importer import BarredTacListImporter
from dirbs.cli.importer import cli as dirbs_import_cli


def test_cli_barred_list_importer(postgres, db_conn, tmpdir, mocked_config, logger):
    """Verify that the cli import command for barred list is working properly."""
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/barred_tac_list')
    valid_data_file_name = 'sample_barred_tac_list.csv'
    valid_data_file = path.join(data_dir, valid_data_file_name)

    # zipping file inside a temp dir
    valid_zip_file_path = str(tmpdir.join('sample_barred_tac_list.zip'))
    with zipfile.ZipFile(valid_zip_file_path, 'w') as valid_zip_file:
        valid_zip_file.write(valid_data_file, valid_data_file_name)

    # invoke cli runner
    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, ['barred_tac_list', valid_zip_file_path],
                           obj={'APP_CONFIG': mocked_config})

    # verify data in db
    with db_conn.cursor() as cursor:
        cursor.execute('SELECT tac FROM barred_tac_list ORDER BY tac')
        result_list = [res.tac for res in cursor]

    assert result.exit_code == 0
    assert result_list == ['10000110', '10000220', '10000330', '10000440', '10000550', '10000660']


@pytest.mark.parametrize('barred_tac_list_importer',
                         [BarredTacListParams(filename='barred_tac_list_missing_header.csv')],
                         indirect=True)
def test_missing_header(barred_tac_list_importer, logger, db_conn):
    """Verify that the barred list data is not imported if a header column is missing."""
    expect_failure(barred_tac_list_importer, exc_message='Metadata header, cannot find the column headers - tac, '
                                                         '10000110')


@pytest.mark.parametrize('barred_tac_list_importer',
                         [BarredTacListParams(filename='sample_barred_tac_list.csv')],
                         indirect=True)
def test_simple_import(barred_tac_list_importer, logger, db_conn):
    """Verify that the valid barred list data can be successfully imported into the db."""
    expect_success(barred_tac_list_importer, 6, db_conn, logger)


@pytest.mark.parametrize('barred_tac_list_importer',
                         [BarredTacListParams(filename='sample_barred_tac_list_v1.csv')],
                         indirect=True)
def test_repeat_import(barred_tac_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                       mocked_config, tmpdir):
    """Verify that valid barred list data can be successfully imported into the database.

    when repeating the import of the same file.
    """
    expect_success(barred_tac_list_importer, 6, db_conn, logger)
    with get_importer(BarredTacListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      BarredTacListParams(filename='sample_barred_tac_list_v1.csv')) as imp:
        expect_success(imp, 6, db_conn, logger)


@pytest.mark.parametrize('barred_tac_list_importer',
                         [BarredTacListParams(filename='sample_barred_tac_list_v2.csv')],
                         indirect=True)
def test_historical_check_percent_fails(barred_tac_list_importer, logger, mocked_statsd, db_conn, mocked_config,
                                        metadata_db_conn, tmpdir):
    """Verify that barred list import fails historical check."""
    expect_success(barred_tac_list_importer, 20, db_conn, logger)
    with get_importer(BarredTacListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      BarredTacListParams(filename='sample_barred_tac_list_historicalcheck.csv',
                                          import_size_variation_percent=mocked_config.barred_tac_threshold_config.
                                          import_size_variation_percent,
                                          import_size_variation_absolute=mocked_config.barred_tac_threshold_config.
                                          import_size_variation_absolute
                                          )) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')


@pytest.mark.parametrize('barred_tac_list_importer',
                         [BarredTacListParams(filename='sample_barred_tac_list_v2.csv')],
                         indirect=True)
def test_historical_check_empty(barred_tac_list_importer, logger, mocked_statsd, db_conn, metadata_db_conn,
                                mocked_config, tmpdir):
    """Verify that empty file import fails after importing a non empty file."""
    expect_success(barred_tac_list_importer, 20, db_conn, logger)
    with get_importer(BarredTacListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      BarredTacListParams(filename='empty_barred_tac_list_historical_check.csv')) as imp:
        expect_failure(imp, exc_message='Failed import size historic check')


@pytest.mark.parametrize('barred_tac_list_importer',
                         [BarredTacListParams(filename='barred_tac_list_incorrectfiletype.txt')],
                         indirect=True)
def test_invalid_file_type(barred_tac_list_importer):
    """Verify that Barred List data is not imported if the filename format is invalid."""
    expect_failure(barred_tac_list_importer, exc_message='Wrong suffix')


@pytest.mark.parametrize('barred_tac_list_importer',
                         [BarredTacListParams(content='TAC,change_type\n'
                                                      '12345678,ADD',
                                              delta=True,
                                              perform_delta_updates_check=False)],
                         indirect=True)
def test_delta_file_prevalidation(barred_tac_list_importer):
    """Test pre-validation schema."""
    expect_failure(barred_tac_list_importer,
                   exc_message='regex("^(add|remove)$") fails for line: 1, column: change_type, value: "ADD"')
