"""
GSMA data import unit tests.

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

from dirbs.importer.gsma_data_importer import GSMADataImporter
from dirbs.cli.importer import cli as dirbs_import_cli
from _helpers import get_importer, expect_success, expect_failure, logger_stream_contents
from _fixtures import *  # noqa: F403, F401
from _importer_params import GSMADataParams


def test_extract(db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd):
    """Test Depot not available because it is not available on the command-line .

    Verify that a zipped txt file can be imported.
    """
    fn = 'gsma_simple_extraction_anonymized.txt'
    abs_fn = path.join(path.abspath(path.dirname(__file__) + '/unittest_data/gsma'), fn)
    zip_name = path.join(str(tmpdir), path.split(fn)[1][:-3] + 'zip')
    with zipfile.ZipFile(zip_name, mode='w') as zf:
        zf.write(abs_fn, arcname=path.split(fn)[1])
    with get_importer(GSMADataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GSMADataParams(filename=zip_name,
                                     extract=True)) as imp:
        imp.import_data()


def test_cli_gsma_importer(postgres, db_conn, tmpdir, mocked_config, logger):
    """Test Depot not available yet.

    Verify that the CLI import command for GSMA is working properly.
    """
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/gsma')
    valid_csv_gsma_data_file_name = 'sample_gsma_import_list_anonymized.txt'
    valid_csv_gsma_data_file = path.join(data_dir, valid_csv_gsma_data_file_name)

    # create a zip file inside a temp dir
    valid_zip_gsma_data_file_path = str(tmpdir.join('sample_gsma_import_list_anonymized.zip'))
    with zipfile.ZipFile(valid_zip_gsma_data_file_path, 'w') as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is
        # the archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_gsma_data_file,
                                                 valid_csv_gsma_data_file_name)

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()  # noqa
    result = runner.invoke(dirbs_import_cli, ['gsma_tac', valid_zip_gsma_data_file_path],
                           obj={'APP_CONFIG': mocked_config})

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT tac FROM gsma_data ORDER BY tac')
        result_list = [res.tac for res in cursor]

    assert result.exit_code == 0
    assert result_list == ['01234401', '01234402', '01234403', '01234404', '01234405', '01234406', '01234407']


def test_row_count_stats(postgres, db_conn, tmpdir, mocked_config, logger):
    """Test Depot not available yet.

    Verify output stats for CLI import command.
    """
    # Part 1) populate gsma_table and verify before import row count
    # Part 2) import gsma file containing one duplicate (same row) and verify that staging_row_count value includes
    # duplicates and import_table_new_row_count doesn't.
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""INSERT INTO historic_gsma_data(tac,
                                                         manufacturer,
                                                         bands,
                                                         allocation_date,
                                                         model_name,
                                                         device_type,
                                                         start_date,
                                                         end_date)
                                 VALUES('01234410',
                                        'AManufacturer',
                                        'd3bdf1170bf4b026e6e29b15a0d66a5ca83f1944',
                                        NOW(),
                                        'AMODEL',
                                        'Handheld',
                                        NOW(),
                                        NULL),
                                        ('01234411',
                                        'AManufacturer',
                                        'd3bdf1170bf4b026e6e29b15a0d66a5ca83f1944',
                                        NOW(),
                                        'AMODEL',
                                        'Handheld',
                                        NOW(),
                                        NULL)
                        """)
        assert cursor.rowcount == 2
        cursor.execute("""REFRESH MATERIALIZED VIEW CONCURRENTLY gsma_data""")

    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/gsma')
    valid_csv_gsma_data_file_name = 'sample_gsma_import_list_dupl_anonymized.txt'
    valid_csv_gsma_data_file = path.join(data_dir, valid_csv_gsma_data_file_name)
    # create a zip file inside a temp dir
    valid_zip_gsma_data_file_path = str(tmpdir.join('sample_gsma_import_list_dupl_anonymized.zip'))
    with zipfile.ZipFile(valid_zip_gsma_data_file_path, 'w') as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is
        # the archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_gsma_data_file,
                                                 valid_csv_gsma_data_file_name)
    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, ['gsma_tac', valid_zip_gsma_data_file_path],
                           obj={'APP_CONFIG': mocked_config})
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute('SELECT tac FROM gsma_data ORDER BY tac')
        result_list = [res.tac for res in cursor]
    assert result.exit_code == 0
    assert result_list == ['01234401', '01234402', '01234403', '01234404', '01234405', '01234406', '01234407']
    assert len(result_list) == 7
    # Test  Part 1)
    assert 'Rows in table prior to import: 2' in logger_stream_contents(logger)
    # Test  Part 2) - self.staging_row_count
    assert 'Rows supplied in full input file: 8' in logger_stream_contents(logger)
    # Test  Part 2) - import_table_new_row_count=rows_before + rows_inserted - rows_deleted
    assert 'Rows in table after import: 7' in logger_stream_contents(logger)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_white_spaces.txt')],
                         indirect=True)
def test_preprocess_trim(gsma_tac_db_importer, logger, db_conn):
    """Test Depot ID not known yet.

    Verify that a white space at the start or the end of a field can be handled and imported successfully.
    The expected behaviour is that the white space is stripped out.
    """
    expect_success(gsma_tac_db_importer, 2, db_conn, logger)
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute('SELECT tac, manufacturer, bands, model_name FROM gsma_data ORDER BY tac')
        result = [(x.tac, x.manufacturer, x.bands, x.model_name) for x in cursor.fetchall()]
        assert result == [('21782434', None, 'a0a0db6e9eccb4a8c3a85452b79db6c793398d6a',
                           '927824c30540c400f59b6c02aeb0a30d5033eb1a'),
                          ('38245933', '326d9e7920b30b698f189a83d2be6f4384496ebc',
                           '6cc923523f  690fe51b51efc747451bfbbe1994d9',
                           'cff96c002766bde09400d9030ad2d055e62b7a45')]


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_doublequotes.txt')],
                         indirect=True)
def test_preprocess_quoted(gsma_tac_db_importer, logger, db_conn):
    """Test Depot ID 96571/2.

    Verify that a double quote at the start of a field can be handled and imported successfully.
    The expected behaviour is that the double quote is stripped out.
    """
    expect_success(gsma_tac_db_importer, 3, db_conn, logger)
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""SELECT optional_fields->'marketing_name' AS mn
                            FROM gsma_data
                           WHERE tac = \'38245933\'""")  # noqa Q444
        assert cursor.fetchone().mn == 'Test Marketing Name'


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_escaped_quotes.txt')],
                         indirect=True)
def test_preprocess_escaped_quotes(gsma_tac_db_importer, logger, db_conn):
    """Test Depot ID 96767/2.

    Verify that it if a field is enclosed in double quote, these are simply stripped out.
    """
    expect_success(gsma_tac_db_importer, 1, db_conn, logger)
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""SELECT optional_fields->'marketing_name' AS mn FROM gsma_data ORDER BY tac""")  # noqa Q444
        assert cursor.fetchone().mn == 'A Marketing name'


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_uneven_columns.txt')],
                         indirect=True)
def test_preprocess_uneven_rows(gsma_tac_db_importer, logger):
    """Test Depot ID 96695/18.

    Verify that the gsma data file is rejected and not
    imported if inconsistent number of fields per row.
    """
    expect_failure(gsma_tac_db_importer, exc_message='Inconsistent number of fields per row')


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_missing_headers_july_2016.txt')],
                         indirect=True)
def test_preprocess_missing_headers(gsma_tac_db_importer, logger):
    """Test Depot ID 96573/4.

    Verify that the gsma data file is rejected and not imported if a header column is missing.
    Test file with no extra fields and missing headers.
    """
    expect_failure(gsma_tac_db_importer, exc_message='Missing mandatory field')


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_small_july_2016.txt')],
                         indirect=True)
def test_preprocess_no_extra(gsma_tac_db_importer, logger, db_conn):
    """Test Depot ID 96670/10.

    Verify that data import of GSMA Data is successful
    when no extra fields are added to the data.
    """
    expect_success(gsma_tac_db_importer, 3, db_conn, logger)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_extracolumns_July_2016.txt')],
                         indirect=True)
def test_preprocess_extra(db_conn, gsma_tac_db_importer):
    """Test Depot ID 96581/12.

    Verify that data import of GSMA Data is
    successful when the extra fields are added to the data.
    """
    gsma_tac_db_importer.import_data()
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""SELECT optional_fields->'marketing_name' AS mn FROM gsma_data ORDER BY tac""")  # noqa Q444
        assert cursor.fetchone().mn == 'test'


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_small_july_2016.txt')],
                         indirect=True)
def test_repeat_data_upload(gsma_tac_db_importer, mocked_config, logger, mocked_statsd, db_conn,
                            metadata_db_conn, tmpdir):
    """Test Depot ID 96579/10.

    Verify that valid GSMA Data can be successfully imported into the database
    when repeating the import of the same file.
    """
    expect_success(gsma_tac_db_importer, 3, db_conn, logger)
    with get_importer(GSMADataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GSMADataParams(filename='gsma_dump_small_july_2016.txt')) as imp:
        expect_success(imp, 3, db_conn, logger)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(
                             filename='gsma_dump_duplicaterecord_2016_large.txt')],
                         indirect=True)
def test_duplicate_tac_count(gsma_tac_db_importer):
    """Test duplicates."""
    # gsma entries: 38245933(2 entries), 38245932(4 entries), 38245931(2 entries)
    # expect the duplicates to be 1+3+1=5
    expect_failure(gsma_tac_db_importer, exc_message='Conflicting rows check failed '
                                                     '(5 rows with same primary key and conflicting data)')


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_partialduplicaterecord_2016.txt')],
                         indirect=True)
def test_duplicate_tac_mismatch(gsma_tac_db_importer):
    """Test Depot ID not known yet.

    Verify that partial duplicate entries(Same TAC and at least 1 identical column value) in
    another row is marked as duplicate and is not imported into the DB.
    """
    expect_failure(gsma_tac_db_importer, exc_message='Conflicting rows check failed '
                                                     '(1 rows with same primary key and conflicting data)')


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_BlankTac_july_2016.txt')],
                         indirect=True)
def test_invalid_column_data_one(gsma_tac_db_importer, logger, db_conn, tmpdir):
    """Test Depot ID 96570/10.

    Verify that GSMA data is pre-checked for invalid column specific information
    and is not inserted into the DB.
    """
    expect_failure(gsma_tac_db_importer,
                   exc_message='length(8) fails for line: 1, column: TAC, value: "BlankTac123"\\nFAIL')


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_InvalidTac_july_2016.txt')],
                         indirect=True)
def test_invalid_column_data_two(gsma_tac_db_importer, logger, db_conn, tmpdir):
    """Test Depot ID 96570/10.

    Verify that GSMA data is pre-checked for invalid column specific information
    and is not inserted into the DB.
    """
    expect_failure(gsma_tac_db_importer,
                   exc_message='length(8) fails for line: 1, column: TAC, value: "9113177"\\nFAIL')


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_InvalidDate_july_2016.txt')],
                         indirect=True)
def test_invalid_column_data_three(gsma_tac_db_importer, logger, db_conn, tmpdir):
    """Test Depot ID 96570/10.

    Verify that GSMA data is pre-checked for invalid column specific information
    and is not inserted into the DB.
    """
    expect_failure(gsma_tac_db_importer,
                   exc_message='fails for line: 1, column: Allocation_Date, value: "2011-23-Sep"\\nFAIL')


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_emptynontac_july_2016.txt')],
                         indirect=True)
def test_empty_values(gsma_tac_db_importer, logger, db_conn):
    """Test Depot ID 96576/7.

    Verify that GSMA Data with null non-TAC values successfully passes
    validation and is imported into the database.
    """
    expect_success(gsma_tac_db_importer, 5, db_conn, logger)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(
                             filename='gsma_dump_doublequotes.txt')],
                         indirect=True)
def test_dubious_quoting(gsma_tac_db_importer, logger, db_conn):
    """Test Depot ID 96571/2.

    Verify that a file containing a row with a single double quote is successfully imported.
    The following test does not conform to RFC-4180, but is accepted
    by both the validator, Postgres and Python CSV. We keep this test
    to ensure it keeps passing.
    """
    expect_success(gsma_tac_db_importer, 3, db_conn, logger)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(
                             filename='gsma_start_field_mandatory_quote.txt')],
                         indirect=True)
def test_start_mandatory_field_quote(gsma_tac_db_importer, logger, db_conn):
    """Test Depot ID 96768/2.

    Verify that GSMA data can be successfully be imported if
    mandatory fields start with quotes.
    """
    expect_success(gsma_tac_db_importer, 1, db_conn, logger)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_commadelimited_2016.txt')],
                         indirect=True)
def test_incorrect_delimiter(gsma_tac_db_importer, logger):
    """Test Depot ID 96572/3.

    Verify that the GSMA data file is rejected and not
    imported if it is not "|" delimited.
    """
    expect_failure(gsma_tac_db_importer, exc_message='Missing mandatory field')


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_mixedcases_2016.txt')],
                         indirect=True)
def test_headers_mixed_cases(gsma_tac_db_importer, db_conn, logger):
    """Test Depot ID 96574/5.

    Verify that the GSMA data file is accepted and imported if
    the headers have mixed cases.
    """
    expect_success(gsma_tac_db_importer, 3, db_conn, logger)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_malformeddate_2016.txt')],
                         indirect=True)
def test_malformed_date(gsma_tac_db_importer, db_conn, logger):
    """Test Depot ID 96575/6.

    Verify that the GSMA data file is rejected if
    the file contains a malformed date.
    """
    expect_failure(gsma_tac_db_importer,
                   exc_message='fails for line: 3, column: Allocation_Date, value: "Sep-23-2011"\\nFAIL')


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_emptynontac_july_2016.txt')],
                         indirect=True)
def test_null_non_tac_entries(gsma_tac_db_importer, db_conn, logger):
    """Test Depot ID 96576/7.

    Verify that GSMA Data with null non-TAC values successfully
    passes validation and is imported into the database.
    """
    expect_success(gsma_tac_db_importer, 5, db_conn, logger)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_requiredfields_reordered_july_2016.txt')],
                         indirect=True)
def test_reordered_columns(gsma_tac_db_importer, db_conn, logger):
    """Test Depot ID 96580/11.

    Verify that Data Import of GSMA Data is
    successful when the columns are re-ordered.
    """
    expect_success(gsma_tac_db_importer, 3, db_conn, logger)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_extracolumns_July_2016.txt')],
                         indirect=True)
def test_data_new_columns(gsma_tac_db_importer, db_conn, logger):
    """Test Depot ID 96581/12.

    Verify that data import of GSMA Data is
    successful when the new columns are added to the data.
    """
    expect_success(gsma_tac_db_importer, 3, db_conn, logger)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_large_july_2016.txt',
                                         extract=False)],
                         indirect=True)
def test_historical_check_fails(gsma_tac_db_importer, mocked_config, logger, mocked_statsd, db_conn,
                                metadata_db_conn, tmpdir):
    """Test Depot ID 96582/13.

    Verify that data is not imported if the historical check fails.
    """
    expect_success(gsma_tac_db_importer, 24727, db_conn, logger)

    # Try a small import
    with get_importer(GSMADataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GSMADataParams(filename='gsma_dump_small_july_2016.txt',
                                     import_size_variation_percent=mocked_config.gsma_threshold_config.
                                     import_size_variation_percent,
                                     import_size_variation_absolute=mocked_config.gsma_threshold_config.
                                     import_size_variation_absolute,
                                     extract=False)) as gsma_small_importer:
        expect_failure(gsma_small_importer, exc_message='Failed import size historic check')


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_small_july_2016.txt',
                                         extract=False)],
                         indirect=True)
def test_historical_check_passes(gsma_tac_db_importer, mocked_config, logger, mocked_statsd, db_conn,
                                 metadata_db_conn, tmpdir):
    """Test Depot ID 96583/14.

    Verify that data is successfully imported if the historical check passes.
    """
    expect_success(gsma_tac_db_importer, 3, db_conn, logger)
    with get_importer(GSMADataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GSMADataParams(filename='gsma_dump_large_july_2016.txt',
                                     import_size_variation_percent=mocked_config.gsma_threshold_config.
                                     import_size_variation_percent,
                                     import_size_variation_absolute=mocked_config.gsma_threshold_config.
                                     import_size_variation_absolute,
                                     extract=False)) as imp:
        expect_success(imp, 24727, db_conn, logger)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_rat_computation_check.txt')],
                         indirect=True)
def test_rat_bitmask_computation(gsma_tac_db_importer, logger, db_conn):
    """Test Depot ID unknown.

    Verify that the RAT bitmask is computed corrected based on band capability.
    """
    expect_success(gsma_tac_db_importer, 9, db_conn, logger)
    with db_conn.cursor() as cursor:
        # Test GSM only model
        cursor.execute("SELECT rat_bitmask FROM gsma_data WHERE tac = \'01132222\'")
        result = cursor.fetchone()
        assert result[0] == int('00000000000000000000000001000000', 2)

        # Test LTE only model
        cursor.execute("SELECT rat_bitmask FROM gsma_data WHERE tac = \'41233333\'")
        result = cursor.fetchone()
        assert result[0] == int('00000000000000000001000000000000', 2)

        # Test GSM + WCDMA model
        cursor.execute("SELECT rat_bitmask FROM gsma_data WHERE tac = \'41255555\'")
        result = cursor.fetchone()
        assert result[0] == int('00000000000000000000001001000000', 2)

        # Test WCDMA only model
        cursor.execute("SELECT rat_bitmask FROM gsma_data WHERE tac = \'41266666\'")
        result = cursor.fetchone()
        assert result[0] == int('00000000000000000000001000000000', 2)

        # Test GSM + WCDMA + LTE model
        cursor.execute("SELECT rat_bitmask FROM gsma_data WHERE tac = \'41277777\'")
        result = cursor.fetchone()
        assert result[0] == int('00000000000000000001001001000000', 2)

        # Test GSM + LTE model
        cursor.execute("SELECT rat_bitmask FROM gsma_data WHERE tac = \'41288888\'")
        result = cursor.fetchone()
        assert result[0] == int('00000000000000000001000001000000', 2)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_large_july_2016.txt',
                                         extract=False)],
                         indirect=True)
def test_validation_check_override(gsma_tac_db_importer, mocked_config, logger, mocked_statsd, db_conn,
                                   metadata_db_conn, tmpdir):
    """Test Depot ID 96586/17.

    Verify that the user can override historical checks when importing GSMA Data files.
    """
    expect_success(gsma_tac_db_importer, 24727, db_conn, logger)
    with get_importer(GSMADataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GSMADataParams(filename='gsma_dump_small_july_2016.txt',
                                     perform_historic_check=False,
                                     extract=False)) as imp:
        expect_success(imp, 3, db_conn, logger)


def test_historic_threshold_config_cli(postgres, db_conn, tmpdir, mocked_config, monkeypatch):
    """Test Depot not available yet. Verify that historic treeshold is configurable by yaml."""
    monkeypatch.setattr(mocked_config.gsma_threshold_config, 'import_size_variation_absolute', 0.3, raising=False)
    monkeypatch.setattr(mocked_config.gsma_threshold_config, 'import_size_variation_percent', 0.3, raising=False)

    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/gsma')
    valid_csv_gsma_data_file_name = 'sample_gsma_import_list_anonymized.txt'
    valid_csv_gsma_data_file = path.join(data_dir, valid_csv_gsma_data_file_name)

    # create a zip file inside a temp dir
    valid_zip_gsma_data_file_path = str(tmpdir.join('sample_gsma_import_list_anonymized.zip'))
    with zipfile.ZipFile(valid_zip_gsma_data_file_path, 'w') as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is
        # the archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_gsma_data_file,
                                                 valid_csv_gsma_data_file_name)

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()  # noqa
    runner.invoke(dirbs_import_cli, ['gsma_tac', valid_zip_gsma_data_file_path],
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


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_small_july_2016.txt',
                                         extract=False)],
                         indirect=True)
def test_same_import_twice(gsma_tac_db_importer, mocked_config, logger, mocked_statsd, db_conn,
                           metadata_db_conn, tmpdir):
    """Test Depot not known yet.

    Verify that if we import twice the same file, same entries are ignored and not added to the historic table.
    """
    expect_success(gsma_tac_db_importer, 3, db_conn, logger)
    with db_conn.cursor() as cursor:
        cursor.execute('SELECT * FROM historic_gsma_data')
        first_import = cursor.rowcount

    with get_importer(GSMADataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GSMADataParams(filename='gsma_dump_small_july_2016.txt',
                                     extract=False)) as imp:
        expect_success(imp, 3, db_conn, logger)

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT * FROM historic_gsma_data')
        second_import = cursor.rowcount

    assert first_import == second_import == 3
