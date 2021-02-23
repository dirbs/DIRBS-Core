"""
dirbs-db unit tests.

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

from click.testing import CliRunner
import testing.postgresql
import pytest
from psycopg2 import sql

from dirbs.cli.listgen import cli as dirbs_listgen_cli
from dirbs.cli.classify import cli as dirbs_classify_cli
from dirbs.cli.db import cli as dirbs_db_cli
from dirbs.importer.operator_data_importer import OperatorDataImporter
from _fixtures import *    # noqa: F403, F401
from _helpers import import_data, get_importer, expect_success
from _importer_params import OperatorDataParams, GSMADataParams, StolenListParams, PairListParams, \
    RegistrationListParams


def test_basic_cli_check(postgres, mocked_config, monkeypatch):
    """Test that the dirbs-db check script runs without an error."""
    runner = CliRunner()
    # Now use non-empty, installed PostgreSQL
    # Run dirbs-db check using db args from the temp postgres instance
    result = runner.invoke(dirbs_db_cli, ['check'], obj={'APP_CONFIG': mocked_config})
    # Test whether dirbs-db check passes after schema install
    assert result.exit_code == 0

    # Create temp empty postgres instance
    empty_postgresql = testing.postgresql.Postgresql()
    dsn = empty_postgresql.dsn()
    for setting in ['database', 'host', 'port', 'user', 'password']:
        monkeypatch.setattr(mocked_config.db_config, setting, dsn.get(setting, None))

    result = runner.invoke(dirbs_db_cli, ['check'], obj={'APP_CONFIG': mocked_config})
    # Test whether check fails on an empty db
    assert result.exit_code == 1


def test_basic_cli_upgrade(postgres, mocked_config):
    """Test that the dirbs-db upgrade script runs without an error."""
    runner = CliRunner()
    result = runner.invoke(dirbs_db_cli, ['upgrade'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0


@pytest.mark.parametrize('operator_data_importer, stolen_list_importer, pairing_list_importer, '
                         'gsma_tac_db_importer, registration_list_importer',
                         [(OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False),
                           StolenListParams(
                             filename='testData1-sample_stolen_list-anonymized.csv'),
                           PairListParams(
                             filename='testData1-sample_pairinglist-anonymized.csv'),
                           GSMADataParams(
                             filename='testData1-gsmatac_operator4_operator1_anonymized.txt'),
                           RegistrationListParams(
                             filename='sample_registration_list.csv'))],
                         indirect=True)
def test_cli_repartition(postgres, mocked_config, db_conn, operator_data_importer, registration_list_importer,
                         pairing_list_importer, stolen_list_importer, gsma_tac_db_importer, tmpdir, logger,
                         metadata_db_conn, mocked_statsd):
    """Test that the dirbs-db partition script runs without an error."""
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    import_data(gsma_tac_db_importer, 'gsma_data', 13, db_conn, logger)
    import_data(stolen_list_importer, 'stolen_list', 21, db_conn, logger)
    import_data(registration_list_importer, 'registration_list', 20, db_conn, logger)
    import_data(pairing_list_importer, 'pairing_list', 7, db_conn, logger)

    # Import second month of operator data to ensure that we have 2 months worth for the same operator
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          content='date,imei,imsi,msisdn\n'
                                  '20161201,64220496727231,123456789012345,123456789012345\n'
                                  '20161201,64220496727232,123456789012345,123456789012345',
                          operator='operator1',
                          extract=False,
                          perform_leading_zero_check=False,
                          perform_unclean_checks=False,
                          perform_home_network_check=False,
                          perform_region_checks=False,
                          perform_historic_checks=False,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                          perform_file_daterange_check=False)) as new_imp:
        expect_success(new_imp, 19, db_conn, logger)

    # Run dirbs-classify and dirbs-listgen to populate some tables prior to re-partition
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    result = runner.invoke(dirbs_listgen_cli, [output_dir], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # Make sure that if we run with some invalid phyical shards, that it fails
    for num_shards in [-1, 0, 101]:
        result = runner.invoke(dirbs_db_cli, ['repartition', '--num-physical-shards={0:d}'.format(num_shards)],
                               obj={'APP_CONFIG': mocked_config})
        assert result.exit_code != 0

    partitioned_tables = ['classification_state', 'historic_pairing_list', 'historic_registration_list',
                          'network_imeis', 'monthly_network_triplets_per_mno_operator1_2016_11',
                          'monthly_network_triplets_country_2016_11', 'blacklist', 'exceptions_lists_operator1',
                          'notifications_lists_operator1', 'historic_stolen_list']

    with db_conn, db_conn.cursor() as cursor:
        # Manually add one record into the notifications_lists for operator_1 so that the repartitioned table
        # is not empty
        cursor.execute("""INSERT INTO notifications_lists_operator1 (operator_id, imei_norm, imsi, msisdn, block_date,
                                                                     reasons, start_run_id, end_run_id, delta_reason,
                                                                     virt_imei_shard)
                               VALUES ('operator1', '12345678901234', '12345678901234', '1', '20170110',
                                      ARRAY['condition1'], 1125, NULL, 'new', calc_virt_imei_shard('12345678901234'))
                       """)

    # Run dirbs-db repartition to 8 partitions and check that it works
    result = runner.invoke(dirbs_db_cli, ['repartition', '--num-physical-shards=8'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    with db_conn, db_conn.cursor() as cursor:
        for base_table in partitioned_tables:
            cursor.execute(sql.SQL('SELECT COUNT(*) FROM {0}').format(sql.Identifier(base_table)))
            tbl_count = cursor.fetchone()[0]
            assert tbl_count > 0

            cursor.execute("""SELECT TABLE_NAME
                                FROM information_schema.tables
                               WHERE TABLE_NAME LIKE %s
                            ORDER BY TABLE_NAME""",
                           ['{0}%'.format(base_table)]),
            res = [x.table_name for x in cursor]
            assert res == ['{0}'.format(base_table),
                           '{0}_0_12'.format(base_table),
                           '{0}_13_25'.format(base_table),
                           '{0}_26_38'.format(base_table),
                           '{0}_39_51'.format(base_table),
                           '{0}_52_63'.format(base_table),
                           '{0}_64_75'.format(base_table),
                           '{0}_76_87'.format(base_table),
                           '{0}_88_99'.format(base_table)]

    # Re-partition back to the default 4 shards so that we do not change state for other tests
    result = runner.invoke(dirbs_db_cli, ['repartition', '--num-physical-shards=4'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    with db_conn, db_conn.cursor() as cursor:
        for base_table in partitioned_tables:
            cursor.execute(sql.SQL('SELECT COUNT(*) FROM {0}').format(sql.Identifier(base_table)))
            tbl_count = cursor.fetchone()[0]
            assert tbl_count > 0

            cursor.execute("""SELECT TABLE_NAME
                                FROM information_schema.tables
                               WHERE TABLE_NAME LIKE %s
                            ORDER BY TABLE_NAME""",
                           ['{0}%'.format(base_table)]),
            res = [x.table_name for x in cursor]
            assert res == ['{0}'.format(base_table),
                           '{0}_0_24'.format(base_table),
                           '{0}_25_49'.format(base_table),
                           '{0}_50_74'.format(base_table),
                           '{0}_75_99'.format(base_table)]
