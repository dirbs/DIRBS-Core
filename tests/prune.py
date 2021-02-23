"""
Data pruning unit tests.

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

import datetime

import pytest
from click.testing import CliRunner

from dirbs.cli.prune import cli as dirbs_prune_cli
from dirbs.cli.classify import cli as dirbs_classify_cli
from dirbs.importer.gsma_data_importer import GSMADataImporter
from _fixtures import *  # noqa: F403, F401
from _helpers import get_importer, expect_success, from_cond_dict_list_to_cond_list
from _importer_params import OperatorDataParams, StolenListParams, GSMADataParams


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                             operator='1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False)],
                         indirect=True)
def test_persistent_network_imeis(postgres, db_conn, tmpdir, logger, operator_data_importer, mocked_config):
    """Test Depot ID 96759/1.

    Verify DIRBS core instance maintain a persistent list of all IMEIs that
    have ever been seen in operator data. Verify the all-time seen IMEI list does not contain
    subscriber information. Verify the all-time seen IMEI list is not be impacted when operator data is pruned.
    Verify the all-time seen IMEI list  record the date that the IMEI first appeared on a per-operator basis.
    """
    operator_data_importer.import_data()

    # compare the results before and after pruning
    with db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm, first_seen '
                    'FROM network_imeis ORDER BY imei_norm ASC')
        result_list_before_prune = [(res.imei_norm, res.first_seen.strftime('%Y%m%d'))
                                    for res in cur.fetchall()]

    runner = CliRunner()
    result = runner.invoke(dirbs_prune_cli, ['triplets'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    with db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm, first_seen '
                    'FROM network_imeis ORDER BY imei_norm ASC')

        result_list_after_prune = [(res.imei_norm, res.first_seen.strftime('%Y%m%d'))
                                   for res in cur.fetchall()]

        assert result_list_after_prune == [('01376803870943', '20161107'), ('21123131308878', '20161110'),
                                           ('21123131308879', '20161111'), ('21260934121733', '20161106'),
                                           ('21260934475212', '20161130'), ('21782434077450', '20161124'),
                                           ('21782434077459', '20161118'), ('38245933AF987001', '20161109'),
                                           ('38709433212541', '20161113'), ('38847733370026', '20161104'),
                                           ('64220297727231', '20161112'), ('64220299727231', '20161112')]

        assert result_list_before_prune == result_list_after_prune


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20110101,8888#888622222,123456789012345,123456789012345\n'
                                     '20110101,88888888622222,123456789012345,123456789012345\n'
                                     '20110101,21111111111111,125456789012345,123456789012345\n'
                                     '20110101,21111111111112,125456789012345,123456789012345\n'
                                     '20110101,88888862222209,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_emptynontac_july_2016.txt')],
                         indirect=True)
@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='testData1-sample_stolen_list-anonymized.csv')],
                         indirect=True)
def test_prune_classification_state(db_conn, metadata_db_conn, tmpdir, logger, mocked_config,
                                    operator_data_importer, stolen_list_importer, monkeypatch,
                                    gsma_tac_db_importer, postgres, mocked_statsd):
    """Test Depot ID not known yet.

    A regulator/partner should be able to run a CLI command to prune classification_state table.
    It will remove any classification state data related to obsolete conditions and
    data with end_date is earlier than the start of the retention window.
    """
    # Step 1:
    # import gsma_dump empty non tac and classify for all the conditions
    # ['gsma_not_found', 'local_stolen', 'duplicate_mk1', 'malformed_imei', 'not_on_registration_list', ..]
    # classification_state_table contains records for cond_name "gsma_not_found". They all have end_date==None

    # Step 2 - TEST RETENTION WINDOW:
    # CLI prune will delete rows where the end_date is earlier than the start of the retention window.
    # retention_months=6
    # curr_date = datetime.date(2017, 7, 13)
    # Import different gsma_db and classify to have different end date for gsma_not_found records in
    # the classification table.

    # Step 3 - TEST CONDITIONS NOT EXISTING:
    # CLI prune for classification_state will look at the current configured conditions and
    # remove any entries corresponding to cond_names that no longer exist in the config.
    # Load a new yaml file without stolen_list condition and run the prune CLI command to test.
    # -- yaml cond config list:
    # ['gsma_not_found', 'malformed_imei', 'not_on_registration_list']
    # -- classification_state condition list:
    # ['gsma_not_found', 'local_stolen', 'malformed_imei', 'not_on_registration_list']

    # Step 1
    operator_data_importer.import_data()
    stolen_list_importer.import_data()
    gsma_tac_db_importer.import_data()

    runner = CliRunner()
    db_conn.commit()
    runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date', '20170713'],
                  obj={'APP_CONFIG': mocked_config})

    with db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm, cond_name, end_date FROM classification_state ORDER BY cond_name, imei_norm')
        res_list = cur.fetchall()
        assert len(res_list) == 32
        assert [(x.imei_norm, x.cond_name, x.end_date) for x in res_list] == \
               [('21111111111111', 'gsma_not_found', None),
                ('21111111111112', 'gsma_not_found', None),
                ('8888#888622222', 'gsma_not_found', None),
                ('88888862222209', 'gsma_not_found', None),
                ('88888888622222', 'gsma_not_found', None),
                ('12432807272315', 'local_stolen', None),
                ('12640904324427', 'local_stolen', None),
                ('12640904372723', 'local_stolen', None),
                ('12727231272313', 'local_stolen', None),
                ('12875502464321', 'local_stolen', None),
                ('12875502572723', 'local_stolen', None),
                ('12875507272312', 'local_stolen', None),
                ('12904502843271', 'local_stolen', None),
                ('12909602432585', 'local_stolen', None),
                ('12909602872723', 'local_stolen', None),
                ('12922902206948', 'local_stolen', None),
                ('12922902243260', 'local_stolen', None),
                ('12922902432742', 'local_stolen', None),
                ('12922902432776', 'local_stolen', None),
                ('12957272313271', 'local_stolen', None),
                ('17272317272723', 'local_stolen', None),
                ('56773605727231', 'local_stolen', None),
                ('64220204327947', 'local_stolen', None),
                ('64220297727231', 'local_stolen', None),
                ('72723147267231', 'local_stolen', None),
                ('72723147267631', 'local_stolen', None),
                ('8888#888622222', 'malformed_imei', None),
                ('21111111111111', 'not_on_registration_list', None),
                ('21111111111112', 'not_on_registration_list', None),
                ('8888#888622222', 'not_on_registration_list', None),
                ('88888862222209', 'not_on_registration_list', None),
                ('88888888622222', 'not_on_registration_list', None)]

        # Step 2
        # all records have end_date == None. Classify twice to have records with different end_date
        # first classification
        with get_importer(GSMADataImporter,
                          db_conn,
                          metadata_db_conn,
                          mocked_config.db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          GSMADataParams(filename='gsma_not_found_anonymized.txt')) as imp:
            expect_success(imp, 1, db_conn, logger)

        runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date', '20170713'],
                      obj={'APP_CONFIG': mocked_config})

        # with db_conn.cursor() as cur:
        cur.execute("""SELECT imei_norm, cond_name, end_date
                         FROM classification_state
                     ORDER BY cond_name, imei_norm""")
        res_list = cur.fetchall()

        gsma_not_found_list = [(x.imei_norm, x.cond_name, x.end_date) for x in res_list
                               if x.cond_name == 'gsma_not_found']

        assert gsma_not_found_list == [('21111111111111', 'gsma_not_found', None),
                                       ('21111111111112', 'gsma_not_found', None),
                                       ('8888#888622222', 'gsma_not_found', None),
                                       ('88888862222209', 'gsma_not_found', None),
                                       ('88888888622222', 'gsma_not_found', datetime.date(2017, 7, 13))]
        # second classification
        with get_importer(GSMADataImporter,
                          db_conn,
                          metadata_db_conn,
                          mocked_config.db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          GSMADataParams(filename='prune_classification_state_gsma.txt')) as imp:
            expect_success(imp, 1, db_conn, logger)

        runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date', '20160101'],
                      obj={'APP_CONFIG': mocked_config})

        # with db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm, cond_name, end_date '
                    '  FROM classification_state '
                    'ORDER BY cond_name, imei_norm, end_date')
        res_list = cur.fetchall()

        gsma_not_found_list = [(x.imei_norm, x.cond_name, x.end_date) for x in res_list if
                               x.cond_name == 'gsma_not_found']

        assert gsma_not_found_list == [('21111111111111', 'gsma_not_found', datetime.date(2016, 1, 1)),
                                       ('21111111111112', 'gsma_not_found', datetime.date(2016, 1, 1)),
                                       ('8888#888622222', 'gsma_not_found', None),
                                       ('88888862222209', 'gsma_not_found', None),
                                       ('88888888622222', 'gsma_not_found', datetime.date(2017, 7, 13)),
                                       ('88888888622222', 'gsma_not_found', None)]

        # Step 3
        # Expect not to be in classification_state table after prune:
        # IMEIs 21111111111111 and 21111111111112 for condition gsma_not found (due to end_date)
        # IMEIs for condition stolen_list (due to condition no longer exist)

        # this commit is to remove locks from the classification_state table so that
        # the table can be dropped inside the prune. The locks were activated by the CLI to classify.
        db_conn.commit()

        cond_dict_list = [{'label': 'gsma_not_found',
                           'reason': 'TAC not found in GSMA TAC database',
                           'grace_period_days': 30,
                           'blocking': True,
                           'dimensions': [{'module': 'gsma_not_found'}]
                           },
                          {'label': 'malformed_imei',
                           'reason': 'Invalid characters detected in IMEI',
                           'grace_period_days': 0,
                           'blocking': False,
                           'dimensions': [{'module': 'malformed_imei'}]
                           },
                          {'label': 'not_on_registration_list',
                           'reason': 'IMEI not found on local registration list',
                           'grace_period_days': 0,
                           'blocking': True,
                           'max_allowed_matching_ratio': 1.0,
                           'dimensions': [{'module': 'not_on_registration_list'}]
                           }]

        monkeypatch.setattr(mocked_config, 'conditions', from_cond_dict_list_to_cond_list(cond_dict_list))
        with db_conn.cursor() as cur:
            result = runner.invoke(dirbs_prune_cli, ['--curr-date', '20170913',
                                                     'classification_state'],
                                   obj={'APP_CONFIG': mocked_config})

            assert result.exit_code == 0
            # ITEMS REMOVED
            # [('17272317272723', 'local_stolen', None), ('12909602872723', 'local_stolen', None),
            # ('12875502572723', 'local_stolen', None), ('12875507272312', 'local_stolen', None),
            # ('64220297727231', 'local_stolen', None), ('12909602432585', 'local_stolen', None),
            # ('64220204327947', 'local_stolen', None), ('72723147267631', 'local_stolen', None),
            # ('72723147267231', 'local_stolen', None), ('12922902243260', 'local_stolen', None),
            # ('12875502464321', 'local_stolen', None), ('12922902432776', 'local_stolen', None),
            # ('12957272313271', 'local_stolen', None), ('12640904324427', 'local_stolen', None),
            # ('12904502843271', 'local_stolen', None), ('12922902432742', 'local_stolen', None),
            # ('12432807272315', 'local_stolen', None), ('12922902206948', 'local_stolen', None),
            # ('56773605727231', 'local_stolen', None), ('12727231272313', 'local_stolen', None),
            # ('12640904372723', 'local_stolen', None),
            # ('21111111111111', 'gsma_not_found', datetime.date(2016, 1, 1)),
            # ('21111111111112', 'gsma_not_found', datetime.date(2016, 1, 1))]

            cur.execute('SELECT imei_norm, cond_name, end_date '
                        'FROM classification_state '
                        'ORDER BY cond_name, imei_norm, end_date')
            res_list = cur.fetchall()
            pruned_class_state_table = [(x.imei_norm, x.cond_name, x.end_date) for x in res_list]

            assert pruned_class_state_table == [('8888#888622222', 'gsma_not_found', None),
                                                ('88888862222209', 'gsma_not_found', None),
                                                ('88888888622222', 'gsma_not_found', datetime.date(2017, 7, 13)),
                                                ('88888888622222', 'gsma_not_found', None),
                                                ('8888#888622222', 'malformed_imei', None),
                                                ('21111111111111', 'not_on_registration_list', None),
                                                ('21111111111112', 'not_on_registration_list', None),
                                                ('8888#888622222', 'not_on_registration_list', None),
                                                ('88888862222209', 'not_on_registration_list', None),
                                                ('88888888622222', 'not_on_registration_list', None)]


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20110101,88888888622222,123456789012345,123456789012345\n'
                                     '20110101,21111111111111,125456789012345,123456789012345\n'
                                     '20110101,21111111111112,125456789012345,123456789012345\n'
                                     '20110101,88888862222209,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_emptynontac_july_2016.txt')],
                         indirect=True)
@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='testData1-sample_stolen_list-anonymized.csv')],
                         indirect=True)
def test_prune_blacklist(db_conn, metadata_db_conn, tmpdir, logger, mocked_config,
                         operator_data_importer, stolen_list_importer, monkeypatch,
                         gsma_tac_db_importer, postgres, mocked_statsd):
    """Verify that the blacklist prune command prune entries related to a specified condition only."""
    operator_data_importer.import_data()
    stolen_list_importer.import_data()
    gsma_tac_db_importer.import_data()

    runner = CliRunner()
    db_conn.commit()
    runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date', '20170713'],
                  obj={'APP_CONFIG': mocked_config})

    with db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm, cond_name, end_date FROM classification_state ORDER BY cond_name, imei_norm')
        res_list = cur.fetchall()
        assert len(res_list) == 29
        assert [(x.imei_norm, x.cond_name, x.end_date) for x in res_list] == \
               [('21111111111111', 'gsma_not_found', None),
                ('21111111111112', 'gsma_not_found', None),
                ('88888862222209', 'gsma_not_found', None),
                ('88888888622222', 'gsma_not_found', None),
                ('12432807272315', 'local_stolen', None),
                ('12640904324427', 'local_stolen', None),
                ('12640904372723', 'local_stolen', None),
                ('12727231272313', 'local_stolen', None),
                ('12875502464321', 'local_stolen', None),
                ('12875502572723', 'local_stolen', None),
                ('12875507272312', 'local_stolen', None),
                ('12904502843271', 'local_stolen', None),
                ('12909602432585', 'local_stolen', None),
                ('12909602872723', 'local_stolen', None),
                ('12922902206948', 'local_stolen', None),
                ('12922902243260', 'local_stolen', None),
                ('12922902432742', 'local_stolen', None),
                ('12922902432776', 'local_stolen', None),
                ('12957272313271', 'local_stolen', None),
                ('17272317272723', 'local_stolen', None),
                ('56773605727231', 'local_stolen', None),
                ('64220204327947', 'local_stolen', None),
                ('64220297727231', 'local_stolen', None),
                ('72723147267231', 'local_stolen', None),
                ('72723147267631', 'local_stolen', None),
                ('21111111111111', 'not_on_registration_list', None),
                ('21111111111112', 'not_on_registration_list', None),
                ('88888862222209', 'not_on_registration_list', None),
                ('88888888622222', 'not_on_registration_list', None)]

        # invoke condition based pruning for local_stolen
        runner.invoke(dirbs_prune_cli, ['blacklist', 'local_stolen'], obj={'APP_CONFIG': mocked_config})
        cur.execute('SELECT imei_norm, cond_name, end_date FROM classification_state ORDER BY cond_name, imei_norm')
        res_list = cur.fetchall()
        assert len(res_list) == 29
        for x in res_list:
            if x.cond_name == 'local_stolen':
                assert x.end_date is not None
            else:
                assert x.end_date is None

        # invoke runner to prune all
        runner.invoke(dirbs_prune_cli, ['blacklist', '--prune-all'], obj={'APP_CONFIG': mocked_config})
        cur.execute('SELECT imei_norm, cond_name, end_date FROM classification_state ORDER BY cond_name, imei_norm')
        res_list = cur.fetchall()
        assert len(res_list) == 29
        for x in res_list:
            assert x.end_date is not None
