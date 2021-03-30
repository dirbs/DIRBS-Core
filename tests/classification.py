"""
Classification unit tests.

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
from datetime import timedelta
import os
import zipfile

from psycopg2 import sql
import pytest
from click.testing import CliRunner

from dirbs.cli.prune import cli as dirbs_prune_cli
from dirbs.config.region import OperatorConfig
from dirbs.config.common import ConfigParseException
from dirbs.cli.classify import cli as dirbs_classify_cli
from dirbs.importer.operator_data_importer import OperatorDataImporter
from dirbs.importer.golden_list_importer import GoldenListImporter
from dirbs.importer.subscriber_reg_list_importer import SubscribersListImporter  # noqa: F401
from dirbs.cli.listgen import cli as dirbs_listgen_cli
from _fixtures import *  # noqa: F403, F401
from _helpers import get_importer, expect_success, matching_imeis_for_cond_name, find_subdirectory_in_dir, \
    logger_stream_contents, logger_stream_reset, invoke_cli_classify_with_conditions_helper, \
    from_cond_dict_list_to_cond_list, find_file_in_dir, from_op_dict_list_to_op_list, from_amnesty_dict_to_amnesty_conf
from _importer_params import GSMADataParams, OperatorDataParams, StolenListParams, \
    RegistrationListParams, GoldenListParams, BarredListParams, BarredTacListParams, SubscribersListParams, \
    DeviceAssociationListParams, MonitoringListParams

base_dummy_cond_config = {
    'label': 'dummy_test_condition',
    'reason': 'some random reason'
}


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161101,01376803870943,123456789012345,123456789012345\n'
                                     '20161101,64220297727231,123456789012345,123456789012345\n'
                                     '20161101,64220299727231,125456789012345,123456789012345\n'
                                     '20161101,64220498727231,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='tac_api_gsma_db.txt',
                                         extract=False)],
                         indirect=True)
@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(content='IMEI,reporting_date,status\n'
                                                   '12432807272315,,\n56773605727231,,\n'
                                                   '64220204327947,,\n64220297727231,,\n'
                                                   '72723147267231,,\n72723147267631,,\n',
                                           extract=False
                                           )],
                         indirect=True)
def test_classification_table_structure_after_pruning(postgres,
                                                      operator_data_importer,
                                                      gsma_tac_db_importer,
                                                      stolen_list_importer, db_conn, mocked_config):
    """Test Depot ID not known yet.

    Verify that classification_state table maintains the original structure after pruning,
    indexes and sequences in particular.
    """
    runner = CliRunner()  # noqa
    result = runner.invoke(dirbs_prune_cli, ['--curr-date', '20170913',
                                             'classification_state'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    operator_data_importer.import_data()
    gsma_tac_db_importer.import_data()
    stolen_list_importer.import_data()
    db_conn.commit()

    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check',
                                                '--disable-sanity-checks',
                                                '--curr-date', '20171130'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_is_test_tac_anonymized_20161101_20161130.csv',
                             operator='1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False)],
                         indirect=True)
def test_is_tac(db_conn, operator_data_importer, mocked_config,
                postgres, logger, monkeypatch):
    """Test Depot not known yet.

    Verify a regulator/partner should be able to classify test tac IMEIs.
    First six digits of the Test IMEI features:
    - first 2 digits are '00';
    - exclude for IMEIs with characters;
    - the third and fourth digits can be either:
          '10' followed by two digits both between 1 and 17
       OR '44', '86' or '91'

    e.g. first six digits of the Test IMEI :
    001 001-
    001 017

    00 44
    00 86
    00 91
    """
    expect_success(operator_data_importer, 14, db_conn, logger)

    # invalid test TAC IMEIs
    # 20161104,001018333700263
    # 20161113,001038332125410
    # 20161107,009237680387094
    # 20161124,008568038709433
    # 20161118,001124340774590
    # 20161124,001019434077450
    # 20161130,2A2609344752127
    # 20161106,2a2609341217330
    # valid test TAC IMEIs
    # 20161121,001001313088793
    # 20161121,001005131308793
    # 20161121,001013313088793
    # 20161121,004431313088793
    # 20161121,008631313088793
    # 20161121,009131313088793
    cond_list = [{
        'label': 'is_test_tac',
        'grace_period_days': 30,
        'blocking': True,
        'sticky': False,
        'reason': 'Found test tac',
        'dimensions': [{'module': 'is_test_tac'}]
    }]
    valid_test_tac_imeis_list = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                           curr_date='20161101', db_conn=db_conn,
                                                                           classify_options=['--no-safety-check'])
    invalid_test_tac = ['00101833370026',
                        '00103833212541',
                        '00923768038709',
                        '00856803870943',
                        '00112434077459',
                        '00101943407745',
                        '2A260934475212',
                        '2a260934121733']

    valid_test_tac = ['00100131308879',
                      '00100513130879',
                      '00101331308879',
                      '00443131308879',
                      '00863131308879',
                      '00913131308879']

    assert all([x not in valid_test_tac_imeis_list for x in invalid_test_tac])
    assert all([x in valid_test_tac_imeis_list for x in valid_test_tac])


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='testData1-sample_stolen_list-anonymized.csv')],
                         indirect=True)
def test_blocking_condition(db_conn, tmpdir, logger, stolen_list_importer, mocked_config, monkeypatch):
    """Test Depot ID 96755/10.

    Verify that block date is set only if the condition is blocking.
    Step 1) blocking: False, verify that no block date is set into classification_table after classifying with
            curr_date (2017, 4, 8)
    Step 2) blocking: True, verify that block date is set into classification_table after classifying for the
            first time with blocking=True and with curr_date (2017, 4, 8)
    Step 3) blocking: True, verify that IMIEs with block date not null do not change block_date after classifying with
            curr_date (2017, 4, 9)
    Step 4) blocking: False, verify that IMIEs with block date not null have block date set to null
    """
    stolen_imeis_list_imported = ['12432807272315', '12640904324427', '12640904372723',
                                  '12727231272313', '12875502464321', '12875502572723',
                                  '12875507272312', '12904502843271', '12909602432585',
                                  '12909602872723', '12922902206948', '12922902243260',
                                  '12922902432742', '12922902432776', '12957272313271',
                                  '17272317272723', '56773605727231', '64220204327947',
                                  '64220297727231', '72723147267231', '72723147267631']

    stolen_list_importer.import_data()
    # Need this commit to unlock stolen_list table that will be used in the local_stolen query inside
    # update_state_table method of Condition class
    db_conn.commit()

    # Step 1
    cond_list = [{
        'label': 'local_stolen',
        'grace_period_days': 87,
        'blocking': False,
        'sticky': False,
        'reason': 'IMEI found on local stolen list',
        'dimensions': [{'module': 'stolen_list'}]
    }]

    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                               curr_date='20170408')
    # date_days_ago is '2017-1-11'
    date_days_ago = (datetime.date(2017, 4, 8) - timedelta(days=87)).isoformat()

    with db_conn.cursor() as cursor:
        cursor.execute(sql.SQL("""SELECT imei_norm, block_date, cond_name
                                    FROM classification_state
                                   WHERE end_date IS NULL
                                     AND block_date IS NULL
                                ORDER BY imei_norm, cond_name"""))
        res = cursor.fetchall()
        imeis_list = [x.imei_norm for x in res]
        comon_cond_name = 'local_stolen' if \
            len([x.cond_name for x in res if x.cond_name != 'local_stolen']) == 0 else ''
        common_block_date = None if len([x.cond_name for x in res if x.block_date is not None]) == 0 else ''
        assert stolen_imeis_list_imported == imeis_list
        assert comon_cond_name == 'local_stolen'
        assert common_block_date is None
        db_conn.commit()

        # Step 2
        cond_list = [{
            'label': 'local_stolen',
            'grace_period_days': 87,
            'blocking': True,
            'sticky': False,
            'reason': 'IMEI found on local stolen list',
            'dimensions': [{'module': 'stolen_list'}]
        }]
    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                               curr_date='20170408')
    # date_days_ago is '2017-1-11'
    date_days_ago = (datetime.date(2017, 4, 8) - timedelta(days=87)).isoformat()

    with db_conn.cursor() as cursor:
        cursor.execute(sql.SQL("""SELECT imei_norm, block_date, cond_name
                                    FROM classification_state
                                   WHERE end_date IS NULL
                                     AND block_date > %s::date
                                ORDER BY imei_norm, cond_name"""), [date_days_ago])

        res = cursor.fetchall()
        imeis_list = [x.imei_norm for x in res]
        comon_cond_name = 'local_stolen' if \
            len([x.cond_name for x in res if x.cond_name != 'local_stolen']) == 0 else ''
        common_block_date = '20170704' if \
            len([x.cond_name for x in res if x.block_date != datetime.date(2017, 7, 4)]) == 0 else ''
        assert stolen_imeis_list_imported == imeis_list
        assert comon_cond_name == 'local_stolen'
        assert common_block_date == '20170704'
        db_conn.commit()

        # Step 3
        cond_list = [{
            'label': 'local_stolen',
            'grace_period_days': 87,
            'blocking': True,
            'sticky': False,
            'reason': 'IMEI found on local stolen list',
            'dimensions': [{'module': 'stolen_list'}]
        }]
    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                               curr_date='20170409')
    # date_days_ago is '2017-1-12'
    date_days_ago = (datetime.date(2017, 4, 9) - timedelta(days=87)).isoformat()

    with db_conn.cursor() as cursor:
        cursor.execute(sql.SQL("""SELECT imei_norm, block_date, cond_name
                                    FROM classification_state
                                   WHERE end_date IS NULL
                                     AND block_date > %s::date
                                ORDER BY imei_norm, cond_name"""), [date_days_ago])

        res = cursor.fetchall()
        imeis_list = [x.imei_norm for x in res]
        comon_cond_name = 'local_stolen' if len(
            [x.cond_name for x in res if x.cond_name != 'local_stolen']) == 0 else ''
        common_block_date = '20170704' if len(
            [x.cond_name for x in res if x.block_date != datetime.date(2017, 7, 4)]) == 0 else ''
        assert stolen_imeis_list_imported == imeis_list
        assert comon_cond_name == 'local_stolen'
        assert common_block_date == '20170704'
        db_conn.commit()

        # Step 4
        cond_list = [{
            'label': 'local_stolen',
            'grace_period_days': 87,
            'blocking': False,
            'sticky': False,
            'reason': 'IMEI found on local stolen list',
            'dimensions': [{'module': 'stolen_list'}]
        }]
    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                               curr_date='20170409')
    # date_days_ago is '2017-1-12'
    date_days_ago = (datetime.date(2017, 4, 9) - timedelta(days=87)).isoformat()

    with db_conn.cursor() as cursor:
        cursor.execute(sql.SQL("""SELECT imei_norm, block_date, cond_name
                                    FROM classification_state
                                   WHERE end_date IS NULL
                                     AND block_date IS NULL
                                ORDER BY imei_norm, cond_name"""), [date_days_ago])

        res = cursor.fetchall()
        imeis_list = [x.imei_norm for x in res]
        comon_cond_name = 'local_stolen' if len(
            [x.cond_name for x in res if x.cond_name != 'local_stolen']) == 0 else ''
        common_block_date = None if len([x.cond_name for x in res if x.block_date is not None]) == 0 else ''
        assert stolen_imeis_list_imported == imeis_list
        assert comon_cond_name == 'local_stolen'
        assert common_block_date is None


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='sample_stolen_list_gen_grace_anonymized.csv')],
                         indirect=True)
@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_list_gen_grace_anonymized_20161101_20161130.csv',
                             operator='operator1',
                             extract=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             perform_unclean_checks=False
                         )],
                         indirect=True)
def test_list_gen_grace_period(db_conn, tmpdir, logger, stolen_list_importer, monkeypatch,
                               operator_data_importer, postgres, mocked_config):
    """Test Depot ID not known yet.

    Verify that it is possible to blacklist IMEIs if current date is newer than or equal to block_date
    """
    # DIRBS-LISTGEN current date is older than block_date
    # CLASSIFY date=20161115 grace_per=30 block_date=20161215
    # LIST_GEN_curr_date=20161214 curr_date < block_date
    stolen_list_importer.import_data()
    operator_data_importer.import_data()
    db_conn.commit()

    cond_list = [{
        'label': 'local_stolen',
        'grace_period_days': 30,
        'blocking': True,
        'sticky': False,
        'reason': 'IMEI found on local stolen list',
        'dimensions': [{'module': 'stolen_list'}],
        'max_allowed_matching_ratio': 1.0
    }]
    monkeypatch.setattr(mocked_config.listgen_config, 'lookback_days', 180)
    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                               classify_options=['--no-safety-check'], curr_date='20161115')
    db_conn.commit()
    output_dir = str(tmpdir)
    runner = CliRunner()
    result = runner.invoke(dirbs_listgen_cli, ['--curr-date', '20161214', output_dir],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    list_gen_path = find_subdirectory_in_dir('listgen*', output_dir)
    zip_fn = find_file_in_dir('*blacklist.zip', list_gen_path)
    with zipfile.ZipFile(zip_fn, 'r') as zf:
        zf.extractall(path=list_gen_path)

    # EMPTY BLACK LIST
    fn = find_file_in_dir('*blacklist.csv', list_gen_path)
    with open(fn, 'r') as bl:
        bl_content = bl.readlines()
        assert len(bl_content) == 1
        assert bl_content == ['imei,block_date,reasons\n']

    zip_fn = find_file_in_dir('*notifications_operator1.zip', list_gen_path)
    with zipfile.ZipFile(zip_fn, 'r') as zf:
        zf.extractall(path=list_gen_path)

    # NOT EMPTY NOTIFICATION LIST
    fn = find_file_in_dir('*notifications_operator1.csv', list_gen_path)
    with open(fn, 'r') as nl:
        nl_content = nl.readlines()
        assert len(nl_content) == 2

    # DIRBS-LISTGEN current date is newer than block_date
    # CLASSIFY date=20161115 grace_per=30 block_date=20161215
    # LIST_GEN_curr_date=20161216 curr_date > block_date
    with db_conn.cursor() as cur:
        cur.execute('TRUNCATE classification_state')
    db_conn.commit()

    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                               classify_options=['--no-safety-check'], curr_date='20161115')

    db_conn.commit()
    one_output_dir = os.path.join(output_dir, 'one')
    os.makedirs(one_output_dir, exist_ok=True)
    result = runner.invoke(dirbs_listgen_cli, ['--curr-date', '20161216', one_output_dir],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    list_gen_path = find_subdirectory_in_dir('listgen*', one_output_dir)
    zip_fn = find_file_in_dir('*blacklist.zip', list_gen_path)
    with zipfile.ZipFile(zip_fn, 'r') as zf:
        zf.extractall(path=list_gen_path)

    # NOT EMPTY BLACK LIST
    fn = find_file_in_dir('*blacklist.csv', list_gen_path)
    with open(fn, 'r') as bl:
        bl_content = bl.readlines()
        assert len(bl_content) == 22

    zip_fn = find_file_in_dir('*notifications_operator1.zip', list_gen_path)
    with zipfile.ZipFile(zip_fn, 'r') as zf:
        zf.extractall(path=list_gen_path)

    # EMPTY NOTIFICATION LIST
    fn = find_file_in_dir('*notifications_operator1.csv', list_gen_path)
    with open(fn, 'r') as nl:
        nl_content = nl.readlines()
        assert len(nl_content) == 1
        assert nl_content == ['imei,imsi,msisdn,block_date,reasons\n']

    # DIRBS-LISTGEN current date is equal to block_date
    # CLASSIFY date=20161115 grace_per=30 block_date=20161215
    # LIST_GEN_curr_date=20161215 curr_date == block_date
    with db_conn.cursor() as cur:
        cur.execute('TRUNCATE classification_state')
    db_conn.commit()

    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                               classify_options=['--no-safety-check'], curr_date='20161115')
    # LIST-GEN
    db_conn.commit()
    output_dir_two = os.path.join(output_dir, 'two')
    os.makedirs(output_dir_two, exist_ok=True)
    result = runner.invoke(dirbs_listgen_cli, ['--curr-date', '20161215', output_dir_two],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    list_gen_path = find_subdirectory_in_dir('listgen*', output_dir_two)
    zip_fn = find_file_in_dir('*blacklist.zip', list_gen_path)
    with zipfile.ZipFile(zip_fn, 'r') as zf:
        zf.extractall(path=list_gen_path)

    # NOT EMPTY BLACK LIST
    fn = find_file_in_dir('*blacklist.csv', list_gen_path)
    with open(fn, 'r') as bl:
        bl_content = bl.readlines()
        assert len(bl_content) == 22

    zip_fn = find_file_in_dir('*notifications_operator1.zip', list_gen_path)
    with zipfile.ZipFile(zip_fn, 'r') as zf:
        zf.extractall(path=list_gen_path)

    # EMPTY NOTIFICATION LIST
    fn = find_file_in_dir('*notifications_operator1.csv', list_gen_path)
    with open(fn, 'r') as nl:
        nl_content = nl.readlines()
        assert len(nl_content) == 1
        assert nl_content == ['imei,imsi,msisdn,block_date,reasons\n']


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_international_roamer_anonymized_20161101_20161201.csv',
                             operator='1',
                             extract=False,
                             perform_region_checks=False,
                             perform_home_network_check=False
                         )],
                         indirect=True)
def test_used_by_international_roamer_dim(db_conn, mocked_config, operator_data_importer, monkeypatch):
    """Test Depot ID not known yet.

    Verify that "Used by International roamer" dimension returns true when IMEI is a member of a
    tuple with an IMSI whose MCC is not in the list of configured MCCs for DIRBS.
    """
    # 13 rows in operator data file.

    # Check date in range lookback_days (11 valid rows):
    # 11 rows have valid date in range (20161130, 20161128) with lookback_days=2
    # 2 invalid IMEIs (imei=36232323232322, imei=36232323232323) has date older than: 20161128

    # Check MCC not in configured MCC for DIRBS. Among the 11 rows with data in-range:
    # 2 invalid IMEIs (imei= 36232323232321, imsi= 11111999999999), (imei= 36232323232329, imsi= 11112999999990)
    # with MCC in in the list of configured MCCs (mcc=['111%'] from yaml file).

    # 9 total matching IMEIs expected.
    operator_data_importer.import_data()
    db_conn.commit()

    cond_list = [{
        'label': 'used_by_international_roamer',
        'grace_period_days': 0,
        'reason': 'IMEI found for international roamer',
        'dimensions': [{'module': 'used_by_international_roamer', 'parameters': {'lookback_days': 2}}]
    }]

    matching_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                classify_options=['--no-safety-check'],
                                                                db_conn=db_conn)
    assert len(matching_imeis) == 9
    expected_not_matching_imeis_due_to_date_list = ['36232323232322', '36232323232323']
    assert all(x not in matching_imeis for x in expected_not_matching_imeis_due_to_date_list)
    expected_not_matching_imeis_due_to_mcc_list = ['36232323232321', '36232323232329']
    assert all(x not in matching_imeis for x in expected_not_matching_imeis_due_to_mcc_list)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_local_non_dirbs_roamer_anonymized_20161101_20161201.csv',
                             operator='1',
                             extract=False,
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_used_by_local_non_dirbs_roamer_dim(db_conn, tmpdir, logger, mocked_config,
                                            operator_data_importer, monkeypatch):
    """Test Depot ID not known yet.

    Verify that "Used by International roamer" dimension returns true when IMEI
    is a member of a tuple with an IMSI whose MCC is in the list of configured MCCs, but MCC-MNC is not.
    """
    # valid mcc from yaml file: ['111%']
    # valid mcc_mnc pairs from yaml file: ['11402%', '11101%', '11103%']

    # valid rows must have: valid date, valid mcc, invalid mcc_mnc pair
    # e.g.
    # 20161130,36222222222222,11401888888888 --
    # valid: valid date, valid mcc (114), valid mnc (01), not valid pair (11401) or
    # valid: valid date, valid mcc (114), invalid mnc (01) to have not valid pair (11107)

    # 15 rows in operator data file.

    # 4 invalid IMEI:
    # date,imei,imsi:

    # 20161122,36232323232321,11101999999990
    # invalid: date older that 20161128, valid mcc(111), valid mnc (01), valid pair

    # 20161130,36222222222222,11201888888888 --
    # invalid: valid date, invalid mcc (112)

    # 20161130,36232323232323,11103999999999 --
    # invalid: valid date, valid mcc (111), valid mnc (03), valid pair

    # 20161129,36232323232324,11402999999990 --
    # invalid: valid date, valid mcc (111), valid mnc (01)

    # 11 total matching IMEIs expected.
    operator_data_importer.import_data()

    op_list = [{'id': 'operator1',
                'name': 'First',
                'mcc_mnc_pairs': [{'mcc': '111', 'mnc': '01'}]},
               {'id': 'operator2',
                'name': 'Second',
                'mcc_mnc_pairs': [{'mcc': '114', 'mnc': '02'}]},
               {'id': 'operator1',
                'name': 'First',
                'mcc_mnc_pairs': [{'mcc': '111', 'mnc': '03'}]}]

    op_list = [OperatorConfig(ignore_env=False, **o) for o in op_list]
    monkeypatch.setattr(mocked_config.region_config, 'operators', op_list)

    cond_list = [{
        'label': 'used_by_local_non_dirbs_roamer',
        'grace_period_days': 0,
        'reason': 'IMEI found for local non DIRBS roamer',
        'dimensions': [{'module': 'used_by_local_non_dirbs_roamer', 'parameters': {'lookback_days': 2}}]
    }]

    matching_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                classify_options=['--no-safety-check'],
                                                                db_conn=db_conn)

    assert len(matching_imeis) == 11
    expected_not_matching_imeis_due_to_date_list = ['36232323232321']
    assert all(x not in matching_imeis for x in expected_not_matching_imeis_due_to_date_list)
    expected_not_matching_imeis_due_to_mcc_mnc_list = ['36222222222222', '36232323232323', '36232323232324']
    assert all(x not in matching_imeis for x in expected_not_matching_imeis_due_to_mcc_mnc_list)
    assert ('36222222222228' in matching_imeis)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_used_by_dirbs_subscribers_anonymized_20161101_20161201.csv',
                             operator='1',
                             extract=False,
                             perform_region_checks=False,
                             perform_home_network_check=False
                         )],
                         indirect=True)
def test_used_by_dirbs_subscribers_dim(db_conn, tmpdir, logger, mocked_config, operator_data_importer, monkeypatch):
    """Test Depot ID not known yet.

    Verify that "Used by International roamer" dimension returns true when IMEI
    is a member of a tuple with an IMSI whose MCC is in the list of configured MCCs, but MNC is not.
    """
    # valid mcc from yaml file: ['111%']
    # valid mnc from yaml file: ['03%', '04%', '01%', '02%']

    # valid rows must have: valid date, valid mcc, invalid mnc

    # 15 rows in operator data file.

    # 4 invalid IMEI:
    # date,imei,imsi:

    # 20161122,36232323232321,11101999999990
    # invalid: date older that 20161128, valid mcc(111), valid mnc (01)

    # 20161130,36222222222222,11205888888888 --
    # invalid: valid date, invalid mcc (112), invalid mnc (05)

    # 20161130,36232323232323,11303999999999 --
    # invalid: valid date, invalid mcc (113), valid mnc (03)

    # 20161129,36232323232324,11105999999990 --
    # invalid: valid date, valid mcc (111), invalid mnc (05)

    # 10 total matching IMEIs expected.
    operator_data_importer.import_data()
    cond_list = [{
        'label': 'used_by_dirbs_subscriber',
        'grace_period_days': 0,
        'reason': 'IMEI found for DIRBS subscribers',
        'dimensions': [{'module': 'used_by_dirbs_subscriber', 'parameters': {'lookback_days': 2}}]
    }]

    matching_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                classify_options=['--no-safety-check'],
                                                                db_conn=db_conn)

    assert len(matching_imeis) == 10
    expected_not_matching_imeis_due_to_date_list = ['36232323232321']
    assert all(x not in matching_imeis for x in expected_not_matching_imeis_due_to_date_list)
    expected_not_matching_imeis_due_to_mcc_mnc_list = ['36222222222222', '36232323232323', '36232323232324']
    assert all(x not in matching_imeis for x in expected_not_matching_imeis_due_to_mcc_mnc_list)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20110101,64220496727231,123456789012345,123456789012345\n'
                                     '20110101,64220496727232,123456789012345,123456789012345',
                             operator='1',
                             extract=False,
                             perform_leading_zero_check=False,
                             perform_unclean_checks=False,
                             perform_home_network_check=False,
                             perform_region_checks=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_file_daterange_check=False)],
                         indirect=True)
@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(
                             content='IMEI,reporting_date,status\n'
                                     '642204967272316,,\n'
                                     '642202977272311,,\n'
                                     '642202043279479,,\n'
                                     '727231472672310,,')],
                         indirect=True)
def test_classification_state_not_dimension_stolen(db_conn, metadata_db_conn, tmpdir, logger, mocked_config,
                                                   operator_data_importer, stolen_list_importer, mocked_statsd,
                                                   monkeypatch):
    """Test Depot ID not known yet.

    A regulator/partner should be able to configure conditions based on the negation of one or
    more dimensions.
    """
    # Step 1 Import same operator data for 2 operators so that network_imeis is not unique in the IMEI space. This
    # catches a previou bug where EXCEPT ALL would result in incorrect inversion when the network_imeis was not
    # unique. The operator data contains 1 stolen IMEI and 1 non-stolen IMEI.
    # Step 2 Classify local_stolen IMEIs values with invert flag to to False and retrieve the set of IMEIs
    # Step 3 Classify local_stolen IMEIs values with invert flag to to True and retrieve the set of IMEIs
    # We expect to not find the above local_stolen IMEIs values in the result.
    # By default invert boolean flag is set to False.

    # Step 1
    operator_data_importer.import_data()

    # Import operator 2 data set
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          content='date,imei,imsi,msisdn\n'
                                  '20110101,64220496727231,123456789012345,123456789012345\n'
                                  '20110101,64220496727232,123456789012345,123456789012345',
                          operator='2',
                          extract=False,
                          perform_leading_zero_check=False,
                          perform_unclean_checks=False,
                          perform_home_network_check=False,
                          perform_region_checks=False,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                          perform_file_daterange_check=False)) as new_imp:
        expect_success(new_imp, 4, db_conn, logger)

    stolen_list_importer.import_data()
    db_conn.commit()

    cond_list = [{
        'label': 'local_stolen',
        'grace_period_days': 0,
        'reason': 'IMEI found on local stolen list',
        'dimensions': [{'module': 'stolen_list'}]
    }]
    stolen_list = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                             classify_options=['--no-safety-check'],
                                                             db_conn=db_conn)
    assert len(stolen_list) == 4
    assert stolen_list == ['64220204327947', '64220297727231', '64220496727231', '72723147267231']

    # Step 2
    cond_list_with_invert = [{
        'label': 'local_stolen',
        'grace_period_days': 0,
        'reason': 'IMEI found on local stolen list',
        'dimensions': [{'module': 'stolen_list', 'invert': True}]
    }]

    db_conn.commit()
    not_stolen_list = invoke_cli_classify_with_conditions_helper(cond_list_with_invert, mocked_config, monkeypatch,
                                                                 classify_options=['--no-safety-check'],
                                                                 db_conn=db_conn)
    assert len(not_stolen_list) == 1
    assert not_stolen_list == ['64220496727232']

    with db_conn.cursor() as c:
        c.execute('SELECT imei_norm FROM network_imeis')
        network_imeis_set = {x.imei_norm for x in c.fetchall()}
        assert len(network_imeis_set) == 2
        assert network_imeis_set == {'64220496727232', '64220496727231'}

        stolen_list_set = set(stolen_list)
        not_stolen_list_set = set(not_stolen_list)
        assert not set.intersection(stolen_list_set, not_stolen_list_set)
        assert network_imeis_set - stolen_list_set == not_stolen_list_set


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                             operator='1',
                             extract=False,
                             perform_leading_zero_check=False,
                             perform_unclean_checks=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_file_daterange_check=False)],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='testData1-gsmatac_operator1_operator4_anonymized.txt',
                                         extract=False)],
                         indirect=True)
def test_classification_state_not_gsma_not_found(db_conn, tmpdir, logger, mocked_config, monkeypatch,
                                                 gsma_tac_db_importer, operator_data_importer):
    """Test Depot ID not known yet.

    A regulator/partner should be able to configure conditions based on the negation of one or
    more dimensions.
    """
    # Step 1 Sample classification gsma_not_found IMEIs values with boolean flag for gsma dimension
    # set to False. Matching IMEIs: '01376803870943','64220297727231', '64220299727231'
    # Step 2 classification for NOT gsma_not_found (boolean flag for gsma dimension set to True for
    # negating the condition).
    # We expect to not find the above gsma_not_found IMEIs values in the result.
    # By default invert boolean flag is set to False.

    # Step 1
    gsma_tac_db_importer.import_data()
    operator_data_importer.import_data()

    cond_list = [{
        'label': 'gsma_not_found',
        'grace_period_days': 30,
        'reason': 'TAC not found in GSMA TAC database',
        'dimensions': [{'module': 'gsma_not_found'}]
    }]
    gsma_not_found_set = set(invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                        classify_options=['--no-safety-check'],
                                                                        db_conn=db_conn))
    with db_conn.cursor() as c:
        c.execute('SELECT imei_norm FROM network_imeis')
        network_imeis_set = {x.imei_norm for x in c.fetchall()}

    # Step 2
    cond_with_invert = [{
        'label': 'gsma_not_found',
        'grace_period_days': 30,
        'reason': 'TAC not found in GSMA TAC database',
        'dimensions': [{'module': 'gsma_not_found', 'invert': True}]
    }]
    db_conn.commit()
    gsma_found_set = set(invoke_cli_classify_with_conditions_helper(cond_with_invert, mocked_config, monkeypatch,
                                                                    classify_options=['--no-safety-check'],
                                                                    db_conn=db_conn))
    assert not set.intersection(gsma_found_set, gsma_not_found_set)
    assert network_imeis_set - gsma_found_set == gsma_not_found_set


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20110101,8888#888622222,123456789012345,123456789012345\n'
                                     '20110101,88888888622222,123456789012345,123456789012345\n'
                                     '20110101,8888888862222209,123456789012345,123456789012345\n'
                                     '20110101,88888862222209**,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_not_found_anonymized.txt')],
                         indirect=True)
@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list.csv')],
                         indirect=True)
def test_basic_cli_classification(postgres, operator_data_importer, gsma_tac_db_importer,
                                  registration_list_importer, db_conn, mocked_config):
    """Test Depot ID not known yet.

    A basic test to verify whether the CLI (dirbs-classify) runs without an instance.
    """
    operator_data_importer.import_data()
    gsma_tac_db_importer.import_data()
    registration_list_importer.import_data()

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    runner = CliRunner()  # noqa
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20110101,8888#888622222,123456789012345,123456789012345\n'
                                     '20110101,88888888622222,123456789012345,123456789012345\n'
                                     '20110101,21111111111111,125456789012345,123456789012345\n'
                                     '20110101,88888862222209,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_not_found_anonymized.txt')],
                         indirect=True)
@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list.csv')],
                         indirect=True)
def test_basic_cli_classification_set_current_date(postgres, operator_data_importer, mocked_config,
                                                   gsma_tac_db_importer, registration_list_importer, db_conn):
    """Test Depot ID not known yet.

    A basic test to verify whether the CLI (dirbs-classify) classifies on a certain date.
    """
    operator_data_importer.import_data()
    gsma_tac_db_importer.import_data()
    registration_list_importer.import_data()

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()
    with db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm FROM classification_state')
        res_list = cur.fetchall()
        assert len(res_list) == 0

    runner = CliRunner()  # noqa
    # only IMEI 64220496727231 from operator data input file is also in registration_list.
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date', '20110102'],
                           obj={'APP_CONFIG': mocked_config})

    with db_conn.cursor() as cur:
        cur.execute('SELECT * FROM classification_state ORDER BY imei_norm, cond_name')
        attr_list = [(res.imei_norm, res.start_date, res.cond_name) for res in cur.fetchall()]
        assert attr_list == [('21111111111111', datetime.date(2011, 1, 2), 'gsma_not_found'),
                             ('21111111111111', datetime.date(2011, 1, 2), 'not_on_registration_list'),
                             ('8888#888622222', datetime.date(2011, 1, 2), 'gsma_not_found'),
                             ('8888#888622222', datetime.date(2011, 1, 2), 'malformed_imei'),
                             ('8888#888622222', datetime.date(2011, 1, 2), 'not_on_registration_list'),
                             ('88888862222209', datetime.date(2011, 1, 2), 'gsma_not_found'),
                             ('88888862222209', datetime.date(2011, 1, 2), 'not_on_registration_list'),
                             ('88888888622222', datetime.date(2011, 1, 2), 'not_on_registration_list')]

    assert result.exit_code == 0


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20110101,8888#888622222,123456789012345,123456789012345\n'
                                     '20110101,88888888622222,123456789012345,123456789012345\n'
                                     '20110101,8888888862222209,123456789012345,123456789012345\n'
                                     '20110101,88888862222209**,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_not_found_anonymized.txt')],
                         indirect=True)
def test_basic_cli_classification_set_condition(postgres, operator_data_importer, gsma_tac_db_importer, db_conn,
                                                mocked_config):
    """Test Depot ID not known yet.

    A basic test to verify whether the CLI (dirbs-classify) classifies only on the conditions specified.
    """
    operator_data_importer.import_data()
    gsma_tac_db_importer.import_data()
    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm FROM classification_state')
        res_list = cur.fetchall()
        assert len(res_list) == 0

    runner = CliRunner()  # noqa
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date',
                                                '20181101', '--conditions', 'malformed_imei'],
                           obj={'APP_CONFIG': mocked_config})

    # IMEIs in classification_state table for all conditions:
    # [('88888862222209', datetime.date(2018, 11, 1), 'gsma_not_found'),
    #  ('8888#888622222', datetime.date(2018, 11, 1), 'gsma_not_found'),
    #  ('8888#888622222', datetime.date(2018, 11, 1), 'malformed_imei')]
    with db_conn.cursor() as cur:
        cur.execute('SELECT * FROM classification_state')
        attr_list = [(res.imei_norm, res.start_date, res.cond_name) for res in cur.fetchall()]
        assert attr_list == [('8888#888622222', datetime.date(2018, 11, 1), 'malformed_imei')]

    assert result.exit_code == 0


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                             operator='1',
                             extract=False,
                             perform_leading_zero_check=False,
                             perform_unclean_checks=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_file_daterange_check=False)],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='testData1-gsmatac_operator1_operator4_anonymized.txt',
                                         extract=False)],
                         indirect=True)
@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='testData1-sample_stolen_list-anonymized.csv')],
                         indirect=True)
def test_classification_state(db_conn, mocked_config, monkeypatch, gsma_tac_db_importer, operator_data_importer,
                              stolen_list_importer):
    """Test Depot ID 96757/1.

    Verify DIRBS core instance stores per-IMEI, per-condition information
    about each non-compliant IMEI.
    """
    gsma_tac_db_importer.import_data()
    operator_data_importer.import_data()
    stolen_list_importer.import_data()
    db_conn.commit()

    cond_list = [{
        'label': 'gsma_not_found',
        'grace_period_days': 30,
        'blocking': True,
        'sticky': True,
        'reason': 'TAC not found in GSMA TAC database',
        'dimensions': [{'module': 'gsma_not_found', 'parameters': {'ignore_rbi_delays': True}}]
    }, {
        'label': 'local_stolen',
        'grace_period_days': 0,
        'blocking': True,
        'sticky': False,
        'reason': 'IMEI found on local stolen list',
        'dimensions': [{'module': 'stolen_list'}]
    }]

    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                               classify_options=['--no-safety-check'])
    with db_conn.cursor() as c:
        c.execute('SELECT imei_norm, cond_name FROM classification_state '
                  'WHERE end_date IS NULL ORDER BY imei_norm, cond_name ASC')

        res_list = [(res[0], res[1]) for res in c.fetchall()]
        assert res_list == [('01376803870943', 'gsma_not_found'),
                            ('12432807272315', 'local_stolen'),
                            ('12640904324427', 'local_stolen'),
                            ('12640904372723', 'local_stolen'),
                            ('12727231272313', 'local_stolen'),
                            ('12875502464321', 'local_stolen'),
                            ('12875502572723', 'local_stolen'),
                            ('12875507272312', 'local_stolen'),
                            ('12904502843271', 'local_stolen'),
                            ('12909602432585', 'local_stolen'),
                            ('12909602872723', 'local_stolen'),
                            ('12922902206948', 'local_stolen'),
                            ('12922902243260', 'local_stolen'),
                            ('12922902432742', 'local_stolen'),
                            ('12922902432776', 'local_stolen'),
                            ('12957272313271', 'local_stolen'),
                            ('17272317272723', 'local_stolen'),
                            ('56773605727231', 'local_stolen'),
                            ('64220204327947', 'local_stolen'),
                            ('64220297727231', 'gsma_not_found'),
                            ('64220297727231', 'local_stolen'),
                            ('64220299727231', 'gsma_not_found'),
                            ('72723147267231', 'local_stolen'),
                            ('72723147267631', 'local_stolen')]


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False)],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='testData1-gsmatac_operator1_operator4_anonymized.txt',
                                         extract=False)],
                         indirect=True)
@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='testData1-sample_stolen_list-anonymized.csv')],
                         indirect=True)
def test_classification_state_with_golden_list(db_conn, metadata_db_conn, tmpdir, logger,
                                               gsma_tac_db_importer, operator_data_importer,
                                               mocked_config, stolen_list_importer, mocked_statsd,
                                               postgres, monkeypatch):
    """Test Depot ID 96865.

    Verify DIRBS core instance stores per-IMEI, per-condition information
    about each non-compliant IMEI except golden_list IMEIs.
    Golden IMEIs are 32-character hex string, MD5 hashed.
    """
    # First Phase:
    # 24 matching IMEIs without importing any golden_list, such as:
    # IMEIs '64220204327947', '12875502464321'.
    # Generate blacklist and check IMEIs '64220204327947', '12875502464321' are included.

    # Second Phase:
    # Add golden_list containing MD5 hashing for IMEIs '64220204327947', '12875502464321' and check that
    # the golden_list IMEIs are not blacklisted.

    # First Phase
    gsma_tac_db_importer.import_data()
    operator_data_importer.import_data()
    stolen_list_importer.import_data()
    db_conn.commit()

    cond_list = [{
        'label': 'gsma_not_found',
        'reason': 'TAC not found in GSMA TAC database',
        'blocking': True,
        'dimensions': [{'module': 'gsma_not_found', 'parameters': {'ignore_rbi_delays': True}}]
    }, {
        'label': 'local_stolen',
        'reason': 'IMEI found on local stolen list',
        'blocking': True,
        'dimensions': [{'module': 'stolen_list'}]
    }]

    matching_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                classify_options=['--no-safety-check'],
                                                                db_conn=db_conn)

    exp_res = ['64220204327947', '12875502464321']
    assert all([(x in matching_imeis) for x in exp_res]) is True
    assert len(matching_imeis) == 24

    db_conn.commit()

    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_listgen_cli, [output_dir], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    list_gen_path = find_subdirectory_in_dir('listgen*', output_dir)
    zip_fn = find_file_in_dir('*blacklist.zip', list_gen_path)
    with zipfile.ZipFile(zip_fn, 'r') as zf:
        zf.extractall(path=list_gen_path)

    # NOT EMPTY BLACK LIST contains IMEIs ['64220204327947', '12875502464321']
    fn = find_file_in_dir('*blacklist.csv', list_gen_path)
    with open(fn, 'r') as bl:
        bl_content = bl.readlines()
        blacklisted_imeis = [x.split(',')[0] for x in bl_content]
        assert len(bl_content) == 24
        assert '64220204327947' in blacklisted_imeis
        assert '12875502464321' in blacklisted_imeis

    # Second Phase
    # import unhashed IMEIs
    imei_one = '64220204327947'
    imei_two = '12875502464321'

    with get_importer(GoldenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GoldenListParams(content='GOLDEN_IMEI\n{0}\n{1}\n'.format(imei_one, imei_two))) as imp:
        expect_success(imp, 2, db_conn, logger)

    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                               classify_options=['--no-safety-check'])

    db_conn.commit()
    one_output_dir = os.path.join(output_dir, 'one')
    os.makedirs(one_output_dir, exist_ok=True)
    result = runner.invoke(dirbs_listgen_cli, [one_output_dir], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    list_gen_path = find_subdirectory_in_dir('listgen*', one_output_dir)
    zip_fn = find_file_in_dir('*blacklist.zip', list_gen_path)
    with zipfile.ZipFile(zip_fn, 'r') as zf:
        zf.extractall(path=list_gen_path)

    # NOT EMPTY BLACK LIST DOES NOT contain IMEIs ['64220204327947', '12875502464321']
    fn = find_file_in_dir('*blacklist.csv', list_gen_path)
    with open(fn, 'r') as bl:
        bl_content = bl.readlines()
        blacklisted_imeis = [x.split(',')[0] for x in bl_content]
        assert len(bl_content) == 22
        assert '64220204327947' not in blacklisted_imeis
        assert '12875502464321' not in blacklisted_imeis


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False)],
                         indirect=True)
def test_duplicate_threshold(db_conn, operator_data_importer, tmpdir, logger, mocked_config, monkeypatch):
    """Test Depot ID 96756/2.

    Verify that A regulator/partner should be able to classify whether
    IMEIs are duplicated based on a total subscriber threshold algorithm.
    """
    operator_data_importer.import_data()
    # Verify dirbs-classify does not find any duplicates
    cond_list = [{
        'label': 'duplicate_threshold',
        'reason': 'duplicate_threshold',
        'dimensions': [{
            'module': 'duplicate_threshold',
            'parameters': {
                'threshold': 12,
                'period_days': 120}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161130')
    assert (len(matched_imeis) == 0)

    # Verify dirbs-classify does finds 2 duplicates
    cond_list = [{
        'label': 'duplicate_threshold',
        'reason': 'duplicate_threshold',
        'dimensions': [{
            'module': 'duplicate_threshold',
            'parameters': {
                'threshold': 2,
                'period_days': 60}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161130')
    assert (len(matched_imeis) == 1)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False)],
                         indirect=True)
def test_duplicate_threshold_with_msisdn(db_conn, operator_data_importer, tmpdir, logger, mocked_config, monkeypatch):
    """Verify that duplicate threshold dimension classify IMEIs using MSISDN as well rather than IMSI."""
    operator_data_importer.import_data()
    # Verify that dirbs-classify does not find any duplicates
    cond_list = [{
        'label': 'duplicate_threshold',
        'reason': 'duplicate_threshold',
        'dimensions': [{
            'module': 'duplicate_threshold',
            'parameters': {
                'threshold': 12,
                'period_days': 120,
                'use_msisdn': True}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161130')
    assert (len(matched_imeis) == 0)

    # Verify that dirbs-classify does finds duplicates
    cond_list = [{
        'label': 'duplicate_threshold',
        'reason': 'duplicate_threshold',
        'dimensions': [{
            'module': 'duplicate_threshold',
            'parameters': {
                'threshold': 2,
                'period_days': 60,
                'use_msisdn': True}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161130')
    assert (len(matched_imeis) == 2)


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
def test_duplicate_threshold_period_months(db_conn, operator_data_importer, tmpdir, logger, mocked_config,
                                           monkeypatch):
    """Test Depot ID unknown.

    Verify that A regulator/partner should be able to classify whether
    IMEIs are duplicated based on a total subscriber threshold algorithm with the lookback
    window specified in months rather than days.
    """
    operator_data_importer.import_data()

    # Verify dirbs-classify does finds 2 duplicates
    cond_list = [{
        'label': 'duplicate_threshold',
        'reason': 'duplicate_threshold',
        'dimensions': [{
            'module': 'duplicate_threshold',
            'parameters': {
                'threshold': 12,
                'period_months': 4}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161130')
    assert (len(matched_imeis) == 0)

    # Verify dirbs-classify does finds 2 duplicates
    cond_list = [{
        'label': 'duplicate_threshold',
        'reason': 'duplicate_threshold',
        'dimensions': [{
            'module': 'duplicate_threshold',
            'parameters': {
                'threshold': 2,
                'period_months': 2}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161130')
    assert (len(matched_imeis) == 1)


def test_bitmask_windowing_function(db_conn):
    """Test the bitmask windowing function."""
    with db_conn.cursor() as cur:
        # Verify when analysis start date is greater than month_first_seen; then lower bits
        # accounting for days outside the analysis window are zeroed out.
        date_bitmask = int('0000001101101111011111010111111', 2)
        month_first_seen = datetime.datetime.strptime('2017-01-01', '%Y-%m-%d').date()
        month_last_seen = datetime.datetime.strptime('2017-01-25', '%Y-%m-%d').date()
        analysis_window_start_date = datetime.datetime.strptime('2017-01-05', '%Y-%m-%d').date()
        analysis_window_end_date = datetime.datetime.strptime('2017-02-10', '%Y-%m-%d').date()
        cur.execute("""SELECT * FROM get_bitmask_within_window(%s, %s, %s, %s, %s, %s, %s)""",
                    [date_bitmask, month_first_seen, month_last_seen, analysis_window_start_date,
                     analysis_window_start_date.day, analysis_window_end_date, analysis_window_end_date.day])
        assert cur.fetchone()[0] == int('0000001101101111011111010110000', 2)

        # Verify when analysis end date is lesser than month_last_seen; then higher bits
        # accounting for days outside the analysis window are zeroed out.
        date_bitmask = int('0000001101101111011111010111111', 2)
        month_first_seen = datetime.datetime.strptime('2017-01-01', '%Y-%m-%d').date()
        month_last_seen = datetime.datetime.strptime('2017-01-25', '%Y-%m-%d').date()
        analysis_window_start_date = datetime.datetime.strptime('2016-01-01', '%Y-%m-%d').date()
        analysis_window_end_date = datetime.datetime.strptime('2017-01-10', '%Y-%m-%d').date()
        cur.execute("""SELECT * FROM get_bitmask_within_window(%s, %s, %s, %s, %s, %s, %s)""",
                    [date_bitmask, month_first_seen, month_last_seen, analysis_window_start_date,
                     analysis_window_start_date.day, analysis_window_end_date, analysis_window_end_date.day])
        assert cur.fetchone()[0] == int('0000000000000000000000010111111', 2)

        # Verify when analysis start date is greater than month_first_seen and analysis end date is
        # lesser than month_last_seen then only middle bits accounting for days within the analysis
        # window are retained and everything else is zeroed out.
        date_bitmask = int('0000001101101111011111010111111', 2)
        month_first_seen = datetime.datetime.strptime('2017-01-01', '%Y-%m-%d').date()
        month_last_seen = datetime.datetime.strptime('2017-01-25', '%Y-%m-%d').date()
        analysis_window_start_date = datetime.datetime.strptime('2017-01-05', '%Y-%m-%d').date()
        analysis_window_end_date = datetime.datetime.strptime('2017-01-20', '%Y-%m-%d').date()
        cur.execute("""SELECT * FROM get_bitmask_within_window(%s, %s, %s, %s, %s, %s, %s)""",
                    [date_bitmask, month_first_seen, month_last_seen, analysis_window_start_date,
                     analysis_window_start_date.day, analysis_window_end_date, analysis_window_end_date.day])
        assert cur.fetchone()[0] == int('0000000000001111011111010110000', 2)


def test_bit_counting_function(db_conn):
    """Test the bit counting function."""
    with db_conn.cursor() as cur:
        # Test 0000000000000000000000000000000 contains 0 bits
        num = int('0000000000000000000000000000000', 2)
        cur.execute("""SELECT * FROM bitcount(%s)""", [num])
        assert cur.fetchone()[0] == 0

        # Test 0000000000000000000000000010000 contains 1 bits
        num = int('0000000000000000000000000010000', 2)
        cur.execute("""SELECT * FROM bitcount(%s)""", [num])
        assert cur.fetchone()[0] == 1

        # Test 0000000000000000000000000001111 contains 4 bits
        num = int('0000000000000000000000000001111', 2)
        cur.execute("""SELECT * FROM bitcount(%s)""", [num])
        assert cur.fetchone()[0] == 4

        # Test 0000000000000101001001101100111 contains 10 bits
        num = int('0000000000000101001001101100111', 2)
        cur.execute("""SELECT * FROM bitcount(%s)""", [num])
        assert cur.fetchone()[0] == 10

        # Test 0010100100101011000010101111010 contains 11 bits
        num = int('0010100100101011000010101111010', 2)
        cur.execute("""SELECT * FROM bitcount(%s)""", [num])
        assert cur.fetchone()[0] == 14


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='test_operator1_average_duplicate_threshold_20161101_20161130.csv',
                             operator='1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False)],
                         indirect=True)
def test_averaging_duplicate_threshold_multiple_days(db_conn, operator_data_importer, mocked_config,
                                                     tmpdir, logger, monkeypatch):
    """Test averaging duplicate algorithm over multiple days.

    Verify that A regulator/partner should be able to classify whether
    IMEIs are duplicated based on an average subscriber threshold algorithm.
    """
    operator_data_importer.import_data()

    # Verify one duplicate IMEI found when averaged over multiple days
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 2.0,
                'period_days': 5,
                'min_seen_days': 5}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161121')
    assert matched_imeis == ['21123131308879']

    # Verify no duplicate IMEIs found when threshold value greater than average
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 2.1,
                'period_days': 5,
                'min_seen_days': 5}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161121')
    assert len(matched_imeis) == 0

    # Verify one duplicate IMEI found when threshold value lesser than average
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 1.9,
                'period_days': 5,
                'min_seen_days': 5}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161121')
    assert matched_imeis == ['21123131308879']

    # Verify multiple duplicate IMEIs found when averaged over multiple days and min_seen_days
    # value is applicable to multiple IMEIs and ratio is a float value
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 1.1,
                'period_days': 19,
                'min_seen_days': 2}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161124')
    assert matched_imeis.sort() == ['21123131308879', '13768038709433', '21260934121733'].sort()

    # Verify min_seen_days excludes duplicate IMEIs having less than 5 days of data.
    # Same test scenario as above with min_seen_days value set to 5.
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 1.1,
                'period_days': 19,
                'min_seen_days': 5}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161124')
    assert matched_imeis == ['21123131308879']

    # Verify multiple (2) duplicate IMEIs (21123131308879, 21260934121733) found
    # ratio is a float value. when averaged over multiple days and and min_seen_days value is applicable
    # to multiple IMEIs and IMEIs and ratio is a float value. IMEI 13768038709433 is not found as
    # a duplicate since it was seen only in 2 days and the first time was post --curr-date 20161121.
    # Duplicates after the curr_date value is not used.
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 1.1,
                'period_days': 19,
                'min_seen_days': 2}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161121')
    assert matched_imeis.sort() == ['21123131308879', '21260934121733'].sort()

    # Verify no duplicate IMEI is found with min_seen_days excludes duplicate IMEIs having less than 5 days
    # of data and with min_seen_days value set to 5 and post --curr-date 20161119. Duplicates after the
    # curr_date value is not used.
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 1.1,
                'period_days': 19,
                'min_seen_days': 5}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161119')
    assert len(matched_imeis) == 0

    # Verify one duplicate IMEI found when averaged over multiple days and ratio is a float value
    # We expect IMEI 21123131308879 to have duplication of 1.833 (11 pairs seen over 6 days)
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 1.83,
                'period_days': 30,
                'min_seen_days': 5}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161130')
    assert matched_imeis == ['21123131308879']

    # Verify no duplicate IMEIs found when averaged over multiple days and ratio is a float value
    # and threshold is greater than that matches average for any IMEI
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 1.84,
                'period_days': 30,
                'min_seen_days': 5}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161130')
    assert len(matched_imeis) == 0


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='test_operator1_average_duplicate_threshold_20161101_20161130.csv',
                             operator='1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False)],
                         indirect=True)
def test_averaging_duplicate_threshold_period_months(db_conn, operator_data_importer, mocked_config,
                                                     tmpdir, logger, monkeypatch):
    """Test Depot ID unknown.

    Verify that the duplicate averaging algorithm works using period_months in the config.
    """
    operator_data_importer.import_data()

    # Verify one duplicate IMEI found when averaged over multiple days and ratio is a float value
    # We expect IMEI 21123131308879 to have duplication of 1.833 (11 pairs seen over 6 days)
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 1.83,
                'period_months': 1,
                'min_seen_days': 5}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161130')
    assert matched_imeis == ['21123131308879']

    # Verify no duplicate IMEIs found when averaged over multiple days and ratio is a float value
    # and threshold is greater than that matches average for any IMEI
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 1.84,
                'period_months': 1,
                'min_seen_days': 5}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161130')
    assert len(matched_imeis) == 0


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='test_operator1_average_duplicate_threshold_20161101_20161130.csv',
                             operator='1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False)],
                         indirect=True)
def test_averaging_duplicate_threshold_across_operators(db_conn, metadata_db_conn, operator_data_importer,
                                                        mocked_config, tmpdir, logger, mocked_statsd, monkeypatch):
    """Test averaging duplicate algorithm to IMEI-IMSI pair is counted only once across multiple operators.

    Verify that A regulator/partner should be able to classify whether
    IMEIs are duplicated based on a average subscriber threshold algorithm.
    """
    operator_data_importer.import_data()

    # Import operator 2 data set
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='test_operator2_average_duplicate_threshold_20161101_20161130.csv',
                          operator='2',
                          extract=False,
                          perform_leading_zero_check=False,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                          perform_unclean_checks=False,
                          perform_file_daterange_check=False)) as new_imp:
        expect_success(new_imp, 30, db_conn, logger)

        # Verify one IMEI-IMSI pair but with multiple MSISDNs is not classified as duplicate.
        # IMEI: 21782434077450 is seen with 111041086604001-223321010866041 IMSI-MSISDN pair on 2016-11-24
        # on operator 1's network. Same IMEI is seen with 111041086604001-229572136924578 and
        # 111041086604001-229572136924578 IMSI-MSISDN pairs on 2016-11-24 on operator 2's network.
        # As the IMEI is seen with only one unique IMSI, it should not be classified as duplicate.
        cond_list = [{
            'label': 'duplicate_daily_avg',
            'reason': 'duplicate daily avg',
            'dimensions': [{
                'module': 'duplicate_daily_avg',
                'parameters': {
                    'threshold': 2.0,
                    'period_days': 1,
                    'min_seen_days': 1}}]
        }]
        matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                   classify_options=['--no-safety-check'],
                                                                   db_conn=db_conn, curr_date='20161124')
        assert len(matched_imeis) == 1
        assert '21782434077450' not in matched_imeis
        assert matched_imeis == ['13768038709433']

        # Verify same IMEI-IMSI-MSISDN pair seen across multiple operators is not classified as duplicate.
        # IMEI: 38709433212541 seen with 111041080094910-223321010800949 on 2016-11-13 on operator 1's network.
        # Same IMEI-IMSI-MSISDN pair is seen on operator 2's network on 2016-11-13 on operator 2's network.
        # As the IMEI is seen with only one unique IMSI, it should not be classified as duplicate.
        cond_list = [{
            'label': 'duplicate_daily_avg',
            'reason': 'duplicate daily avg',
            'dimensions': [{
                'module': 'duplicate_daily_avg',
                'parameters': {
                    'threshold': 2.0,
                    'period_days': 1,
                    'min_seen_days': 1}}]
        }]
        matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                   classify_options=['--no-safety-check'],
                                                                   db_conn=db_conn, curr_date='20161113')
        assert len(matched_imeis) == 0
        assert '38709433212541' not in matched_imeis

        # Verify when current date not specified, maximum date seen in in operator data is used.
        # The maximum date in operator's data set is 2016-11-30. For that date, IMEI: 21260934475212
        # is seen with 111041087139444-223321010871394 IMSI-MSISDN pair on operator 1's network and
        # 111045501652848-223850174512645 IMSI-MSISDN pair on operator 2's network. As it is seen with
        # 2 unique IMSI's which is greater than equal to threshold value, it is classified as duplicate.
        cond_list = [{
            'label': 'duplicate_daily_avg',
            'reason': 'duplicate daily avg',
            'dimensions': [{
                'module': 'duplicate_daily_avg',
                'parameters': {
                    'threshold': 2.0,
                    'period_days': 1,
                    'min_seen_days': 1}}]
        }]
        matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                   classify_options=['--no-safety-check'],
                                                                   db_conn=db_conn)
        assert len(matched_imeis) == 1
        assert matched_imeis == ['21260934475212']

        # Verify above scenario does not return any duplicates when threshold is raised.
        cond_list = [{
            'label': 'duplicate_daily_avg',
            'reason': 'duplicate daily avg',
            'dimensions': [{
                'module': 'duplicate_daily_avg',
                'parameters': {
                    'threshold': 2.1,
                    'period_days': 1,
                    'min_seen_days': 1}}]
        }]
        matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                   classify_options=['--no-safety-check'],
                                                                   db_conn=db_conn)
        assert len(matched_imeis) == 0


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='test_duplicate_mk1_dirbs_527_20161115_20161115.csv',
                             operator='1',
                             extract=False,
                             mcc_mnc_pairs=[{'mcc': '123', 'mnc': '45'}],
                             cc=['12'],
                             perform_leading_zero_check=False,
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False)],
                         indirect=True)
def test_duplicate_mk1_dirbs_527(db_conn, operator_data_importer, tmpdir, logger, mocked_config, monkeypatch):
    """Test Depot ID 96756/2.

    Verify that a IMEI seen on multiple days but only with a single IMSI/MSISDN is not marked as duplicate.
    """
    operator_data_importer.import_data()
    cond_list = [{
        'label': 'duplicate_threshold',
        'reason': 'duplicate_threshold',
        'dimensions': [{
            'module': 'duplicate_threshold',
            'parameters': {
                'threshold': 3,
                'period_days': 15}}]
    }]

    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20160116')
    assert ('88888888888889' not in matched_imeis)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             operator='operator1')],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='testData1-gsmatac_operator1_operator4_anonymized.txt',
                                         extract=False)],
                         indirect=True)
def test_gsma_not_found(db_conn, operator_data_importer, gsma_tac_db_importer, tmpdir, logger, mocked_config,
                        monkeypatch):
    """Test Depot ID 96562/2.

    Verify that regulator/partner should be able to create, delete and modify
    conditions - delete stolen_list only;
    A regulator/partner should be able to classify whether IMEIs have properly allocated TAC values in the
    GSMA TAC DB.
    """
    # The operator input data file contain '64220297727231', '64220299727231'.
    # The first 8 digits (TAC) of this imeis is not contained in the gsma data file
    gsma_tac_db_importer.import_data()
    operator_data_importer.import_data()
    db_conn.commit()

    cond_list = [{
        'label': 'gsma_not_found',
        'reason': 'TAC not found in GSMA TAC database',
        'dimensions': [{'module': 'gsma_not_found', 'parameters': {'ignore_rbi_delays': True}}]
    }]

    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                                               classify_options=['--no-safety-check'])

    # The operator input data file contains '64220297727231', '64220299727231', '01376803870943'.
    # The first 8 digits (TAC) of these imeis is not contained in the gsma data file.
    assert len(matched_imeis) == 3

    cond_list = [{
        'label': 'gsma_not_found',
        'reason': 'TAC not found in GSMA TAC database',
        'dimensions': [{'module': 'gsma_not_found', 'parameters': {'ignore_rbi_delays': False}}]
    }]

    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                                               classify_options=['--no-safety-check'])

    # The operator input data file contains '64220297727231', '64220299727231', '01376803870943'.
    # The first 8 digits (TAC) of these imeis is not contained in the gsma data file.
    # The '01' RBI has a default delay configured which puts it past the curr_date and
    # hence IMEIs belonging to that RBI are no longer classified as gsma_not_found
    assert len(matched_imeis) == 2

    cond_list = [{
        'label': 'gsma_not_found',
        'reason': 'TAC not found in GSMA TAC database',
        'dimensions': [{'module': 'gsma_not_found', 'parameters': {'per_rbi_delays': {'01': 5},
                                                                   'ignore_rbi_delays': False}}]
    }]

    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                                               classify_options=['--no-safety-check'],
                                                               curr_date='20161113')

    # The operator input data file contains '64220297727231', '64220299727231', '01376803870943'.
    # The first 8 digits (TAC) of these imeis is not contained in the gsma data file.
    # The '01' RBI has a default delay of 30 configured which puts it past the curr_date but it is overriden to 5
    # config file which makes it no longer past the curr_date, hence all 3 IMEIs should be classified as invalid.
    assert len(matched_imeis) == 3

    cond_list = [{
        'label': 'gsma_not_found',
        'reason': 'TAC not found in GSMA TAC database',
        'dimensions': [{'module': 'gsma_not_found', 'parameters': {'per_rbi_delays': {'64': 30},
                                                                   'ignore_rbi_delays': False}}]
    }]

    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                                               classify_options=['--no-safety-check'])

    # The operator input data file contains '64220297727231', '64220299727231', '01376803870943'.
    # The first 8 digits (TAC) of these imeis is not contained in the gsma data file.
    # The '01' RBI has a default delay configured which puts it past the curr_date and
    # hence IMEIs belonging to that RBI are no longer classified as gsma_not_found
    # The '64' RBI (not in GSMA db) does not have a default delay configured but the config
    #  specifies a delay for it and hence IMEIs belonging to that RBI are no longer classified as gsma_not_found
    assert len(matched_imeis) == 0


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161115,10000000000000,123456789012345,123456789012345\n'
                                     '20161115,025896314741025,123456789012345,123456789012345\n'
                                     '20161115,645319782302145,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list.csv')],
                         indirect=True)
def test_registration_list(db_conn, operator_data_importer, registration_list_importer, mocked_config, monkeypatch):
    """Test Depot ID not known yet.

    Verify a regulator/partner should be able to classify whether
    IMEIs are blacklist because they are not in the registration list.
    """
    operator_data_importer.import_data()
    registration_list_importer.import_data()
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm FROM classification_state')
        res_list = cur.fetchall()
        assert len(res_list) == 0

    cond_list = [{
        'label': 'not_on_registration_list',
        'reason': 'IMEI not found on local registration_list',
        'max_allowed_matching_ratio': 1.0,
        'dimensions': [{'module': 'not_on_registration_list'}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                                               classify_options=['--no-safety-check'])

    # Only first operator data IMEI is in sample_registration_list
    matched_imeis.sort()
    assert matched_imeis == ['02589631474102', '64531978230214']


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20190415,920000000043212,123456789012345,123456789012345\n'
                                     '20190415,625896314741126,123456789012345,123456789012345\n'
                                     '20190415,745309782312055,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(filename='sample_barred_list_v2.csv')],
                         indirect=True)
def test_exists_in_barred_list_dim(db_conn, operator_data_importer, barred_list_importer, mocked_config, monkeypatch):
    """Test Depot ID not known yet.

    Verify that a regulator/partner should be able to classify whether
    IMEIs are blacklisted because they are on barred list.
    """
    operator_data_importer.import_data()
    barred_list_importer.import_data()
    db_conn.commit()

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT imei_norm FROM classification_state')
        res_list = cursor.fetchall()
        assert len(res_list) == 0

    cond_list = [{
        'label': 'exists_in_barred_list',
        'reason': 'IMEI found on local barred_list',
        'max_allowed_matching_ratio': 1.0,
        'dimensions': [{'module': 'exists_in_barred_list'}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                                               classify_options=['--no-safety-check'])
    matched_imeis.sort()
    assert '74530978231205' in matched_imeis


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20190415,920000010043212,123456789012345,123456789012345\n'
                                     '20190415,625896324741126,123456789012345,123456789012345\n'
                                     '20190415,745309792312055,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('barred_tac_list_importer',
                         [BarredTacListParams(
                             content='TAC\n'
                                     '62589632\n'
                                     '74530979'
                         )],
                         indirect=True)
def test_is_barred_tac_dim(db_conn, operator_data_importer, barred_tac_list_importer, mocked_config, monkeypatch):
    """Test Depot ID not known yet.

    Verify that a regulator/partner should be able to classify whether
    IMEIs are blacklisted because their tac is on barred_tac list.
    """
    operator_data_importer.import_data()
    barred_tac_list_importer.import_data()
    db_conn.commit()

    with db_conn.cursor() as cursor:
        cursor.execute('SELECT imei_norm FROM classification_state')
        res_list = cursor.fetchall()
        assert len(res_list) == 0

    cond_list = [{
        'label': 'tac_in_barred_list',
        'reason': 'IMEI TAC is in barred tac list',
        'max_allowed_matching_ratio': 1.0,
        'dimensions': [{'module': 'is_barred_tac'}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                                               classify_options=['--no-safety-check'])
    matched_imeis.sort()
    assert matched_imeis == ['62589632474112', '74530979231205']


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161115,10000000000000,123456789012345,123456789012345\n'
                                     '20161115,012344014741025,123456789012345,123456789012345\n'
                                     '20161115,012344022302145,123456789012345,123456789012345\n'
                                     '20161115,012344035454564,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list.csv')],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='sample_gsma_import_list_anonymized.txt',
                                         extract=False)],
                         indirect=True)
def test_registration_list_with_exempted_device_types(db_conn, operator_data_importer, registration_list_importer,
                                                      gsma_tac_db_importer, mocked_config, monkeypatch):
    """Test Depot ID not known yet.

    Verify a regulator/partner should be able to classify whether IMEIs are on blacklist because they are not
    in the registration list; but IMEIs belonging to exempted device types are excluded.
    """
    operator_data_importer.import_data()
    registration_list_importer.import_data()
    gsma_tac_db_importer.import_data()
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm FROM classification_state')
        res_list = cur.fetchall()
        assert len(res_list) == 0

    monkeypatch.setattr(mocked_config.region_config, 'exempted_device_types', ['Vehicle', 'Dongle'])
    # Verify IMEIs belonging to exempted device types are excluded.
    cond_list = [{
        'label': 'not_on_registration_list',
        'reason': 'IMEI not found on local registration_list',
        'max_allowed_matching_ratio': 1.0,
        'dimensions': [{'module': 'not_on_registration_list'}]
    }]

    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                                               classify_options=['--no-safety-check'])

    # Three IMEIs in operator data are not on registration list, but two belong to exempted device types;
    # hence only one IMEI is matched for the condition.
    assert matched_imeis == ['01234401474102']


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='testData1-sample_stolen_list-anonymized.csv')],
                         indirect=True)
def test_local_stolen(db_conn, stolen_list_importer, mocked_config, monkeypatch):
    """Test Depot ID 96561/1.

    Verify a regulator/partner should be able to classify whether
    IMEIs are on the local stolen list.
    """
    stolen_list_importer.import_data()
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm FROM classification_state')
        res_list = cur.fetchall()
        assert len(res_list) == 0

    cond_list = [{
        'label': 'stolen_list',
        'reason': 'IMEI found on local stolen list',
        'dimensions': [{'module': 'stolen_list'}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                                               classify_options=['--no-safety-check'])
    # The stolen_list input data file contains 21 rows.
    assert len(matched_imeis) == 21
    db_conn.commit()

    cond_list = [{
        'label': 'stolen_list',
        'reason': 'IMEI found on local stolen list',
        'dimensions': [{'module': 'stolen_list'}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                                               classify_options=['--no-safety-check'])
    # Same stolen_list input data file which contains 21 rows.
    assert len(matched_imeis) == 21


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator4-dupInvalidTriplet-'
                                      'anonymized_20161101_20161130.csv',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_null_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='1')],
                         indirect=True)
def test_invalid_imsi_dupl(db_conn, tmpdir, logger, operator_data_importer, mocked_config, monkeypatch):
    """Test Depot ID 96764/3.

    Verify DIRBS core update duplicate dimensions to exclude potentially invalid
    IMSIs from duplicate analysis to avoid false-positives.
    """
    operator_data_importer.import_data()

    # operator data file contains IMEI-IMSI pairs with same IMEI and null or shorter IMSI
    cond_list = [{
        'label': 'duplicate_mk1',
        'reason': 'Duplicate IMEI detected',
        'dimensions': [{'module': 'duplicate_threshold', 'parameters': {'period_days': 60, 'threshold': 2}}]
    }]
    attr_list = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                                           classify_options=['--no-safety-check'])

    assert '38847733370026' not in attr_list
    assert '21123131308879' in attr_list


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_normalizeIMEI_classification_test_anonymized_20161101_20161130.csv',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='1')],
                         indirect=True)
def test_normalized_imei(db_conn, tmpdir, logger, operator_data_importer, mocked_config, monkeypatch):
    """Test Depot ID 96764/3.

    Verify DIRBS core update duplicate dimensions to exclude potentially invalid
    triplets from duplicate analysis to avoid false-positives.
    """
    operator_data_importer.import_data()

    # operator data file contains triplets with different IMEIs but same normalized IMEI.
    cond_list = [{
        'label': 'duplicate_mk1',
        'reason': 'Duplicate IMEI detected',
        'dimensions': [{'module': 'duplicate_threshold', 'parameters': {'period_days': 60, 'threshold': 2}}]
    }]
    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                               classify_options=['--no-safety-check'])
    with db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm FROM classification_state WHERE end_date IS NULL ORDER BY imei_norm')
        attr_list = [res.imei_norm for res in cur.fetchall()]

    assert '38847733243375' not in attr_list
    assert '21123131239940' in attr_list


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator4_v1-anonymized_20161101_20161130.csv',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1')],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='testData1-gsmatac_operator1_operator4_anonymized.txt',
                                         extract=False)],
                         indirect=True)
@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='testData1-sample_stolen_list-anonymized.csv')],
                         indirect=True)
def test_multi_dim_conditions(db_conn, tmpdir, logger, mocked_config,
                              gsma_tac_db_importer, operator_data_importer, stolen_list_importer, monkeypatch):
    """Test Depot ID 96566/6.

    Verify a  regulator/partner should be able to
    configure conditions that contain multi dimensions.
    """
    gsma_tac_db_importer.import_data()
    operator_data_importer.import_data()
    stolen_list_importer.import_data()
    db_conn.commit()

    not_compound_cond_config_dict = [{
        'label': 'gsma_not_found',
        'reason': 'TAC not found in GSMA TAC database',
        'dimensions': [{'module': 'gsma_not_found', 'parameters': {'ignore_rbi_delays': True}}]
    }, {
        'label': 'stolen_list',
        'reason': 'IMEI found on local stolen list',
        'dimensions': [{'module': 'stolen_list'}]
    }]
    invoke_cli_classify_with_conditions_helper(not_compound_cond_config_dict, mocked_config, monkeypatch,
                                               classify_options=['--no-safety-check'])

    with db_conn.cursor() as cur:
        cur.execute('SELECT cond_name, COUNT(imei_norm) FROM '
                    'classification_state WHERE end_date IS NULL GROUP BY cond_name ORDER BY cond_name')
        res_list = cur.fetchall()

    # sample_stolen_list.csv contains 21 rows;
    # gsmatac_operator4_operator1.txt contains 6 TACS which are not in the first
    # 8 digits of the imeis in operator-operator4_v1_20161101_20161130.csv
    assert res_list[0].cond_name == 'gsma_not_found'
    assert res_list[0].count == 6
    assert res_list[1].cond_name == 'stolen_list'
    assert res_list[1].count == 21

    # compound condition
    cond_list = [{
        'label': 'compound_gsma_not_found_stolen_list',
        'grace_period_days': 3,
        'reason': 'TAC not found in GSMA TAC database',
        'dimensions': [{'module': 'gsma_not_found', 'parameters': {'ignore_rbi_delays': True}},
                       {'module': 'stolen_list'}]
    }]

    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                               classify_options=['--no-safety-check'],
                                               curr_date='20161115')
    # matched_imeis for compound (intersection) condition:
    # '64220297727231', '12640904324427', '72723147267631', '72723147267231'
    # all the IMEIs have as date: 2016-11-12.
    assert len(matching_imeis_for_cond_name(db_conn, cond_name='compound_gsma_not_found_stolen_list')) == 4


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(filename='testData1-sample_stolen_list-anonymized.csv')],
                         indirect=True)
def test_grace_period_condition(db_conn, stolen_list_importer, mocked_config, monkeypatch):
    """Test Depot ID 96755/10.

    Verify regulator/partner is able to configure a condition with a grace
    period until blacklisting. Verify the IMEIs are added to the blacklist immediately after the
    grace period expires. Verify Blacklist contain IMEIs that meet a blocking condition with an
    expired grace period.
    """
    stolen_list_importer.import_data()
    db_conn.commit()

    cond_list = [{
        'label': 'local_stolen',
        'grace_period_days': 87,
        'blocking': True,
        'sticky': False,
        'reason': 'IMEI found on local stolen list',
        'dimensions': [{'module': 'stolen_list'}]
    }]

    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, curr_date='20170408',
                                               classify_options=['--no-safety-check'])

    # date_days_ago is '2017-1-11'
    date_days_ago = (datetime.date(2017, 4, 8) - timedelta(days=87)).isoformat()

    with db_conn.cursor() as cursor:
        cursor.execute(sql.SQL("""SELECT imei_norm, block_date, cond_name
                                    FROM classification_state
                                   WHERE end_date IS NULL
                                     AND block_date IS NOT NULL
                                     AND block_date > %s::date
                                ORDER BY imei_norm, cond_name"""), [date_days_ago])

        attr_list = [[res.imei_norm, res.block_date.strftime('%Y%m%d'), res.cond_name] for res in cursor.fetchall()]

        # The following list contains IMEIs, block dates and reason info.
        # All the IMEIs in the list have block date greater than 2017-1-11.
        # This date is 87 days (grace_period) before the classification date (2017, 4, 8).
        b = [['12432807272315', '20170704', 'local_stolen'],
             ['12640904324427', '20170704', 'local_stolen'],
             ['12640904372723', '20170704', 'local_stolen'],
             ['12727231272313', '20170704', 'local_stolen'],
             ['12875502464321', '20170704', 'local_stolen'],
             ['12875502572723', '20170704', 'local_stolen'],
             ['12875507272312', '20170704', 'local_stolen'],
             ['12904502843271', '20170704', 'local_stolen'],
             ['12909602432585', '20170704', 'local_stolen'],
             ['12909602872723', '20170704', 'local_stolen'],
             ['12922902206948', '20170704', 'local_stolen'],
             ['12922902243260', '20170704', 'local_stolen'],
             ['12922902432742', '20170704', 'local_stolen'],
             ['12922902432776', '20170704', 'local_stolen'],
             ['12957272313271', '20170704', 'local_stolen'],
             ['17272317272723', '20170704', 'local_stolen'],
             ['56773605727231', '20170704', 'local_stolen'],
             ['64220204327947', '20170704', 'local_stolen'],
             ['64220297727231', '20170704', 'local_stolen'],
             ['72723147267231', '20170704', 'local_stolen'],
             ['72723147267631', '20170704', 'local_stolen']]

        assert attr_list == b


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
def test_malformed_imeis(db_conn, operator_data_importer, mocked_config, monkeypatch,
                         tmpdir, logger):
    """Test Depot ID 96560/1.

    Verify a regulator/partner should be able to classify whether
    IMEIs contain characters A-F, *, or #.
    """
    operator_data_importer.import_data()
    cond_list = [{
        'label': 'malformed_imei',
        'reason': 'Invalid characters detected in IMEI',
        'dimensions': [{'module': 'malformed_imei'}]
    }]
    matched_imeis_list = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                    curr_date='20170408', db_conn=db_conn,
                                                                    classify_options=['--no-safety-check'])

    assert '38245933AF987001' in matched_imeis_list


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_with_rat_info_20160701_20160731.csv',
                             unclean_threshold=0.5,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             perform_rat_import=True)],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_rat_computation_check.txt',
                                         extract=False)],
                         indirect=True)
def test_inconsistent_rat_dimension(db_conn, tmpdir, logger, mocked_config,
                                    postgres, gsma_tac_db_importer, operator_data_importer):
    """Test Depot ID unknown.

    Verify that devices seen on RATs inconsistent with their band capabilities are correctly classified.
    """
    expect_success(gsma_tac_db_importer, 9, db_conn, logger)
    expect_success(operator_data_importer, 9, db_conn, logger)

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    with db_conn.cursor() as cur:
        cur.execute('SELECT imei_norm FROM classification_state')
        res_list = cur.fetchall()
        assert len(res_list) == 0

    runner = CliRunner()  # noqa
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date', '20160801',
                                                '--conditions', 'inconsistent_rat'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    with db_conn.cursor() as c:
        c.execute('SELECT imei_norm, cond_name FROM classification_state '
                  'WHERE end_date IS NULL ORDER BY imei_norm, cond_name ASC')
        res_list = [(res[0], res[1]) for res in c.fetchall()]
        # IMEI seen on 4G, but TAC only supports 3G
        assert ('41266666370026', 'inconsistent_rat') in res_list
        # IMEI seen on 3G, but TAC only support 2G/4G
        assert ('41288888370026', 'inconsistent_rat') in res_list
        # IMEI seen on 2G, but TAC only support 3G/4G
        assert ('41299999370026', 'inconsistent_rat') in res_list
        # IMEI seen on 2G/3G/4G, but model supports all these RATs across TACs
        assert ('41233333638746', 'inconsistent_rat') not in res_list
        # IMEI associated with a TAC having NULL manufacturer is not checked for inconsistent RAT
        # (IMEI is set up to meet inconsistent RAT criteria)
        assert ('11111111638746', 'inconsistent_rat') not in res_list
        # IMEI associated with a TAC having NULL model name is not checked for inconsistent RAT
        # (IMEI is set up to meet inconsistent RAT criteria)
        assert ('22222222638746', 'inconsistent_rat') not in res_list


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_with_rat_info_20160701_20160731.csv',
                             unclean_threshold=0.5,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             perform_rat_import=True)],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_small_imeicheck_2016.txt',
                                         extract=False)],
                         indirect=True)
def test_safety_check(db_conn, tmpdir, logger, mocked_config, monkeypatch,
                      postgres, gsma_tac_db_importer, operator_data_importer):
    """Test Depot ID unknown.

    Verifies that the safety check CLI option works for classification
    """
    expect_success(operator_data_importer, 9, db_conn, logger)

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-classify using db args from the temp postgres instance
    runner = CliRunner()  # noqa
    result = runner.invoke(dirbs_classify_cli, ['--disable-sanity-checks', '--conditions', 'gsma_not_found'],
                           obj={'APP_CONFIG': mocked_config})

    # Program should not exit succesfully
    assert result.exit_code != 0

    # There should be a message output about the safety check triggering
    assert "Refusing to classify using condition \'gsma_not_found\'" in logger_stream_contents(logger)

    # Check that there are no records classified
    imeis = matching_imeis_for_cond_name(db_conn, cond_name='gsma_not_found')
    assert len(imeis) == 0

    # Truncate logger
    logger_stream_reset(logger)

    # Try again with safety check disable
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--disable-sanity-checks',
                                                '--conditions', 'gsma_not_found'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # Check that there all records are classified
    imeis = matching_imeis_for_cond_name(db_conn, cond_name='gsma_not_found')
    assert len(imeis) == 9

    # Now import GSMA TAC DB
    expect_success(gsma_tac_db_importer, 4, db_conn, logger)

    db_conn.commit()

    # Truncate logger
    logger_stream_reset(logger)

    # Now re-classify (5 out of 9 should fail, which is above config default of 10%)
    result = runner.invoke(dirbs_classify_cli, ['--disable-sanity-checks', '--conditions', 'gsma_not_found'],
                           obj={'APP_CONFIG': mocked_config})
    # Program should exit successfully
    assert result.exit_code != 0

    # There should be a message output about the safety check triggering
    assert "Refusing to classify using condition \'gsma_not_found\'" in logger_stream_contents(logger)

    # Set new config with higher safety check threshild
    cond_dict_list = [{'label': 'gsma_not_found',
                       'reason': 'TAC not found in GSMA TAC database',
                       'grace_period_days': 30,
                       'blocking': True,
                       'max_allowed_matching_ratio': 0.56,
                       'dimensions': [{'module': 'gsma_not_found'}]
                       }]

    monkeypatch.setattr(mocked_config, 'conditions', from_cond_dict_list_to_cond_list(cond_dict_list))

    # Now re-classify (5 out of 9 should again fail, but should now be below safety check value of 56%)
    result = runner.invoke(dirbs_classify_cli, ['--disable-sanity-checks', '--conditions', 'gsma_not_found'],
                           obj={'APP_CONFIG': mocked_config})
    # Program should exit successfully
    assert result.exit_code == 0

    # Check that there are 5 records classified
    imeis = matching_imeis_for_cond_name(db_conn, cond_name='gsma_not_found')
    assert len(imeis) == 5


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(content='IMEI,reporting_date,status\n'
                                                   '12432807272315,20160930,blACklist\n'
                                                   '22723147267231,20160930,\n'
                                                   '32723147267631,20160930,any_other_status\n',
                                           extract=False
                                           )],
                         indirect=True)
def test_blocking_condition_with_status(db_conn, logger, stolen_list_importer, mocked_config, monkeypatch):
    """Test Depot ID not known yet.

    Verify only IMEIs with status NULL or blacklist are considered blocked.
    Verify that status value is converted to lowercase and IMEI 12432807272315 with 'blACklist' status is not filtered
    out.
    12432807272315 has status blACklist;
    22723147267231 has status null;
    32723147267631 has status 'any_other_status';
    expect that 32723147267631 is not classified as blocked because its status is neither block or null.
    """
    expect_success(stolen_list_importer, 3, db_conn, logger)

    cond_list = [{
        'label': 'local_stolen',
        'grace_period_days': 1,
        'blocking': False,
        'sticky': False,
        'reason': 'IMEI found on local stolen list',
        'dimensions': [{'module': 'stolen_list'}]
    }]
    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                               curr_date='20160930')
    with db_conn.cursor() as cursor:
        cursor.execute(sql.SQL("""SELECT imei_norm, block_date
                                    FROM classification_state
                                   WHERE end_date IS NULL
                                     AND block_date IS NULL
                                ORDER BY imei_norm"""))

        res = {x.imei_norm for x in cursor.fetchall()}
        assert len(res) == 2
        assert res == {'12432807272315', '22723147267231'}


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161115,10000000000000,123456789012345,123456789012345\n'
                                     '20161115,025896314741025,123456789012345,123456789012345\n'
                                     '20161115,645319782302145,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(content='approved_imei,make,model,status,'
                                                         'model_number,brand_name,device_type,'
                                                         'radio_interface,device_id\n'
                                                         '10000000000000,   ,   ,whiTelist,,,,,1\n'
                                                         '025896314741025,   ,   ,,,,,,2\n'
                                                         '645319782302145,   ,   ,any_other_status,,,,,3')],
                         indirect=True)
def test_registration_list_with_status(db_conn, operator_data_importer, registration_list_importer, mocked_config,
                                       monkeypatch):
    """Test Depot ID not known yet.

    Verify that classification for not_on_registration_list matches all the IMEIs that are not in
    registration_list with either status NULL or whitelist.
    Verify that status value is converted to lowercase and IMEI 10000000000000 with 'whiTelist' status is
    filtered out.
    10000000000000 has status whiTelist;
    025896314741025 has status NULL;
    645319782302145 has status 'any_other_status';
    expect that only 645319782302145 is matching because is in registration list BUT its status is
    neither register or null.
    """
    operator_data_importer.import_data()
    registration_list_importer.import_data()
    db_conn.commit()
    cond_list = [{
        'label': 'not_on_registration_list',
        'reason': 'IMEI not found on local registration_list',
        'max_allowed_matching_ratio': 1.0,
        'dimensions': [{'module': 'not_on_registration_list'}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn)
    assert len(matched_imeis) == 1
    assert matched_imeis[0] == '64531978230214'


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161115,10000000000000,123456789012345,123456789012345\n'
                                     '20161115,025896314741025,123456789012345,123456789012345\n'
                                     '20161115,645319782302145,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(content='approved_imei,make,model,status,'
                                                         'model_number,brand_name,'
                                                         'device_type,radio_interface,device_id\n'
                                                         '10000000000000,   ,   ,whiTelist,,,,,1\n'
                                                         '025896314741025,   ,   ,,,,,,2\n'
                                                         '645319782302145,   ,   ,any_other_status,,,,,3')],
                         indirect=True)
def test_amnesty_conditions(db_conn, operator_data_importer, registration_list_importer, mocked_config, monkeypatch):
    """Verify that the conditions' amnesty parameters are stored appropriately."""
    operator_data_importer.import_data()
    registration_list_importer.import_data()
    db_conn.commit()

    monkeypatch.setattr(mocked_config.amnesty_config, 'amnesty_enabled', True)
    monkeypatch.setattr(mocked_config.amnesty_config, 'evaluation_period_end_date', datetime.date(2017, 1, 1))
    monkeypatch.setattr(mocked_config.amnesty_config, 'amnesty_period_end_date', datetime.date(2017, 2, 2))

    cond_list_amnesty_enabled = [{
        'label': 'not_on_registration_list',
        'reason': 'IMEI not found on local registration_list',
        'max_allowed_matching_ratio': 1.0,
        'blocking': True,
        'grace_period_days': 10,
        'amnesty_eligible': True,
        'dimensions': [{'module': 'not_on_registration_list'}]
    }]

    cond_list_amnesty_disabled = [{
        'label': 'not_on_registration_list',
        'reason': 'IMEI not found on local registration_list',
        'max_allowed_matching_ratio': 1.0,
        'blocking': True,
        'grace_period_days': 10,
        'amnesty_eligible': False,
        'dimensions': [{'module': 'not_on_registration_list'}]
    }]

    query_imeis_from_cs = sql.SQL("""SELECT imei_norm, block_date, amnesty_granted
                                       FROM classification_state
                                   ORDER BY imei_norm""")

    # Step 1: Verify that the amnesty_eligible and block_date values are set properly
    invoke_cli_classify_with_conditions_helper(cond_list_amnesty_enabled, mocked_config, monkeypatch, db_conn=db_conn,
                                               curr_date='20170101')
    with db_conn.cursor() as cursor:
        cursor.execute(query_imeis_from_cs)
        for x in cursor.fetchall():
            assert x.amnesty_granted
            assert x.block_date is None

    # Step 2: Verify that the amnesty_eligible flag is updated to False and block_date is set
    invoke_cli_classify_with_conditions_helper(cond_list_amnesty_disabled, mocked_config, monkeypatch, db_conn=db_conn,
                                               curr_date='20170101')
    with db_conn.cursor() as cursor:
        cursor.execute(query_imeis_from_cs)
        for x in cursor.fetchall():
            assert not x.amnesty_granted
            assert x.block_date == datetime.date(2017, 1, 11)

    # Step 3: Verify that the block_date is cleared when amnesty_eligible flag is re-enabled
    invoke_cli_classify_with_conditions_helper(cond_list_amnesty_enabled, mocked_config, monkeypatch, db_conn=db_conn,
                                               curr_date='20170101')
    with db_conn.cursor() as cursor:
        cursor.execute(query_imeis_from_cs)
        for x in cursor.fetchall():
            assert x.amnesty_granted
            assert x.block_date is None

    # Step 4: Verify that the block_date is set to amnesty_end_date when in amnesty period
    invoke_cli_classify_with_conditions_helper(cond_list_amnesty_enabled, mocked_config, monkeypatch, db_conn=db_conn,
                                               curr_date='20170102')
    with db_conn.cursor() as cursor:
        cursor.execute(query_imeis_from_cs)
        for x in cursor.fetchall():
            assert x.amnesty_granted
            assert x.block_date == datetime.date(2017, 2, 2)

    # Step 4: Verify that the block_date is updated to new amnesty_end_date when in amnesty period
    monkeypatch.setattr(mocked_config.amnesty_config, 'amnesty_period_end_date', datetime.date(2017, 3, 2))
    invoke_cli_classify_with_conditions_helper(cond_list_amnesty_enabled, mocked_config, monkeypatch, db_conn=db_conn,
                                               curr_date='20170102')
    with db_conn.cursor() as cursor:
        cursor.execute(query_imeis_from_cs)
        for x in cursor.fetchall():
            assert x.amnesty_granted
            assert x.block_date == datetime.date(2017, 3, 2)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='test_operator1_average_duplicate_threshold_20161101_20161130.csv',
                             operator='1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False)],
                         indirect=True)
def test_duplicate_daily_avg_with_msisdn(db_conn, operator_data_importer, mocked_config,
                                         tmpdir, logger, monkeypatch):
    """Verify that the duplicate_daily_avg does classify with MSISDN instead of IMSI."""
    operator_data_importer.import_data()
    # Verify that one duplicate IMEI found when averaged over multiple days
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 2.0,
                'period_days': 5,
                'min_seen_days': 5,
                'use_msisdn': True}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161121')
    assert matched_imeis == ['21123131308879']

    # Verify no duplicate IMEIs found when threshold value greater than average
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 2.1,
                'period_days': 5,
                'min_seen_days': 5,
                'use_msisdn': True}}]
    }]
    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161121')
    assert len(matched_imeis) == 0


def test_sanity_checks_conditions(db_conn, mocked_config, tmpdir, logger, monkeypatch):
    """Verify that the sanity checks are performed on conditions."""
    classify_options = []
    classify_options.extend(['--curr-date', '20161130'])
    classify_options.extend(['--disable-sanity-checks'])
    runner = CliRunner()
    result = runner.invoke(dirbs_classify_cli, classify_options, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # monkey patch conditions
    classify_options = []
    classify_options.extend(['--curr-date', '20161130'])
    cond_list = [{
        'label': 'duplicate_daily_avg',
        'reason': 'duplicate daily avg',
        'dimensions': [{
            'module': 'duplicate_daily_avg',
            'parameters': {
                'threshold': 2.1,
                'period_days': 5,
                'min_seen_days': 5,
                'use_msisdn': True}}]
    }]
    cond_list = from_cond_dict_list_to_cond_list(cond_list)
    monkeypatch.setattr(mocked_config, 'conditions', cond_list)
    result = runner.invoke(dirbs_classify_cli, classify_options, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1


def test_sanity_checks_operators(db_conn, mocked_config, tmpdir, logger, monkeypatch):
    """Verify that the sanity checks are performed on operators config."""
    classify_options = []
    classify_options.extend(['--curr-date', '20161130'])
    classify_options.extend(['--disable-sanity-checks'])
    runner = CliRunner()
    result = runner.invoke(dirbs_classify_cli, classify_options, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # monkey patch operator config
    classify_options = []
    classify_options.extend(['--curr-date', '20161130'])
    operator_conf = [{
        'id': 'xyzoperatortest',
        'name': 'First Operator',
        'mcc_mnc_pairs': [{
            'mcc': '111',
            'mnc': '01'
        }]
    }]

    operator_conf = from_op_dict_list_to_op_list(operator_conf)
    monkeypatch.setattr(mocked_config.region_config, 'operators', operator_conf)
    result = runner.invoke(dirbs_classify_cli, classify_options, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1


def test_sanity_checks_amnesty(db_conn, mocked_config, tmpdir, logger, monkeypatch):
    """Verify that the sanity checks are performed on amnesty configs."""
    classify_options = []
    classify_options.extend(['--curr-date', '20161130'])
    classify_options.extend(['--disable-sanity-checks'])
    runner = CliRunner()
    result = runner.invoke(dirbs_classify_cli, classify_options, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # monkey patch amnesty config
    classify_options = []
    classify_options.extend(['--curr-date', '20161130'])
    amnesty_config = {
        'amnesty_enabled': False,
        'evaluation_period_end_date': 19500202,
        'amnesty_period_end_date': 19500302
    }

    amnesty_config = from_amnesty_dict_to_amnesty_conf(amnesty_config)
    monkeypatch.setattr(mocked_config, 'amnesty_config', amnesty_config)
    result = runner.invoke(dirbs_classify_cli, classify_options, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_duplicate_uid_20190925_20191025.csv',
                             operator='1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             perform_historic_checks=False
                         )],
                         indirect=True)
@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(
                             filename='subscribers1_list_duplicate_uid.csv',
                             extract=False
                         )],
                         indirect=True)
def test_daily_avg_uid(db_conn, operator_data_importer, subscribers_list_importer, mocked_config,
                       tmpdir, logger, monkeypatch, metadata_db_conn, mocked_statsd):
    """Verify that the duplicate_daily_uid dimension correctly flags the IMEI(s) based on the data."""
    # detection case, importing operator data with duplicate entries
    operator_data_importer.import_data()
    # importing 3 uid-imsi pairs with different uid(s) and different imsi(s)
    subscribers_list_importer.import_data()
    # configure condition
    cond_list = [{
        'label': 'daily_avg_uid',
        'reason': 'daily avg uid',
        'dimensions': [{
            'module': 'daily_avg_uid',
            'parameters': {
                'threshold': 2.5,
                'period_days': 30,
                'min_seen_days': 5}}]
    }]

    matched_imeis = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20191025')
    assert matched_imeis == ['35236015100001', '86422502001110', '86422502012303', '86544602033101']


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_not_on_association_list_20190925_20191025.csv',
                             operator='1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             perform_historic_checks=False
                         )],
                         indirect=True)
@pytest.mark.parametrize('device_association_list_importer',
                         [DeviceAssociationListParams(
                             filename='association_list_not_on_association_list.csv',
                             extract=False
                         )],
                         indirect=True)
def test_not_on_association_list(db_conn, operator_data_importer, device_association_list_importer, mocked_config,
                                 tmpdir, logger, monkeypatch, metadata_db_conn, mocked_statsd):
    """Verify that the not_on_association_list dimension works correctly."""
    operator_data_importer.import_data()
    device_association_list_importer.import_data()

    # configure condition
    condition = [{
        'label': 'not_on_association_list',
        'reason': 'device not associated to uid',
        'dimensions': [{
            'module': 'not_on_association_list'
        }],
        'max_allowed_matching_ratio': 1.0,
        'blocking': True,
        'grace_period_days': 10
    }]

    # invoke classification and get back results
    matched_imeis = invoke_cli_classify_with_conditions_helper(condition, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20191025')
    assert matched_imeis == ['35236015100006', '35236015100007', '35236015100008', '35236015100009',
                             '35236015100010', '35236015100011', '35236015100012', '35236015100013']


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161115,10000000000000,123456789012345,123456789012345\n'
                                     '20161115,025896314741025,123456789012345,123456789012345\n'
                                     '20161115,645319782302149,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('monitoring_list_importer',
                         [MonitoringListParams(content='imei\n'
                                                       '645319782302149')],
                         indirect=True)
def test_exists_in_monitoring_list(db_conn, operator_data_importer, monitoring_list_importer, mocked_config,
                                   tmpdir, logger, monkeypatch, metadata_db_conn, mocked_statsd):
    """Verify that exists_in_monitoring_list classification dimension works correctly."""
    operator_data_importer.import_data()
    monitoring_list_importer.import_data()

    # configure condition
    condition = [{
        'label': 'on_monitoring',
        'reason': 'on monitoring list',
        'dimensions': [{
            'module': 'exists_in_monitoring_list'
        }],
        'max_allowed_matching_ratio': 1.0,
        'blocking': False,
        'grace_period_days': 10
    }]

    matched_imeis = invoke_cli_classify_with_conditions_helper(condition, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161110')
    assert matched_imeis == ['64531978230214']


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161115,10000000000000,123456789012345,123456789012345\n'
                                     '20161115,025896314741025,123456789012345,123456789012345\n'
                                     '20161115,645319782302149,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
@pytest.mark.parametrize('monitoring_list_importer',
                         [MonitoringListParams(content='imei\n'
                                                       '645319782302149\n'
                                                       '025896314741025')],
                         indirect=True)
def test_exists_in_monitoring_list_dim_with_param(per_test_postgres, db_conn, operator_data_importer,
                                                  monitoring_list_importer, mocked_config, tmpdir, logger, monkeypatch,
                                                  metadata_db_conn, mocked_statsd):
    """Verifies monitored_days param of exists_in_monitoring_list dimension."""
    operator_data_importer.import_data()
    monitoring_list_importer.import_data()

    # configure without monitored_days param, imeis should classify
    condition = [{
        'label': 'on_monitoring',
        'reason': 'on monitoring list',
        'dimensions': [{
            'module': 'exists_in_monitoring_list'
        }],
        'max_allowed_matching_ratio': 1.0,
        'blocking': False,
        'grace_period_days': 10
    }]

    matched_imeis = invoke_cli_classify_with_conditions_helper(condition, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161110')
    assert matched_imeis == ['02589631474102', '64531978230214']

    # configure with monitored_days, should not classify
    condition = [{
        'label': 'on_monitoring',
        'reason': 'on monitoring list',
        'dimensions': [{
            'module': 'exists_in_monitoring_list',
            'parameters': {
                'monitored_days': 20
            }
        }],
        'max_allowed_matching_ratio': 1.0,
        'blocking': False,
        'grace_period_days': 10
    }]

    matched_imeis = invoke_cli_classify_with_conditions_helper(condition, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20161110')
    assert matched_imeis == []


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='transient_imei_operator1_20201201_20201231.csv',
                             operator='1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             perform_historic_checks=False
                         )],
                         indirect=True)
def test_transient_imei_dimension(per_test_postgres, db_conn, operator_data_importer, mocked_config, logger,
                                  tmpdir, monkeypatch, metadata_db_conn, mocked_statsd):
    """Verifies transient_imei_dimension."""
    # param validation checks first
    condition = [{
        'label': 'transient_imei',
        'reason': 'IMEI detect as transient',
        'dimensions': [{
            'module': 'transient_imei',
            'parameters': {
                'num_msisdns': 3
            }
        }],
        'max_allowed_matching_ratio': 1.0,
        'blocking': True,
        'grace_period_days': 10
    }]

    # should raise Config parse exception on null period param
    with pytest.raises(ConfigParseException):
        invoke_cli_classify_with_conditions_helper(condition, mocked_config, monkeypatch,
                                                   classify_options=['--no-safety-check'],
                                                   db_conn=db_conn, curr_date='20201201')

    # should raise Config parse exception on non int period param
    condition = [{
        'label': 'transient_imei',
        'reason': 'IMEI detect as transient',
        'dimensions': [{
            'module': 'transient_imei',
            'parameters': {
                'num_msisdns': 3,
                'period': 'A'
            }
        }],
        'max_allowed_matching_ratio': 1.0,
        'blocking': True,
        'grace_period_days': 10
    }]

    with pytest.raises(ConfigParseException):
        invoke_cli_classify_with_conditions_helper(condition, mocked_config, monkeypatch,
                                                   classify_options=['--no-safety-check'],
                                                   db_conn=db_conn, curr_date='20201201')

        # should raise Config parse exception on null num_msisdns param
        condition = [{
            'label': 'transient_imei',
            'reason': 'IMEI detect as transient',
            'dimensions': [{
                'module': 'transient_imei',
                'parameters': {
                    'period': 20
                }
            }],
            'max_allowed_matching_ratio': 1.0,
            'blocking': True,
            'grace_period_days': 10
        }]

    with pytest.raises(ConfigParseException):
        invoke_cli_classify_with_conditions_helper(condition, mocked_config, monkeypatch,
                                                   classify_options=['--no-safety-check'],
                                                   db_conn=db_conn, curr_date='20201201')

    # should raise Config parse exception on non int num_msisdns param
    condition = [{
        'label': 'transient_imei',
        'reason': 'IMEI detect as transient',
        'dimensions': [{
            'module': 'transient_imei',
            'parameters': {
                'num_msisdns': 'a',
                'period': 20
            }
        }],
        'max_allowed_matching_ratio': 1.0,
        'blocking': True,
        'grace_period_days': 10
    }]

    with pytest.raises(ConfigParseException):
        invoke_cli_classify_with_conditions_helper(condition, mocked_config, monkeypatch,
                                                   classify_options=['--no-safety-check'],
                                                   db_conn=db_conn, curr_date='20201201')

    # now a happy scenario, 1 imei detected as transient
    operator_data_importer.import_data()
    condition = [{
        'label': 'transient_imei',
        'reason': 'IMEI detect as transient',
        'dimensions': [{
            'module': 'transient_imei',
            'parameters': {
                'num_msisdns': 3,
                'period': 30
            }
        }],
        'max_allowed_matching_ratio': 1.0,
        'blocking': True,
        'grace_period_days': 10
    }]

    matched_imeis = invoke_cli_classify_with_conditions_helper(condition, mocked_config, monkeypatch,
                                                               classify_options=['--no-safety-check'],
                                                               db_conn=db_conn, curr_date='20201231')
    assert matched_imeis == ['34444444444444']
