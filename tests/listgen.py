"""
List generation unit tests.

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
import csv
import datetime
import fnmatch
import glob
import zipfile
import re

import pytest
from click.testing import CliRunner
import luhn

from dirbs.cli.listgen import cli as dirbs_listgen_cli
from dirbs.cli.classify import cli as dirbs_classify_cli
from dirbs.importer.operator_data_importer import OperatorDataImporter
from dirbs.config import ConditionConfig
from _helpers import job_metadata_importer, expect_success
from _importer_params import OperatorDataParams, PairListParams, GoldenListParams,\
    StolenListParams, RegistrationListParams, BarredListParams, BarredTacListParams
from _helpers import get_importer, from_cond_dict_list_to_cond_list, find_file_in_dir, find_subdirectory_in_dir, \
    import_data, invoke_cli_classify_with_conditions_helper, from_op_dict_list_to_op_list, \
    from_amnesty_dict_to_amnesty_conf
from _fixtures import *    # noqa: F403, F401
from dirbs.metadata import query_for_command_runs


def _verify_per_operator_lists_generated(dir_path, type_list):
    """Helper function to check that notification or exception lists are generated for operator_ids 1 to 4."""
    for operator_id in range(1, 5):
        pattern = '*_{0}_operator{1}.csv'.format(type_list, operator_id)
        assert find_file_in_dir(pattern, dir_path)


def _cli_listgen_helper(db_conn, tmpdir, sub_temp_dir, mocked_config, date=None, base_run_id=None,  # noqa: C901
                        no_full_list=None, no_clean_up=None, unzip_files=True, combine_deltas=True,
                        disable_sanity_checks=True, conditions=None):
    """Helper function for CLI list-gen."""
    options_list = []
    if date:
        options_list.extend(['--curr-date', date])
    if base_run_id:
        options_list.extend(['--base', base_run_id])
    if no_full_list:
        options_list.extend(['--no-full-lists'])
    if no_clean_up:
        options_list.extend(['--no-cleanup'])
    if conditions:
        options_list.extend(['--conditions', conditions])
    options_list.extend(['--disable-sanity-checks'])
    output_dir = str(tmpdir.mkdir(sub_temp_dir))
    options_list.append(output_dir)
    runner = CliRunner()
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)
    assert result.exit_code == 0
    job_record_list = query_for_command_runs(db_conn, 'dirbs-listgen')
    db_conn.commit()
    assert job_record_list
    run_id = [x.run_id for x in job_record_list][0]

    # If requested, auto unzip all the files as well as many test rely on the .csv files being there
    if unzip_files:
        listgen_path = find_subdirectory_in_dir('listgen*', output_dir)
        for zip_path in glob.glob(os.path.join(listgen_path, '*.zip')):
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(path=listgen_path)

        # If requested, combine all delta files into a single file containing an extra change_type column to match
        # the previous spec that the tests were based off
        if combine_deltas:
            for delta_csv_path in glob.glob(os.path.join(listgen_path, '*delta*.csv')):
                fn = os.path.basename(delta_csv_path)
                file_type = re.sub(r'^\d+_\d+_([a-z]+)_.*$', r'\1', fn)
                date_str = re.sub(r'^(.*)_{0}.*$'.format(file_type), r'\1', fn)
                run_id_range = re.sub(r'^.*_delta_([-0-9]+_[0-9]+)_.*$', r'\1', fn)
                change_type = re.sub(r'^.*{0}_(.*)\.csv$'.format(run_id_range), r'\1', fn)

                if file_type == 'blacklist':
                    combined_fn = os.path.join(listgen_path,
                                               '{0}_blacklist_delta_{1}.csv'.format(date_str, run_id_range))
                else:
                    operator_id = re.sub(r'^.*_{0}_(.*)_delta.*csv$'.format(file_type), r'\1', fn)
                    combined_fn = os.path.join(listgen_path,
                                               '{0}_{1}_{2}_delta_{3}.csv'.format(date_str,
                                                                                  file_type,
                                                                                  operator_id,
                                                                                  run_id_range))

                write_header = True if not os.path.exists(combined_fn) else False
                with open(delta_csv_path, 'r') as input_file, open(combined_fn, 'a') as output_file:
                    input_lines = input_file.read().splitlines()
                    if write_header:
                        output_file.write(input_lines[0] + ',change_type\n')
                    for input_line in input_lines[1:]:
                        output_file.write(input_line + ',{0}\n'.format(change_type))

                os.remove(delta_csv_path)

    return run_id, output_dir


def _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, sub_temp_dir, delta_fn='exceptions_operator1_delta',
                              date=None, base_run_id=None, no_full_list=None, no_clean_up=None):
    """Helper function to run list-gen specifying a base."""
    # call CLI list-gen
    run_id, output_dir_gen = _cli_listgen_helper(db_conn, tmpdir, sub_temp_dir, mocked_config, date=date,
                                                 base_run_id=base_run_id, no_full_list=no_full_list,
                                                 no_clean_up=no_clean_up)
    # read csv
    rows = _read_rows_from_file(delta_fn, tmpdir, output_dir=output_dir_gen)
    return rows, run_id


def _read_rows_from_file(listgen_file_name, tmpdir, output_dir=None, dir_name=None):
    """Helper function to get rows in a file.

    Given part of filename, get the rows from file. This code is usefull for those test who run list-gen once and
    need to check multiple files.
    """
    if not output_dir:
        output_dir = os.path.join(str(tmpdir), dir_name)
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    # we use assert len(matching_file_names) == 1 to assert that we always have one matching file
    # in this function if there is no matching file we return None
    file_names = [x for x in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, x))]
    matching_file_names = fnmatch.filter(file_names, '*{0}*'.format(listgen_file_name))
    if matching_file_names:
        assert len(matching_file_names) == 1
        with open(os.path.join(dir_path, matching_file_names[0]), 'r') as file:
            rows = file.readlines()
        return rows
    return None


def import_operator_data(db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                         mocked_statsd, imported_rows, content, operator_id):
    """Helper function to populate operator data tables."""
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          content=content,
                          extract=False,
                          perform_unclean_checks=False,
                          perform_leading_zero_check=False,
                          perform_region_checks=False,
                          perform_home_network_check=False,
                          perform_historic_checks=False,
                          operator=operator_id
                      )) as new_imp:
        expect_success(new_imp, imported_rows, db_conn, logger)


def _notification_list_classification_state_common_code(db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                                                        mocked_statsd):
    """Helper function to remove boilerplate in notification delta tests.

    i.e. notifications_list_delta_changed, notifications_list_delta_blacklisted, notifications_list_delta_resolved.
    """
    # Populate monthly_network_triplets tables including some triplets with the IMEI in classification_state table
    # (but not all IMEIs)
    # Do it for multiple operators (can be same triplets)
    # Some of the triplets associated with the IMEI should have an IMSI starting with the MCC-MNC of
    # one of the operators (12345678901230, 12345678901231)
    # Other triplets associated with the IMEI should have an IMSI starting with none of the configured
    # networks (12345678901228, 12345678901229)
    # Have some other triplets with some other IMEI not meeting any condition(12345678901227)
    # classification data:
    # imei_norm,cond_name,start_date,end_date,block_date
    # 12345678901227,duplicate_mk1,'2016-01-01',,'2016-04-01'
    # 12345678901230,duplicate_mk1,'2016-01-01',,'2016-04-01'
    # 12345678901233,duplicate_mk1,'2016-01-01',,'2016-04-01'
    for i in range(1, 3):
        import_operator_data(db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                             mocked_statsd, 5 * i, content='date,imei,imsi,msisdn\n'
                                                           '20160222,12345678901227,11106678901234,1\n'
                                                           '20160222,12345678901228,11106678901234,1\n'
                                                           '20160222,12345678901229,11106678901234,1\n'
                                                           '20160222,12345678901230,11101678901234,1\n'
                                                           '20160221,12345678901231,11101678901234,1',
                             operator_id='operator{0}'.format(i))
    # add MCC-MNC from operator 4 '20161122,12345678901233,11104678901234,1\n'
    # and check it is on the list even if we don't import it.
    import_operator_data(db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                         mocked_statsd, 16, content='date,imei,imsi,msisdn\n'
                                                    '20160222,12345678901227,11106678901234,1\n'
                                                    '20160222,12345678901228,11106678901234,1\n'
                                                    '20160222,12345678901229,11106678901234,1\n'
                                                    '20160222,12345678901230,11101678901234,1\n'
                                                    '20160222,12345678901233,11104678901234,1\n'
                                                    '20160221,12345678901231,11101678901234,1',
                         operator_id='operator3')

    # Run dirbs-listgen once, with curr_date set to ensure that at the one IMEI meeting a blocking condition is still
    # in the grace period.
    # Triplets whose IMSI start with the MCC-MNC of the operator should be in that operator's delta notifications
    # with change_type new. They should not be in the other operator's delta list.
    # Triplets whose IMSI does not start with the MCC-MNC of any operator should be in
    # both operators' delta list with change_type new.
    # Triplets with a different IMEI complete should not be on any list
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run0',
                                               delta_fn='notifications_operator1_delta', date='20160301')

    assert len(rows_op_one) == 3
    assert rows_op_one[0] == 'imei,imsi,msisdn,block_date,reasons,change_type\n'
    set(rows_op_one[1:]) == {'12345678901227,11106678901234,1,20160401,Duplicate IMEI detected,new\n',
                             '12345678901230,11101678901234,1,20160401,Duplicate IMEI detected,new\n'}

    rows_op_two = _read_rows_from_file('notifications_operator2.csv', tmpdir, dir_name='run0')
    rows_op_three = _read_rows_from_file('notifications_operator3.csv', tmpdir, dir_name='run0')
    assert len(rows_op_two) == 2
    assert len(rows_op_three) == 2
    assert rows_op_two[0] == rows_op_three[0] == 'imei,imsi,msisdn,block_date,reasons\n'
    expected_rows = {'12345678901227,11106678901234,1,20160401,Duplicate IMEI detected\n'}
    assert set(rows_op_two[1:]) == set(rows_op_three[1:]) == expected_rows
    # should have also this entry 20161122,12345678901233,11104678901234
    rows_op_four = _read_rows_from_file('notifications_operator4.csv', tmpdir, dir_name='run0')
    assert len(rows_op_four) == 2
    assert rows_op_four[0] == 'imei,imsi,msisdn,block_date,reasons\n'
    assert set(rows_op_four[1:]) == {'12345678901233,11104678901234,1,20160401,Duplicate IMEI detected\n'}


def _sql_func_gen_delta_list_common_code(db_conn, name_proc):
    """Helper function to remove boilerplate in sql function delta list tests."""
    with db_conn, db_conn.cursor() as cursor:
        cursor.callproc(name_proc, ['operator_1', 1])
        cursor.callproc(name_proc, ['operator_1', -1, 2])

        with pytest.raises(Exception) as ex:
            cursor.callproc(name_proc, ['operator_1', -1, 'a'])
        assert 'invalid input syntax for type bigint: "a"' in str(ex.value)

    with db_conn, db_conn.cursor() as cursor:
        with pytest.raises(Exception) as ex:
            cursor.callproc(name_proc, ['operator_1', 'a'])
        assert 'invalid input syntax for type bigint: "a"' in str(ex.value)

    with db_conn, db_conn.cursor() as cursor:
        with pytest.raises(Exception) as ex:
            cursor.callproc(name_proc, ['operator_1', 8, 2])
        assert 'Parameter base_run_id 8 greater than run_id 2' in str(ex.value)


def _sql_func_gen_list_common_code(db_conn, name_proc):
    """Helper function to remove boilerplate in sql function gen list tests."""
    with db_conn, db_conn.cursor() as cursor:
        cursor.callproc(name_proc, ['operator_1'])
        cursor.callproc(name_proc, ['operator_1', -1])
        with pytest.raises(Exception) as ex:
            cursor.callproc(name_proc, ['operator_1', 'a'])
        assert 'invalid input syntax for type bigint: "a"' in str(ex.value)


def test_cli_arg_no_full_lists(tmpdir, db_conn, mocked_config):
    """Test that the --no-full-lists CLI option works (doesn't produce CSV full lists)."""
    row_bl_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run0', date=None, base_run_id=None,
                                                 no_full_list=True, delta_fn='blacklist.csv')
    rows_exc_op_two = _read_rows_from_file('exceptions_operator2.csv', tmpdir, dir_name='run0')
    rows_not_op_two = _read_rows_from_file('notifications_operator2.csv', tmpdir, dir_name='run0')
    rows_not_op_three = _read_rows_from_file('notifications_operator3.csv', tmpdir, dir_name='run0')
    assert all([x is None for x in [row_bl_op_one, rows_exc_op_two, rows_not_op_two, rows_not_op_three]])
    assert _read_rows_from_file('notifications_operator1_delta', tmpdir, dir_name='run0')

    row_bl_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1', date=None, base_run_id=None,
                                                 delta_fn='blacklist.csv')
    rows_exc_op_two = _read_rows_from_file('exceptions_operator2.csv', tmpdir, dir_name='run1')
    rows_exc_op_two_delta = _read_rows_from_file('exceptions_operator2_delta', tmpdir, dir_name='run1')
    rows_not_op_two = _read_rows_from_file('notifications_operator2.csv', tmpdir, dir_name='run1')
    assert row_bl_op_one == ['imei,block_date,reasons\n']
    assert rows_exc_op_two == ['imei,imsi,msisdn\n']
    assert rows_not_op_two == ['imei,imsi,msisdn,block_date,reasons\n']
    assert rows_exc_op_two_delta == ['imei,imsi,msisdn,change_type\n']


def test_cli_invalid_arg_base_test(tmpdir, db_conn, mocked_config):
    """Test invalid input handling for base CLI arg."""
    with pytest.raises(Exception) as ex:
        _cli_listgen_helper(db_conn, tmpdir, 'run_4', mocked_config, base_run_id=1)
    assert 'Specified base run id 1 not found in list of successful dirbs-listgen runs' in str(ex.value)

    run_id, _ = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config)
    _cli_listgen_helper(db_conn, tmpdir, 'run_2', mocked_config, base_run_id=run_id)

    with pytest.raises(Exception) as ex:
        _cli_listgen_helper(db_conn, tmpdir, 'run_3', mocked_config, base_run_id='a')


def test_sql_func_overall_delta_reason(db_conn):
    """Test the SQL function, both with the run_id == -1 and run_id set to a real run_id.

    This is already tested in aggregate_notifications_list_changes test, so just need to check that the function
    signature hasn't changed.
    """
    def overall_delta_reason_helper(reasons_list_input):
        """Helper function."""
        with db_conn, db_conn.cursor() as cursor:
            cursor.execute("""SELECT overall_delta_reason(reason)
                                FROM (SELECT UNNEST(%s) AS reason) foo;""", [reasons_list_input])
            res = cursor.fetchone().overall_delta_reason
            return res

    # If net_adds is non_zero, return the most recent add or remove reason
    reasons_list = ['changed', 'new', 'removed', 'new']
    assert overall_delta_reason_helper(reasons_list) == 'new'
    reasons_list = ['changed', 'new']
    assert overall_delta_reason_helper(reasons_list) == 'new'
    reasons_list = ['removed', 'new', 'removed', 'changed']
    assert overall_delta_reason_helper(reasons_list) == 'removed'

    # Else if there was no change reason seen, return NULL
    reasons_list = ['new', 'removed']
    assert overall_delta_reason_helper(reasons_list) is None
    reasons_list = ['removed', 'new', 'removed', 'new']
    assert overall_delta_reason_helper(reasons_list) is None

    # Else if there was a change, and the last add or remove reason was a add, return 'changed'
    reasons_list = ['changed', 'new', 'removed']
    assert overall_delta_reason_helper(reasons_list) == 'changed'
    reasons_list = ['new', 'removed', 'changed']
    assert overall_delta_reason_helper(reasons_list) == 'changed'

    # Test some invalid combinations
    reasons_list = ['new', 'new']
    with pytest.raises(Exception) as ex:
        overall_delta_reason_helper(reasons_list)
    assert 'Multiple add reasons in a row - should not happen!' in str(ex.value)

    # Test some invalid combinations
    reasons_list = ['unblocked', 'unblocked']
    with pytest.raises(Exception) as ex:
        overall_delta_reason_helper(reasons_list)
    assert 'Multiple remove reasons in a row - should not happen!' in str(ex.value)

    reasons_list = ['foo', 'bar']
    with pytest.raises(Exception) as ex:
        overall_delta_reason_helper(reasons_list)
    assert 'Unknown reason "foo" - not add, remove or change type!' in str(ex.value)


def test_sql_func_gen_blacklist(db_conn):
    """Test the SQL function, both with the run_id == -1 and run_id set to a real run_id.

    This is already tested in aggregate_notifications_list_changes test, so just need to check that the function
    signature hasn't changed.
    """
    with db_conn, db_conn.cursor() as cursor:
        cursor.callproc('gen_blacklist', [1])
        cursor.callproc('gen_blacklist', [])

        with pytest.raises(Exception) as ex:
            cursor.callproc('gen_blacklist', ['a'])
        assert 'invalid input syntax for type bigint: "a"' in str(ex.value)


def test_sql_func_gen_notifications_list(db_conn):
    """Test the SQL function, both with the run_id == -1 and run_id set to a real run_id.

    This is already tested in aggregate_notifications_list_changes test, so just need to check that the function
    signature hasn't changed.
    """
    _sql_func_gen_list_common_code(db_conn, 'gen_notifications_list')


def test_sql_func_gen_exceptions_list(db_conn):
    """Test the SQL function, both with the run_id == -1 and run_id set to a real run_id.

    This is already tested in aggregate_notifications_list_changes test, so just need to check that the function
    signature hasn't changed.
    """
    _sql_func_gen_list_common_code(db_conn, 'gen_exceptions_list')


def test_sql_func_gen_delta_blacklist(db_conn):
    """Test the SQL function, both with the run_id == -1 and run_id set to a real run_id.

    This is already tested in aggregate_notifications_list_changes test, so just need to check that the function
    signature hasn't changed.
    """
    with db_conn, db_conn.cursor() as cursor:
        cursor.callproc('gen_delta_blacklist', [1])
        cursor.callproc('gen_delta_blacklist', [-1, 2])

        with pytest.raises(Exception) as ex:
            cursor.callproc('gen_delta_blacklist', [-1, 'a'])
        assert 'invalid input syntax for type bigint: "a"' in str(ex.value)

    with db_conn, db_conn.cursor() as cursor:
        with pytest.raises(Exception) as ex:
            cursor.callproc('gen_delta_blacklist', ['a'])
        assert 'invalid input syntax for type bigint: "a"' in str(ex.value)


def test_sql_func_gen_delta_notifications_list(db_conn):
    """Test the SQL function, both with the run_id == -1 and run_id set to a real run_id.

    This is already tested in aggregate_notifications_list_changes test, so just need to check that the function
    signature hasn't changed.
    """
    _sql_func_gen_delta_list_common_code(db_conn, 'gen_delta_notifications_list')


def test_sql_func_gen_delta_exceptions_list(db_conn):
    """Test the SQL function, both with the run_id == -1 and run_id set to a real run_id.

    This is already tested in aggregate_notifications_list_changes test, so just need to check that the function
    signature hasn't changed.
    """
    _sql_func_gen_delta_list_common_code(db_conn, 'gen_delta_exceptions_list')


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v7.csv'],
                         indirect=True)
def test_blacklist_delta_blocked(postgres, db_conn, tmpdir, mocked_config, logger, classification_data):
    """Test blacklist_delta_blocked.

    Test that the delta between two listgen rows is OK when a new IMEI is added into the
    classification_state table for a blocking condition
    """
    # Populate the classification_state table with some initial data
    # imei_norm, cond_name, start_date, end_date, block_date
    # 12345678901227, duplicate_mk1, '2016-01-01',, '2016-06-01'
    # 12345678901230, duplicate_mk1, '2016-01-01',, '2016-04-01'
    # 12345678901233, duplicate_mk1, '2016-01-01',, '2016-12-01'
    # Run dirbs-listgen once, with curr_date set to ensure that at least one IMEI is blacklisted
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run0', date='20160501',
                                               delta_fn='blacklist_delta')
    assert rows_op_one == ['imei,block_date,reasons,change_type\n',
                           '12345678901230,20160401,Duplicate IMEI detected,blocked\n']
    # Add a different IMEI into classification_state
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""INSERT INTO classification_state (run_id, imei_norm, cond_name, start_date, end_date,
                                                            block_date, virt_imei_shard)
                               VALUES('1','12345678901231','gsma_not_found','2016-01-01',NULL,'2016-02-01',
                                      calc_virt_imei_shard('12345678901231'))""")
    # Run dirbs-listgen again, with curr_date set to ensure that the new IMEI is blacklisted
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1', date='20160501',
                                               delta_fn='blacklist_delta')
    # Assert that delta blacklist contains one row with the IMEI and change_type == 'blocked'
    assert rows_op_one == ['imei,block_date,reasons,change_type\n',
                           '12345678901231,20160201,TAC not found in GSMA TAC database,blocked\n']


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v7.csv'],
                         indirect=True)
def test_blacklist_delta_unblocked(postgres, db_conn, tmpdir, mocked_config, logger, classification_data):
    """Test blacklist_delta_unblocked.

    Test that the delta between two listgen rows is OK when a new IMEI is added into the
    classification_state table for a blocking condition
    """
    # Populate the classification_state table with some initial data
    # imei_norm, cond_name, start_date, end_date, block_date
    # 12345678901227, duplicate_mk1, '2016-01-01',, '2016-06-01'
    # 12345678901230, duplicate_mk1, '2016-01-01',, '2016-04-01'
    # 12345678901233, duplicate_mk1, '2016-01-01',, '2016-12-01'
    # Run dirbs-listgen once, with curr_date set to ensure that at least two IMEIs blacklisted
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run0', date='20160701',
                                               delta_fn='blacklist_delta')
    assert rows_op_one == ['imei,block_date,reasons,change_type\n',
                           '12345678901227,20160601,Duplicate IMEI detected,blocked\n',
                           '12345678901230,20160401,Duplicate IMEI detected,blocked\n']
    # Delete one IMEI from classification_state
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""DELETE FROM classification_state
                                WHERE imei_norm = '12345678901227'""")
        # Run dirbs-listgen again, with curr_date set to ensure that the removed IMEI is unblocked
        # Assert that delta blacklist contains one row with the IMEI and change_type == 'unblocked'
        rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1', date='20160501',
                                                   delta_fn='blacklist_delta')
        assert rows_op_one == ['imei,block_date,reasons,change_type\n',
                               '12345678901227,20160601,Duplicate IMEI detected,unblocked\n']


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v6.csv'],
                         indirect=True)
def test_blacklist_delta_changed(postgres, db_conn, tmpdir, mocked_config, logger, classification_data):
    """Test blacklist_delta_changed.

    Test that the delta between two listgen rows is OK when a new IMEI is added into
    the classification_state table for a blocking condition
    """
    # Populate the classification_state table with some initial data
    # imei_norm,cond_name,start_date,end_date,block_date
    # 12345678901227,duplicate_mk1,'2016-01-01',,'2016-04-01'
    # 12345678901230,duplicate_mk1,'2016-01-01',,'2016-02-01'
    # 12345678901233,duplicate_mk1,'2016-01-01',,'2016-04-01'
    # Run dirbs-listgen once, with curr_date set to ensure that at least one IMEI is blacklisted
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run0', date='20160201',
                                               delta_fn='blacklist_delta')
    assert rows_op_one == ['imei,block_date,reasons,change_type\n',
                           '12345678901230,20160201,Duplicate IMEI detected,blocked\n']

    # Add the same IMEI into classification_state with a different cond_name (blocking)
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""INSERT INTO classification_state (run_id, imei_norm, cond_name, start_date, end_date,
                                                            block_date, virt_imei_shard)
                               VALUES('1','12345678901230','gsma_not_found','2016-01-01',NULL,'2016-02-01',
                                      calc_virt_imei_shard('12345678901230'))""")
    # Run dirbs-listgen again, with curr_date set to ensure that the new condition is blacklisted
    # Assert that delta blacklist contains one row with the IMEI and change_type == 'changed'
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1', date='20160301',
                                               delta_fn='blacklist_delta')
    assert rows_op_one == ['imei,block_date,reasons,change_type\n',
                           '12345678901230,20160201,Duplicate IMEI detected|'
                           'TAC not found in GSMA TAC database,changed\n']


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v5.csv'],
                         indirect=True)
def test_notifications_list_delta_new(postgres, db_conn, tmpdir, mocked_config, logger,
                                      metadata_db_conn, mocked_statsd, classification_data):
    """Test notifications_list_delta_new."""
    # run common code for notification_list_delta tests to test classification_state and notification tables.
    _notification_list_classification_state_common_code(db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                                                        mocked_statsd)
    # Add a new triplet with the IMEI meeting a blocking condition

    import_operator_data(db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                         mocked_statsd, 17, content='date,imei,imsi,msisdn\n'
                                                    '20160222,12345678901233,11102678901234,1',
                         operator_id='operator2')
    rows_op_two, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1', date='20160301',
                                               delta_fn='notifications_operator2_delta')
    assert rows_op_two == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                           '12345678901233,11102678901234,1,20160401,Duplicate IMEI detected,new\n']


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v5.csv'],
                         indirect=True)
@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(content='imei,imsi,msisdn\n'
                                                 '12345678901230,11101678901234,222222222334443')],
                         indirect=True)
@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(content='GOLDEN_IMEI\n'
                                                   '12345678901227')],
                         indirect=True)
def test_notifications_list_delta_resolved(postgres, db_conn, tmpdir, mocked_config, logger, pairing_list_importer,
                                           metadata_db_conn, mocked_statsd, classification_data, golden_list_importer):
    """Test notifications_list_delta_resolved."""
    # run common code for notification_list_delta tests to test classification_state and notification tables.
    _notification_list_classification_state_common_code(db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                                                        mocked_statsd)
    # Add a pairing to the pairing list for one IMEI-IMSI and re-run dirbs-listgen ('12345678901230,11101678901234')
    # IMEIs in notification_list before pairing (12345678901227, 12345678901230). Expect to remove 12345678901230.
    import_data(pairing_list_importer, 'pairing_list', 1, db_conn, logger)
    # Run dirbs-listgen, triplets with that IMEI-IMSI should now have a 'resolved' change_type
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1', date='20160301',
                                               delta_fn='notifications_operator1.csv')
    assert rows_op_one == ['imei,imsi,msisdn,block_date,reasons\n',
                           '12345678901227,11106678901234,1,20160401,Duplicate IMEI detected\n']

    rows_op_one = _read_rows_from_file('notifications_operator1_delta', tmpdir, dir_name='run1')
    assert rows_op_one == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                           '12345678901230,11101678901234,1,20160401,Duplicate IMEI detected,resolved\n']

    # Add the IMEI to the golden list(12345678901227)
    import_data(golden_list_importer, 'golden_list', 1, db_conn, logger)
    # Run dirbs-listgen, remaining triplets with that IMEI should now have a 'resolved' change_type
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run2', date='20160301',
                                               delta_fn='notifications_operator1.csv')
    assert rows_op_one == ['imei,imsi,msisdn,block_date,reasons\n']
    rows_op_one = _read_rows_from_file('notifications_operator1_delta', tmpdir, dir_name='run2')
    assert rows_op_one == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                           '12345678901227,11106678901234,1,20160401,Duplicate IMEI detected,resolved\n']
    # Remove IMEI from the golden list
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""DELETE FROM golden_list""")
    # Run dirbs-listgen, remaining triplets should be added back with 'new' change_type
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run3', date='20160301',
                                               delta_fn='notifications_operator1.csv')
    assert rows_op_one == ['imei,imsi,msisdn,block_date,reasons\n',
                           '12345678901227,11106678901234,1,20160401,Duplicate IMEI detected\n']
    rows_op_one = _read_rows_from_file('notifications_operator1_delta', tmpdir, dir_name='run3')
    assert rows_op_one == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                           '12345678901227,11106678901234,1,20160401,Duplicate IMEI detected,new\n']
    # Remove IMEI from the classification_state table by setting end_date to non-NULL
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""UPDATE classification_state
                             SET end_date = '20160201'
                           WHERE imei_norm = '12345678901227'""")
    # Run dirbs-listgen, remaining triplets with that IMEI should now have a 'resolved' change_type
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run4', date='20160301',
                                               delta_fn='notifications_operator1.csv')
    assert rows_op_one == ['imei,imsi,msisdn,block_date,reasons\n']
    rows_op_one_delta = _read_rows_from_file('notifications_operator1_delta', tmpdir, dir_name='run4')
    assert rows_op_one_delta == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                                 '12345678901227,11106678901234,1,20160401,Duplicate IMEI detected,resolved\n']


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v5.csv'],
                         indirect=True)
@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(content='imei,imsi,msisdn\n'
                                                 '12345678901230,11101678901234,12345678901230')],
                         indirect=True)
@pytest.mark.parametrize('golden_list_importer',
                         [GoldenListParams(content='GOLDEN_IMEI\n'
                                                   '12345678901233')],
                         indirect=True)
def test_notifications_list_delta_no_longer_seen(postgres, db_conn, tmpdir, mocked_config, logger,
                                                 pairing_list_importer, golden_list_importer,
                                                 metadata_db_conn, mocked_statsd, classification_data, monkeypatch):
    """Test notifications_list_delta_no_longer_seen."""
    # run common code for notification_list_delta tests to test classification_state and notification tables.
    _notification_list_classification_state_common_code(db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                                                        mocked_statsd)

    # If we reduce the lookback days to 0, we expect to see a whole bunch of no_longer_seen
    monkeypatch.setattr(mocked_config.listgen_config, 'lookback_days', 0)

    # Add a pairing to the pairing list for one IMEI-IMSI and re-run dirbs-listgen ('12345678901230,11101678901234')
    # IMEIs in notification_list before pairing (12345678901227, 12345678901230). Expect to remove 12345678901230.
    import_data(pairing_list_importer, 'pairing_list', 1, db_conn, logger)

    # Add the IMEI to the golden list(12345678901233)
    import_data(golden_list_importer, 'golden_list', 1, db_conn, logger)

    # Run dirbs-listgen, all triplets should now have a 'no_longer_seen' change_type
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1', date='20160301',
                                               delta_fn='notifications_operator1.csv')

    assert rows_op_one == ['imei,imsi,msisdn,block_date,reasons\n']

    rows_op_one = _read_rows_from_file('notifications_operator1_delta', tmpdir, dir_name='run1')
    assert rows_op_one[0] == 'imei,imsi,msisdn,block_date,reasons,change_type\n'
    # 12345678901227 should be no_longer_seen since it is not paired. 12345678901230 should be resolved since it
    # was paired in the pairing list import
    assert set(rows_op_one[1:]) == \
        {'12345678901227,11106678901234,1,20160401,Duplicate IMEI detected,no_longer_seen\n',
         '12345678901230,11101678901234,1,20160401,Duplicate IMEI detected,resolved\n'}

    rows_op_one = _read_rows_from_file('notifications_operator4.csv', tmpdir, dir_name='run1')
    assert rows_op_one == ['imei,imsi,msisdn,block_date,reasons\n']
    rows_op_one = _read_rows_from_file('notifications_operator4_delta', tmpdir, dir_name='run1')
    assert rows_op_one[0] == 'imei,imsi,msisdn,block_date,reasons,change_type\n'
    # 12345678901233 should be resolved since is it is on the golden_list -- should not be no_longer_seen
    assert set(rows_op_one[1:]) == {'12345678901233,11104678901234,1,20160401,Duplicate IMEI detected,resolved\n'}


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v5.csv'],
                         indirect=True)
def test_notifications_list_delta_blacklisted(postgres, db_conn, tmpdir, mocked_config, logger,
                                              metadata_db_conn, mocked_statsd, classification_data):
    """Test notifications_list_delta_changed."""
    # run common code for notification_list_delta tests to test classification_state and notification tables.
    _notification_list_classification_state_common_code(db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                                                        mocked_statsd)
    # Run dirbs-listgen again with a different curr_date after the block_date of that IMEI so that it on
    # the blacklist.
    # block date 20160401 - curr-date for list-gen 20160601
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1', date='20160601',
                                               delta_fn='notifications_operator1.csv')
    # All triplets should be on the delta notifications list with a change_type of 'blacklisted'
    assert rows_op_one == ['imei,imsi,msisdn,block_date,reasons\n']
    rows_op_one = _read_rows_from_file('notifications_operator1_delta', tmpdir, dir_name='run1')
    assert len(rows_op_one) == 3
    assert rows_op_one[0] == 'imei,imsi,msisdn,block_date,reasons,change_type\n'
    assert set(rows_op_one[1:]) == {'12345678901227,11106678901234,1,20160401,Duplicate IMEI detected,blacklisted\n',
                                    '12345678901230,11101678901234,1,20160401,Duplicate IMEI detected,blacklisted\n'}


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v5.csv'],
                         indirect=True)
def test_notifications_list_delta_changed(postgres, db_conn, tmpdir, mocked_config, logger,
                                          metadata_db_conn, mocked_statsd, classification_data):
    """Test notifications_list_delta_changed."""
    # run common code for notification_list_delta tests to test classification_state and notification tables.
    _notification_list_classification_state_common_code(db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                                                        mocked_statsd)

    # Insert new row in classification_state with another blocking condition's cond_name for the same
    # IMEI(12345678901230)
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""INSERT INTO classification_state (run_id, imei_norm, cond_name, start_date, end_date,
                                                            block_date, virt_imei_shard)
                               VALUES('1','12345678901230','gsma_not_found','2016-01-01',NULL,'2016-04-01',
                                      calc_virt_imei_shard('12345678901230'))""")
    # Run dirbs-listgen again, should see change_type of 'changed' for this IMEI and the delta list should
    # contain the new reasons (2) (pipe-delimited on one row)
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1', date='20160301',
                                               delta_fn='notifications_operator1.csv')
    assert len(rows_op_one) == 3
    assert rows_op_one[0] == 'imei,imsi,msisdn,block_date,reasons\n'
    assert set(rows_op_one[1:]) == {'12345678901227,11106678901234,1,20160401,Duplicate IMEI detected\n',
                                    '12345678901230,11101678901234,1,20160401,Duplicate IMEI detected|TAC '
                                    'not found in GSMA TAC database\n'}
    rows_op_one = _read_rows_from_file('notifications_operator1_delta', tmpdir, dir_name='run1')
    assert rows_op_one == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                           '12345678901230,11101678901234,1,20160401,'
                           'Duplicate IMEI detected|TAC not found in GSMA TAC database,changed\n']
    with db_conn, db_conn.cursor() as cursor:
        # Remove first row from classification_state table for that IMEI
        cursor.execute("""DELETE FROM classification_state
                                WHERE imei_norm = '12345678901230'
                                  AND cond_name = 'duplicate_mk1'""")
    # Run dirbs-listgen again, should see change_type of 'changed' for this IMEI and the delta
    # list should contain the new reasons (the new one only)
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run2', date='20160301',
                                               delta_fn='notifications_operator1.csv')
    assert len(rows_op_one) == 3
    assert rows_op_one[0] == 'imei,imsi,msisdn,block_date,reasons\n'
    assert set(rows_op_one[1:]) == {'12345678901227,11106678901234,1,20160401,Duplicate IMEI detected\n',
                                    '12345678901230,11101678901234,1,20160401,TAC not found in GSMA TAC database\n'}
    rows_op_one = _read_rows_from_file('notifications_operator1_delta', tmpdir, dir_name='run2')
    assert rows_op_one == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                           '12345678901230,11101678901234,1,20160401,TAC not found in GSMA TAC database,changed\n']
    with db_conn, db_conn.cursor() as cursor:
        # Change the block_date of the row in the classification_state table
        cursor.execute("""UPDATE classification_state
                             SET block_date = '20160403'
                           WHERE imei_norm IN ('12345678901230', '12345678901227')""")
    # Run dirbs-listgen again, should see change_type of 'changed' for this IMEI
    # and the delta list should contain the new reasons (the new one only) and the new block date
    rows_op_one, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run3', date='20160301',
                                               delta_fn='notifications_operator1.csv')
    rows_op_one = _read_rows_from_file('notifications_operator1.csv', tmpdir, dir_name='run3')
    assert len(rows_op_one) == 3
    assert rows_op_one[0] == 'imei,imsi,msisdn,block_date,reasons\n'
    assert set(rows_op_one[1:]) == {'12345678901227,11106678901234,1,20160403,Duplicate IMEI detected\n',
                                    '12345678901230,11101678901234,1,20160403,TAC not found in GSMA TAC database\n'}
    delta_fn = 'notifications_operator1_delta'
    rows_op_one = _read_rows_from_file(delta_fn, tmpdir, dir_name='run3')
    assert len(rows_op_one) == 3
    assert rows_op_one[0] == 'imei,imsi,msisdn,block_date,reasons,change_type\n'
    assert set(rows_op_one[1:]) == {'12345678901227,11106678901234,1,20160403,Duplicate IMEI detected,changed\n',
                                    '12345678901230,11101678901234,1,20160403,TAC not found in '
                                    'GSMA TAC database,changed\n'}


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(content='imei,imsi,msisdn\n'
                                                 '12345678901228,11105678901234,12345678901228\n'
                                                 '12345678901229,11106678901234,12345678901229\n'
                                                 '12345678901230,11107678901234,12345678901230\n'
                                                 '12345678901231,11108678901234,12345678901231\n'
                                                 '12345678901232,11109678901234,12345678901232\n'
                                                 '12345678901227,11110678901234,12345678901227\n'
                                                 '12345678901233,11101678901234,12345678901233\n'
                                                 '12345678901234,11101678901234,12345678901234\n'
                                                 '12345678901235,11102678901234,12345678901235\n'
                                                 '12345678901236,11102678901234,12345678901236')],
                         indirect=True)
@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161122,12345678901228,11105678901234,1\n'
                                     '20161122,12345678901229,11106678901234,1\n'
                                     '20161122,12345678901230,11107678901234,1',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_leading_zero_check=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
def test_exceptions_list_delta_added(postgres, db_conn, tmpdir, mocked_config, pairing_list_importer, logger,
                                     operator_data_importer, metadata_db_conn, mocked_statsd):
    """Test exceptions_list_delta_added."""
    # Populate pairing list with some IMEI-IMSI pairs
    # Have at least some IMSIs (at least 3) where the MCC-MNC does not start with the prefix for any operator
    # '12345678901228,11105678901234\n'
    # '12345678901229,11106678901234\n'
    # '12345678901230,11107678901234\n'
    # '12345678901231,11108678901234\n'
    # '12345678901232,11109678901234\n'
    # '12345678901233,11110678901234\n'
    # Have some IMSIs that start with operator 1's prefix
    # '12345678901233,11101678901234\n'
    # '12345678901234,11101678901234\n'
    # Have some IMSIs that start with operator 2's prefix
    # '12345678901235,11102678901234\n'
    # '12345678901236,11102678901234\n'
    with db_conn:
        expect_success(pairing_list_importer, 10, db_conn, logger)
    # Populate monthly_network_triplets table
    # Put some of the "unknown" IMSIs into operator 1's operator data
    # '12345678901228,11105678901234\n'
    # '20161122,12345678901230,11107678901234,1\n',
    # Put some of the "unknown" IMSIs into operator 2's operator data
    # '20161121,12345678901231,11108678901234,1\n',
    # Put some of the "unknown" IMSIs into both operator's data
    # '20161122,12345678901229,11106678901234,1\n'
    # Some of the "unknown" IMSIs should not be in any operator's data
    # '12345678901232,11109678901234\n'
    # '12345678901227,11110678901234\n'
    expect_success(operator_data_importer, 3, db_conn, logger)
    import_operator_data(db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                         mocked_statsd, 5, content='date,imei,imsi,msisdn\n'
                                                   '20161122,12345678901229,11106678901234,1\n'
                                                   '20161121,12345678901231,11108678901234,1', operator_id='operator2')
    # Run dirbs-listgen
    rows_op_one_list, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run0', date='20160301',
                                                    delta_fn='exceptions_operator1_delta')
    # verify that:
    # - Delta exceptions list for operator 1 contains all the IMEI-IMSIs
    # where the prefix started with Operator 1's MCC-MNC (12345678901233, 12345678901234)
    # or where the IMSI did not match any operator
    # The change_type should be 'added'.
    assert len(rows_op_one_list) == 9
    assert rows_op_one_list[0] == 'imei,imsi,msisdn,change_type\n'
    assert set(rows_op_one_list[1:]) == {'12345678901228,11105678901234,12345678901228,added\n',
                                         '12345678901229,11106678901234,12345678901229,added\n',
                                         '12345678901230,11107678901234,12345678901230,added\n',
                                         '12345678901233,11101678901234,12345678901233,added\n',
                                         '12345678901234,11101678901234,12345678901234,added\n',
                                         '12345678901231,11108678901234,12345678901231,added\n',
                                         '12345678901227,11110678901234,12345678901227,added\n',
                                         '12345678901232,11109678901234,12345678901232,added\n'}

    # - Delta exceptions list for operator 2 contains all the IMEI-IMSIs where the prefix started with
    # Operator 2's MCC-MNC or where the IMSI did not match any operator. The change_type should be 'added'.
    rows_op_two_list = _read_rows_from_file('exceptions_operator2_delta', tmpdir, dir_name='run0')
    assert len(rows_op_two_list) == 9
    assert rows_op_two_list[0] == 'imei,imsi,msisdn,change_type\n'
    assert set(rows_op_two_list[1:]) == {'12345678901229,11106678901234,12345678901229,added\n',
                                         '12345678901231,11108678901234,12345678901231,added\n',
                                         '12345678901235,11102678901234,12345678901235,added\n',
                                         '12345678901236,11102678901234,12345678901236,added\n',
                                         '12345678901227,11110678901234,12345678901227,added\n',
                                         '12345678901232,11109678901234,12345678901232,added\n',
                                         '12345678901230,11107678901234,12345678901230,added\n',
                                         '12345678901228,11105678901234,12345678901228,added\n'}
    # - Delta exceptions list for all operators contain any "unknown" IMEIs that were not seen with any operator
    # rows_op_one is a list of str
    # i.e. ['imei,imsi,change_type\n','12345678901228,11105678901234,added\n','12345678901229,11106678901234,added\n']
    for rows_op_list in [rows_op_one_list, rows_op_two_list]:
        assert len(set([r[:14] for r in rows_op_list]) & {'12345678901232', '12345678901227', ''}) == 2
    # IMSIs that were "unknown" and seen with both operator 1 and operator 2 should be in both operators'
    # delta exceptions lists with a change_type of added.
    for rows_op_list in [rows_op_one_list, rows_op_two_list]:
        assert len(set([r[:14] for r in rows_op_list]) & {'12345678901229'}) == 1


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(content='imei,imsi,msisdn\n'
                                                 '12345678901230,11107678901234,333333333333331\n'
                                                 '12345678901231,11108678901234,333333333333332\n'
                                                 '12345678901232,11109678901234,333333333333333\n'
                                                 '12345678901233,11101678901234,333333333333334\n'
                                                 '12345678901234,11101678901234,333333333333335\n'
                                                 '12345678901235,11102678901234,333333333333336\n'
                                                 '12345678901236,11102678901234,333333333333337\n')],
                         indirect=True)
def test_exceptions_list_delta_removed(postgres, db_conn, tmpdir, mocked_config, pairing_list_importer, logger):
    """Test exceptions_list_delta_removed."""
    # Populate pairing list with some IMEI-IMSI pairs
    # Have at least some IMSIs (at least 3) where the MCC-MNC does not start with the prefix for any operator
    # '12345678901230,11107678901234\n'
    # '12345678901231,11108678901234\n'
    # '12345678901232,11109678901234\n'
    # Have some IMSIs that start with operator 1's prefix
    # '12345678901233,11101678901234\n'
    # '12345678901234,11101678901234\n'
    # Have some IMSIs that start with operator 2's prefix
    # '12345678901235,11102678901234\n'
    # '12345678901236,11102678901234\n'
    with db_conn:
        expect_success(pairing_list_importer, 7, db_conn, logger)
    # Run dirbs-listgen once
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run0',
                                        delta_fn='exceptions_operator1_delta')
    # Verify that rows have been added
    assert len(rows) == 6
    assert rows[0] == 'imei,imsi,msisdn,change_type\n'
    assert set(rows[1:]) == {'12345678901233,11101678901234,333333333333334,added\n',
                             '12345678901234,11101678901234,333333333333335,added\n',
                             '12345678901230,11107678901234,333333333333331,added\n',
                             '12345678901231,11108678901234,333333333333332,added\n',
                             '12345678901232,11109678901234,333333333333333,added\n'}
    rows = _read_rows_from_file('exceptions_operator2_delta', tmpdir, dir_name='run0')
    assert len(rows) == 6
    assert rows[0] == 'imei,imsi,msisdn,change_type\n'
    assert set(rows[1:]) == {'12345678901235,11102678901234,333333333333336,added\n',
                             '12345678901236,11102678901234,333333333333337,added\n',
                             '12345678901230,11107678901234,333333333333331,added\n',
                             '12345678901231,11108678901234,333333333333332,added\n',
                             '12345678901232,11109678901234,333333333333333,added\n'}
    rows = _read_rows_from_file('exceptions_operator3_delta', tmpdir, dir_name='run0')
    assert len(rows) == 4
    assert rows[0] == 'imei,imsi,msisdn,change_type\n'
    assert set(rows[1:]) == {'12345678901230,11107678901234,333333333333331,added\n',
                             '12345678901231,11108678901234,333333333333332,added\n',
                             '12345678901232,11109678901234,333333333333333,added\n'}
    # Remove a pairing per each operator delta list
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""DELETE
                            FROM pairing_list
                           WHERE imei_norm IN ('12345678901230', '12345678901233', '12345678901236')""")
    # Run dirbs-listgen, verify that:
    # All exception list deltas are empty except for the one pairing with a change_type of 'removed'
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1',
                                        delta_fn='exceptions_operator1_delta')
    assert len(rows) == 3
    assert rows[0] == 'imei,imsi,msisdn,change_type\n'
    assert set(rows[1:]) == {'12345678901233,11101678901234,333333333333334,removed\n',
                             '12345678901230,11107678901234,333333333333331,removed\n'}
    rows = _read_rows_from_file('exceptions_operator2_delta', tmpdir, dir_name='run1')
    assert len(rows) == 3
    assert rows[0] == 'imei,imsi,msisdn,change_type\n'
    assert set(rows[1:]) == {'12345678901236,11102678901234,333333333333337,removed\n',
                             '12345678901230,11107678901234,333333333333331,removed\n'}
    rows = _read_rows_from_file('exceptions_operator3_delta', tmpdir, dir_name='run1')
    assert len(rows) == 2
    assert set(rows[1:]) == {'12345678901230,11107678901234,333333333333331,removed\n'}


def test_aggregate_blacklist_changes(postgres, db_conn, tmpdir, mocked_config):
    """Test aggregate_blacklist_changes."""
    # Run dirbs-listgen once to create empty partitions
    _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run0', date='20160301')
    # Manually populate one operator's exceptions_list
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""INSERT into blacklist(imei_norm, block_date, reasons, start_run_id, end_run_id, delta_reason,
                                                virt_imei_shard)
                               VALUES('12345678901234', '20170201', ARRAY['condition2', 'condition3'],
                                      1116, NULL, 'unblocked', calc_virt_imei_shard('12345678901234')),
                                      ('12345678901234', '20170201', ARRAY['condition2', 'condition3'], 1113, 1116,
                                      'changed', calc_virt_imei_shard('12345678901234')),
                                      ('12345678901234', '20170301', ARRAY['condition2'], 1112, 1113, 'blocked',
                                       calc_virt_imei_shard('12345678901234')),
                                      ('12345678901234', '20170301', ARRAY['condition1'], 1004, 1112, 'unblocked',
                                       calc_virt_imei_shard('12345678901234')),
                                      ('12345678901234', '20170301', ARRAY['condition1'], 1000, 1004, 'blocked',
                                       calc_virt_imei_shard('12345678901234'))""")
        cursor.execute("""SELECT COUNT(*) AS count_bl FROM blacklist""")
        assert cursor.fetchone().count_bl == 5

    for i in [900, 1000, 1003, 1004, 1112, 1113, 1116]:
        job_metadata_importer(db_conn=db_conn, command='dirbs-listgen', run_id=i,
                              status='success', extra_metadata={})
    delta_fn = 'blacklist_delta'
    # Run dirbs-listgen with --base 900, should be no change
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1', delta_fn=delta_fn, base_run_id=900)
    assert rows == ['imei,block_date,reasons,change_type\n']
    # Run dirbs-listgen with --base 1000, should get change_type of unblocked
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run2', delta_fn=delta_fn,
                                        base_run_id=1000)
    assert rows == ['imei,block_date,reasons,change_type\n',
                    '12345678901234,20170201,condition2|condition3,unblocked\n']
    # Run dirbs-listgen with --base 1003, should get change_type of unblocked
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run3', delta_fn=delta_fn,
                                        base_run_id=1003)
    assert rows == ['imei,block_date,reasons,change_type\n',
                    '12345678901234,20170201,condition2|condition3,unblocked\n']
    # Run dirbs-listgen with --base 1004 should be no change
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run4', delta_fn=delta_fn,
                                        base_run_id=1004)
    assert rows == ['imei,block_date,reasons,change_type\n']
    # Run dirbs-listgen with --base 1112 should get change_type of unblocked
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run5', delta_fn=delta_fn,
                                        base_run_id=1112)
    assert rows == ['imei,block_date,reasons,change_type\n',
                    '12345678901234,20170201,condition2|condition3,unblocked\n']
    # Run dirbs-listgen with --base 1113 should get change_type of unblocked
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run6', delta_fn=delta_fn,
                                        base_run_id=1113)
    assert rows == ['imei,block_date,reasons,change_type\n',
                    '12345678901234,20170201,condition2|condition3,unblocked\n']
    # Run dirbs-listgen with --base 1116 should be no change
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run7', delta_fn=delta_fn,
                                        base_run_id=1116)
    assert rows == ['imei,block_date,reasons,change_type\n']


def test_aggregate_notifications_list_changes(postgres, db_conn, tmpdir, mocked_config):
    """Test aggregate_notifications_list_changes."""
    # Run dirbs-listgen once to create empty partitions
    _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run0')
    # Manually populate one operator's notifications_list
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""INSERT INTO notifications_lists_operator1 (operator_id, imei_norm, imsi, msisdn, block_date,
                                                                     reasons, start_run_id, end_run_id, delta_reason,
                                                                     virt_imei_shard)
                               VALUES ('operator1', '12345678901234', '12345678901234', '1', '20170110',
                                      ARRAY['condition1'], 1125, NULL, 'new', calc_virt_imei_shard('12345678901234')),
                                      ('operator1', '12345678901234', '12345678901234', '1', '20170110',
                                      ARRAY['condition1'], 1122, 1125, 'blacklisted',
                                      calc_virt_imei_shard('12345678901234')),
                                      ('operator1', '12345678901234', '12345678901234', '1', '20170110',
                                      ARRAY['condition1'], 1121, 1122, 'changed',
                                      calc_virt_imei_shard('12345678901234')),
                                      ('operator1', '12345678901234', '12345678901234', '1', '20170105',
                                      ARRAY['condition1', 'condition2'], 1120, 1121, 'changed',
                                      calc_virt_imei_shard('12345678901234')),
                                      ('operator1', '12345678901234', '12345678901234', '1', '20170101',
                                      ARRAY['condition1', 'condition2', 'condition3'], 1116, 1120, 'new',
                                      calc_virt_imei_shard('12345678901234')),
                                      ('operator1', '12345678901234', '12345678901234', '1', '20170101',
                                      ARRAY['condition1', 'condition2', 'condition3'], 1113, 1116, 'resolved',
                                      calc_virt_imei_shard('12345678901234')),
                                      ('operator1', '12345678901234', '12345678901234', '1', '20170101',
                                      ARRAY['condition1', 'condition2', 'condition3'], 1112, 1113, 'changed',
                                      calc_virt_imei_shard('12345678901234')),
                                      ('operator1', '12345678901234', '12345678901234', '1', '20170201',
                                      ARRAY['condition1', 'condition2'], 1004, 1112, 'changed',
                                      calc_virt_imei_shard('12345678901234')),
                                      ('operator1', '12345678901234', '12345678901234', '1', '20170301',
                                      ARRAY['condition1'], 1000, 1004, 'new', calc_virt_imei_shard('12345678901234'))
                                                                   """)
        cursor.execute("""SELECT COUNT(*) AS count_nl FROM notifications_lists_operator1""")
        assert cursor.fetchone().count_nl == 9

    for i in [900, 1000, 1003, 1004, 1112, 1113, 1116, 1120, 1121, 1122, 1125]:
        job_metadata_importer(db_conn=db_conn, command='dirbs-listgen', run_id=i,
                              status='success', extra_metadata={})
    # delta filename
    delta_fn = 'notifications_operator1_delta'
    # Run dirbs-listgen with --base 900, should be "new"
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1', delta_fn=delta_fn, base_run_id=900)
    assert rows == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                    '12345678901234,12345678901234,1,20170110,condition1,new\n']
    # Run dirbs-listgen with --base 1000, should get change_type of "changed"
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run2', delta_fn=delta_fn,
                                        base_run_id=1000)
    assert rows == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                    '12345678901234,12345678901234,1,20170110,condition1,changed\n']
    # Run dirbs-listgen with --base 1003, should get change_type of "changed"
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run3', delta_fn=delta_fn,
                                        base_run_id=1003)
    assert rows == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                    '12345678901234,12345678901234,1,20170110,condition1,changed\n']
    # Run dirbs-listgen with --base 1004, should get change_type of "changed"
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run4', delta_fn=delta_fn,
                                        base_run_id=1004)
    assert rows == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                    '12345678901234,12345678901234,1,20170110,condition1,changed\n']
    # Run dirbs-listgen with --base 1112, should get change_type of "changed"
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run5', delta_fn=delta_fn,
                                        base_run_id=1112)
    assert rows == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                    '12345678901234,12345678901234,1,20170110,condition1,changed\n']
    # Run dirbs-listgen with --base 1113 should get change_type of "new"
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run6', delta_fn=delta_fn,
                                        base_run_id=1113)
    assert rows == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                    '12345678901234,12345678901234,1,20170110,condition1,new\n']
    # Run dirbs-listgen with --base 1116 should get change_type of "changed"
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run7', delta_fn=delta_fn,
                                        base_run_id=1116)
    assert rows == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                    '12345678901234,12345678901234,1,20170110,condition1,changed\n']
    # Run dirbs-listgen with --base 1120 should get change_type of "changed"
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run8', delta_fn=delta_fn,
                                        base_run_id=1120)
    assert rows == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                    '12345678901234,12345678901234,1,20170110,condition1,changed\n']
    # Run dirbs-listgen with --base 1121 should be no change
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run9', delta_fn=delta_fn,
                                        base_run_id=1121)
    assert rows == ['imei,imsi,msisdn,block_date,reasons,change_type\n']
    # Run dirbs-listgen with --base 1122 should get change_type of "new"
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run10', delta_fn=delta_fn,
                                        base_run_id=1122)
    assert rows == ['imei,imsi,msisdn,block_date,reasons,change_type\n',
                    '12345678901234,12345678901234,1,20170110,condition1,new\n']
    # Run dirbs-listgen with --base 1125 should be no change
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run11', delta_fn=delta_fn,
                                        base_run_id=1125)
    assert rows == ['imei,imsi,msisdn,block_date,reasons,change_type\n']


def test_aggregate_exceptions_list_changes(postgres, db_conn, tmpdir, mocked_config):
    """Test aggregate_exceptions_list_changes."""
    # Run dirbs-listgen once to create empty partitions
    _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run0')
    # Manually populate one operator's exceptions_list
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""INSERT INTO exceptions_lists_operator1 (operator_id, imei_norm, imsi, start_run_id,
                                                                  end_run_id, delta_reason, virt_imei_shard, msisdn)
                               VALUES ('operator1', '12345678901234', '12345678901234', 1116, NULL, 'removed',
                                       calc_virt_imei_shard('12345678901234'), '12345678901234'),
                                      ('operator1', '12345678901234', '12345678901234', 1113, 1116, 'added',
                                       calc_virt_imei_shard('12345678901234'), '12345678901234'),
                                      ('operator1', '12345678901234', '12345678901234', 1112, 1113, 'removed',
                                       calc_virt_imei_shard('12345678901234'), '12345678901234'),
                                      ('operator1', '12345678901234', '12345678901234', 1004, 1112, 'added',
                                       calc_virt_imei_shard('12345678901234'), '12345678901234'),
                                      ('operator1', '12345678901234', '12345678901234', 1000, 1004, 'removed',
                                       calc_virt_imei_shard('12345678901234'), '12345678901234')""")
        cursor.execute('SELECT COUNT(*) AS count_ex FROM exceptions_lists_operator1')
        assert cursor.fetchone().count_ex == 5

    for i in [900, 1000, 1003, 1116, 1113, 1112, 1004]:
        job_metadata_importer(db_conn=db_conn, command='dirbs-listgen', run_id=i,
                              status='success', extra_metadata={})
    # Run dirbs-listgen with --base 900, should throw an exception
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run1', base_run_id=900)
    assert rows == ['imei,imsi,msisdn,change_type\n', '12345678901234,12345678901234,12345678901234,removed\n']
    # Run dirbs-listgen with --base 1000, should throw an exception
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run2', base_run_id=1000)
    assert rows == ['imei,imsi,msisdn,change_type\n']
    # Run dirbs-listgen with --base 1003, should be no change
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run3', base_run_id=1003)
    assert rows == ['imei,imsi,msisdn,change_type\n']
    # Run dirbs-listgen with --base 1004, should be no change
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run4', base_run_id=1004)
    assert rows == ['imei,imsi,msisdn,change_type\n', '12345678901234,12345678901234,12345678901234,removed\n']
    # Run dirbs-listgen with --base 1112, should be no change
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run5', base_run_id=1112)
    assert rows == ['imei,imsi,msisdn,change_type\n']
    # Run dirbs-listgen with --base 1113 should be "removed"
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run6', base_run_id=1113)
    assert rows == ['imei,imsi,msisdn,change_type\n', '12345678901234,12345678901234,12345678901234,removed\n']
    # Run dirbs-listgen with --base 1116 should be no change.
    rows, _ = _run_list_gen_rows_run_id(db_conn, tmpdir, mocked_config, 'run7', base_run_id=1116)
    assert rows == ['imei,imsi,msisdn,change_type\n']


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state.csv'],
                         indirect=True)
def test_store_list_in_db_blacklist(classification_data, db_conn, tmpdir, mocked_config):
    """Test blacklist table.

    Verify that dirbs-listgen stores balcklist in the database with run_id and operator_id
    columns where appropriate.
    """
    _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config)
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute('SELECT * FROM gen_blacklist() ORDER BY imei_norm')
        blacklist_entries = [(x.imei_norm, x.block_date) for x in cursor.fetchall()]
        assert blacklist_entries == [('35000000000000', datetime.date(2016, 4, 1)),
                                     ('35111111111110', datetime.date(2016, 4, 1)),
                                     ('35900000000000', datetime.date(2016, 4, 1)),
                                     ('86222222222226', datetime.date(2016, 4, 1))]


@pytest.mark.parametrize('operator_data_importer, pairing_list_importer',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,811111013136460,111018001111111,223338000000\n'
                                     '20160203,311111060451100,111025111111111,223355000000\n'
                                     '20160203,411111013659809,310035111111111,743614000000',
                             operator='operator1',
                             cc=['22', '74'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}, {'mcc': '111', 'mnc': '02'},
                                            {'mcc': '310', 'mnc': '03'}],
                             extract=False),
                           PairListParams(
                               content='imei,imsi,msisdn\n'
                                       '811111013136460,111018001111111,444444444444441\n'
                                       '311111060451100,111025111111111,444444444444442\n'
                                       '411111013659809,310035111111111,444444444444443'))],
                         indirect=True)
def test_store_list_in_db_exception_list(operator_data_importer, mocked_config,
                                         pairing_list_importer, logger, db_conn, tmpdir):
    """Test exception-list table.

    Test that dirbs-listgen stores exception list in the database with run_id and operator_id
    columns where appropriate.
    """
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)
    import_data(pairing_list_importer, 'pairing_list', 3, db_conn, logger)
    _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160203')
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""SELECT imei_norm, imsi, msisdn
                            FROM gen_exceptions_list('operator1')
                        ORDER BY imei_norm""")
        exception_list_entries = {(x.imei_norm, x.imsi, x.msisdn) for x in cursor.fetchall()}
        assert exception_list_entries == {('41111101365980', '310035111111111', '444444444444443'),
                                          ('81111101313646', '111018001111111', '444444444444441')}

        cursor.execute("""SELECT imei_norm, imsi, msisdn
                            FROM gen_exceptions_list('operator2')
                        ORDER BY imei_norm""")
        exception_list_entries = {(x.imei_norm, x.imsi, x.msisdn) for x in cursor.fetchall()}
        assert exception_list_entries == {('31111106045110', '111025111111111', '444444444444442'),
                                          ('41111101365980', '310035111111111', '444444444444443')}


@pytest.mark.parametrize('operator_data_importer, classification_data',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,86222222222226,111018001111111,223338000000\n'
                                     '20160203,35111111111110,111015111111111,223355000000\n'
                                     '20160203,35900000000000,310035111111111,743614000000',
                             operator='operator1',
                             cc=['22', '74'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}, {'mcc': '310', 'mnc': '03'}],
                             extract=False),
                           'classification_state/imei_api_class_state.csv')],
                         indirect=True)
def test_store_list_in_db_notification_list(operator_data_importer, mocked_config,
                                            classification_data, logger, db_conn, tmpdir):
    """Test notification-list table.

    Verify that dirbs-listgen stores notification list in the database with run_id and operator_id
    columns where appropriate.
    """
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)
    # Run dirbs-listgen using db args from the temp postgres instance
    runner = CliRunner()
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date=20170101'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160203')
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""SELECT imei_norm, imsi FROM gen_notifications_list('operator1') ORDER BY imei_norm""")
        notif_list_entries = {(x.imei_norm, x.imsi) for x in cursor.fetchall()}

        assert notif_list_entries == {('35111111111110', '111015111111111'),
                                      ('35900000000000', '310035111111111'),
                                      ('86222222222226', '111018001111111')}


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state.csv'],
                         indirect=True)
def test_basic_cli_listgen(postgres, classification_data, db_conn, tmpdir, mocked_config):
    """Test that the dirbs-listgen instance runs without an error."""
    run_id, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config)
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    for list_type in ['exceptions', 'notifications']:
        # make sure exception and notification lists are generated for all operators
        _verify_per_operator_lists_generated(dir_path, list_type)


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state.csv'],
                         indirect=True)
def test_basic_cli_listgen_zip_files(postgres, classification_data, db_conn, tmpdir, mocked_config):
    """Test that the dirbs-listgen instance runs without an error and generates zip files with the right members."""
    run_id, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config)
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    zip_fn = find_file_in_dir('*blacklist.zip', dir_path)
    with zipfile.ZipFile(zip_fn, mode='r') as zf:
        members = zf.namelist()
        assert len(members) == 4
        assert any([re.match(r'^.*blacklist\.csv$', m) is not None for m in members])
        for change_type in ['blocked', 'unblocked', 'changed']:
            assert any([re.match(r'^.*blacklist_.*_{0}\.csv$'.format(change_type), m) is not None for m in members])

    for op_id in range(1, 5):
        zip_fn = find_file_in_dir('*notifications_operator{0}.zip'.format(op_id), dir_path)
        with zipfile.ZipFile(zip_fn, mode='r') as zf:
            members = zf.namelist()
            assert len(members) == 6
            assert any([re.match(r'^.*notifications_operator{0}.csv$'.format(op_id), m) is not None for m in members])
            for change_type in ['new', 'resolved', 'blacklisted', 'no_longer_seen', 'changed']:
                assert any([re.match(r'^.*notifications_operator{0}_delta_.*_{1}\.csv$'.format(op_id, change_type), m)
                            is not None for m in members])

        zip_fn = find_file_in_dir('*exceptions_operator{0}.zip'.format(op_id), dir_path)
        with zipfile.ZipFile(zip_fn, mode='r') as zf:
            members = zf.namelist()
            assert len(members) == 3
            assert any([re.match('^.*exceptions_operator{0}.csv$'.format(op_id), m) is not None for m in members])
            for change_type in ['added', 'removed']:
                assert any([re.match(r'^.*exceptions_operator{0}_delta_.*_{1}\.csv$'.format(op_id, change_type), m)
                            is not None for m in members])


@pytest.mark.parametrize('operator_data_importer, pairing_list_importer',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,811111013136460,111018001111111,223338000000\n'
                                     '20160203,311111060451100,111025111111111,223355000000\n'
                                     '20160203,411111013659809,310035111111111,743614000000',
                             operator='operator1',
                             cc=['22', '74'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}, {'mcc': '111', 'mnc': '02'},
                                            {'mcc': '310', 'mnc': '03'}],
                             extract=False),
                           PairListParams(
                               content='imei,imsi,msisdn\n'
                                       '811111013136460,111018001111111,555555555555551\n'
                                       '311111060451100,111025111111111,555555555555552\n'
                                       '411111013659809,310035111111111,555555555555553'))],
                         indirect=True)
def test_exception_listgen_no_home_network(postgres, operator_data_importer, mocked_config,
                                           pairing_list_importer, logger, db_conn, tmpdir):
    """Test that dirbs-listgen generates put pairing with no MCC-MNC match on every MNO's exception list."""
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)
    import_data(pairing_list_importer, 'pairing_list', 3, db_conn, logger)
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160203')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    # Check to make sure exception list is generated for all operators in config file
    _verify_per_operator_lists_generated(dir_path, 'exceptions')
    fn = find_file_in_dir('*exceptions_operator1.csv', dir_path)
    # Check IMSI matching operator1 prefix is added to correct exception list
    with open(fn, 'r') as file:
        rows = file.readlines()
        assert len(rows) == 3
        assert ('81111101313646,111018001111111,555555555555551\n') in rows
        assert ('41111101365980,310035111111111,555555555555553\n') in rows

    fn = find_file_in_dir('*exceptions_operator2.csv', dir_path)
    # Check IMSI matching operator2 prefix is added to correct exception list
    with open(fn, 'r') as file:
        rows = file.readlines()
        assert len(rows) == 3
        assert ('31111106045110,111025111111111,555555555555552\n') in rows
        assert ('41111101365980,310035111111111,555555555555553\n') in rows

    fn = find_file_in_dir('*exceptions_operator3.csv', dir_path)
    # Check IMSI matching operator2 prefix is added to correct exception list
    with open(fn, 'r') as file:
        rows = file.readlines()
        assert len(rows) == 2
        assert ('41111101365980,310035111111111,555555555555553\n') in rows


@pytest.mark.parametrize('operator_data_importer, pairing_list_importer, classification_data',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,811111013136460,111018001111111,223338000000\n'
                                     '20160203,359000000000000,111015113222222,223355000000\n'
                                     '20160203,357756065985824,111015113333333,223355111111',
                             cc=['22', '74'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}],
                             operator='operator1',
                             extract=False),
                           PairListParams(
                               content='imei,imsi,msisdn\n'
                                       '811111013136460,111018001111111,666666666666661\n'
                                       '359000000000000,111015113222222,666666666666662\n'
                                       '357756065985824,111015113333333,666666666666663'),
                           'classification_state/imei_api_class_state_v2.csv')],
                         indirect=True)
def test_exception_listgen_with_only_blacklisted_imeis_for_valid_conditions(postgres, operator_data_importer,
                                                                            pairing_list_importer, monkeypatch,
                                                                            classification_data, mocked_config,
                                                                            logger, db_conn, tmpdir):
    """Test that dirbs-listgen generates exception lists without the blacklisted IMEIs.

    All the IMEIs in the exception list need to have valid conditions to not be ignored.
    """
    # IMEI 35900000000000 in the exception list is not ignored because local_stolen condition is valid.
    # valid condition = [local_stolen]
    # imei_api_class_state_v2.csv contains the following row:
    # imei_norm, cond_name, start_date, end_date, block_date
    # '35900000000000,111015113222222, local_stolen
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)
    import_data(pairing_list_importer, 'pairing_list', 3, db_conn, logger)
    # Run dirbs-listgen using db args from the temp postgres instance
    cond = {
        'label': 'local_stolen',
        'reason': 'IMEI found on local stolen list',
        'blocking': True,
        'dimensions': [{
            'module': 'stolen_list'}]
    }
    monkeypatch.setattr(mocked_config, 'conditions', [ConditionConfig(ignore_env=True, **cond)])
    monkeypatch.setattr(mocked_config.listgen_config, 'restrict_exceptions_list', True)
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160401')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    fn = find_file_in_dir('*exceptions_operator1.csv', dir_path)
    with open(fn, 'r') as file:
        rows = file.readlines()
        # Assert two rows in file; with one being the header.
        assert len(rows) == 2
        # Check non-blacklisted IMEI on the pairing list is not on the exception list
        assert ('81111101313646,111018001111111,666666666666661\n') not in rows
        assert ('35775606598582,111015113333333,666666666666663\n') not in rows
        # Check IMEI on the blacklist is on the exception list
        assert ('35900000000000,111015113222222,666666666666662\n') in rows


@pytest.mark.parametrize('operator_data_importer, pairing_list_importer, classification_data',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,811111013136460,111018001111111,223338000000\n'
                                     '20160203,359000000000000,111015113222222,223355000000\n'
                                     '20160203,357756065985824,111015113333333,223355111111',
                             cc=['22', '74'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}],
                             operator='operator1',
                             extract=False),
                           PairListParams(
                               content='imei,imsi,msisdn\n'
                                       '811111013136460,111018001111111,777777777777771\n'
                                       '359000000000000,111015113222222,777777777777772\n'
                                       '357756065985824,111015113333333,777777777777773'),
                           'classification_state/imei_api_class_state_v1.csv')],
                         indirect=True)
def test_exception_listgen_ignores_invalid_conditions(postgres, operator_data_importer, pairing_list_importer,
                                                      classification_data, logger, db_conn, tmpdir,
                                                      mocked_config, monkeypatch):
    """Verify that all entries in the exception list with invalid conditions are ignored."""
    # IMEI '35900000000000' on the blacklist should be on the exception list but is ignored
    # because the condition names are not valid (duplicate_mk1, crazy_name)
    # valid condition = ['stolen_list']
    # imei_api_class_state_v1.csv significant rows:
    # imei_norm, cond_name, start_date, end_date, block_date
    # 35900000000000,duplicate_mk1,'2016-01-01',,'2016-04-01'
    # 35900000000000,crazy_name,'2016-01-01',,'2016-04-01'
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)
    import_data(pairing_list_importer, 'pairing_list', 3, db_conn, logger)
    cond = {
        'label': 'local_stolen',
        'reason': 'IMEI found on local stolen list',
        'blocking': True,
        'dimensions': [{
            'module': 'stolen_list'}]
    }
    monkeypatch.setattr(mocked_config, 'conditions', [ConditionConfig(ignore_env=True, **cond)])
    monkeypatch.setattr(mocked_config.listgen_config, 'restrict_exceptions_list', True)
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160401')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    fn = find_file_in_dir('*exceptions_operator1.csv', dir_path)
    with open(fn, 'r') as file:
        rows = file.readlines()
        # IMEI '35900000000000' on the blacklist should be on the exception list but is ignored
        # because the condition name is not a valid one (stolen_list)
        assert ('35900000000000,111015113222222,777777777777772\n') not in rows


@pytest.mark.parametrize('operator_data_importer, classification_data',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,86222222222226,111018001111111,223338000000\n'
                                     '20160203,35111111111110,111015111111111,223355000000\n'
                                     '20160203,35900000000000,310035111111111,743614000000',
                             operator='operator1',
                             cc=['22', '74'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}, {'mcc': '310', 'mnc': '03'}],
                             extract=False),
                           'classification_state/imei_api_class_state.csv')],
                         indirect=True)
def test_notification_listgen_with_no_fallback_records(postgres, operator_data_importer, mocked_config,
                                                       classification_data, logger, db_conn, tmpdir):
    """Test that dirbs-listgen generates per-operator notification lists and no operator_undetermined lists."""
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)
    runner = CliRunner()
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date=20170101'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160203')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    # Check to make sure notification list is generated for all operators in config file
    _verify_per_operator_lists_generated(dir_path, 'notifications')

    # Check IMSI matching operator1 prefix is added to correct notification list
    fn = find_file_in_dir('*notifications_operator1.csv', dir_path)
    with open(fn, 'r') as file:
        rows = [tuple(map(str, i.split(',')))[:4] for i in file]
        assert ('86222222222226', '111018001111111', '223338000000', '20160401') in rows
        assert ('35111111111110', '111015111111111', '223355000000', '20160401') in rows


@pytest.mark.parametrize('operator_data_importer, classification_data',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,86222222222226,111118001111111,223338000000\n'
                                     '20160203,35111111111110,111115111111111,223355000000\n'
                                     '20160203,35900000000000,111111111111111,743614000000',
                             operator='operator1',
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             extract=False),
                           'classification_state/imei_api_class_state.csv')],
                         indirect=True)
def test_notification_lists_with_seen_networks(postgres, operator_data_importer, mocked_config,
                                               classification_data, logger, db_conn, metadata_db_conn,
                                               tmpdir, mocked_statsd):
    """Test Depot ID Unknown.

    Test that dirbs-listgen put subscribers on every network they were seen with if IMSI does not match the home
    network for any configured operator.

    The IMSI prefix 11111 is used as it does not match any operator MCC-MNC configured in the config file used
    for unit testing. This means that we will fall back to the fallback method of looking at which operators
    that triplet was seen on.

    The same data is imported for 2 operators to simulate a roaming situation where the same triplet is seen
    with multiple operators with no clear home network identified. In this case, we expect the triplets to be
    output on both operator notification lists.
    """
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)

    # Import the same data set again as operator 2 so that the same triplets appear in 2 operator data sets
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          content='date,imei,imsi,msisdn\n'
                                  '20160203,86222222222226,111118001111111,223338000000\n'
                                  '20160203,35111111111110,111115111111111,223355000000\n'
                                  '20160203,35900000000000,111111111111111,743614000000',
                          operator='operator2',
                          perform_region_checks=False,
                          perform_home_network_check=False,
                          extract=False)) as operator_data_importer:
        import_data(operator_data_importer, 'operator_data', 6, db_conn, logger)

    runner = CliRunner()
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date=20170101'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160203')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    # Check to make sure notification list is generated for all operators in config file
    _verify_per_operator_lists_generated(dir_path, 'notifications')
    expected_header_cols = ['imei', 'imsi', 'msisdn', 'block_date', 'reasons']
    expected_rows = [['86222222222226', '111118001111111', '223338000000', '20160401'],
                     ['35900000000000', '111111111111111', '743614000000', '20160401'],
                     ['35111111111110', '111115111111111', '223355000000', '20160401']]

    # Make sure all triplets were seen on the operator 1 notifications list
    fn = find_file_in_dir('*notifications_operator1.csv', dir_path)
    with open(fn, 'r') as file:
        csvreader = csv.reader(file)
        rows = list(csvreader)
        assert len(rows) == 4
        assert rows[0] == expected_header_cols
        for er in expected_rows:
            assert er in [x[:4] for x in rows]

    # Make sure all triplets were seen on the operator 2 notifications list
    fn = find_file_in_dir('*notifications_operator2.csv', dir_path)
    with open(fn, 'r') as file:
        csvreader = csv.reader(file)
        rows = list(csvreader)
        assert len(rows) == 4
        assert rows[0] == expected_header_cols
        for er in expected_rows:
            assert er in [x[:4] for x in rows]


@pytest.mark.parametrize('operator_data_importer, classification_data',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,86222222222226,111018001111111,223338000000\n'
                                     '20160203,35111111111110,111015111111111,223355000000\n'
                                     '20160203,35900000000000,111011111111111,743614000000',
                             operator='operator1',
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             extract=False),
                           'classification_state/imei_api_class_state.csv')],
                         indirect=True)
def test_notification_lists_with_home_network_multiple_seen(postgres, operator_data_importer, mocked_config,
                                                            classification_data, logger, db_conn, metadata_db_conn,
                                                            tmpdir, mocked_statsd):
    """Test Depot ID Unnown.

    Test that dirbs-listgen only puts a notification on the home network even if a triplet is seen with
    multiple operators.

    The IMSI prefix 11101 is used as it matches the operator1 MCC-MNC prefix in the unit test config file for
    operator 1.
    The same data is imported for 2 operators to simulate a roaming situation where the same triplet is seen
    with multiple operators with a clear home network identified. In this case, we expect the triplets to be
    output on only operator 1, since it is the home network
    """
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)
    # Import the same data set again as operator 2 so that the same triplets appear in 2 operator data sets
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          content='date,imei,imsi,msisdn\n'
                                  '20160203,86222222222226,111018001111111,223338000000\n'
                                  '20160203,35111111111110,111015111111111,223355000000\n'
                                  '20160203,35900000000000,111011111111111,743614000000',
                          operator='operator2',
                          perform_region_checks=False,
                          perform_home_network_check=False,
                          extract=False)) as operator_data_importer:
        import_data(operator_data_importer, 'operator_data', 6, db_conn, logger)

    runner = CliRunner()
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date=20170101'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160203')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    file_list = os.listdir(dir_path)
    # Check to make sure notification list is generated for all operators in config file
    _verify_per_operator_lists_generated(dir_path, 'notifications')
    expected_header_cols = ['imei', 'imsi', 'msisdn', 'block_date', 'reasons']
    expected_rows = [['86222222222226', '111018001111111', '223338000000', '20160401'],
                     ['35900000000000', '111011111111111', '743614000000', '20160401'],
                     ['35111111111110', '111015111111111', '223355000000', '20160401']]

    # Make sure all triplets were seen on the operator 1 notifications list
    fn = find_file_in_dir('*notifications_operator1.csv', dir_path)
    with open(fn, 'r') as file:
        csvreader = csv.reader(file)
        rows = list(csvreader)
        assert len(rows) == 4
        assert rows[0] == expected_header_cols
        for er in expected_rows:
            assert er in [x[:4] for x in rows]

    fn = find_file_in_dir('*notifications_operator2.csv', dir_path)
    # No triplets should appear on tje operator 2 list
    with open(fn, 'r') as file:
        csvreader = csv.reader(file)
        rows = list(csvreader)
        assert len(rows) == 1
        assert rows[0] == expected_header_cols

    # Every record should have been seen on a network, so there should be no undetermined records
    assert not fnmatch.filter(file_list, '*exceptions_operator_undetermined.csv')
    assert not fnmatch.filter(file_list, '*notifications_operator_undetermined.csv')


@pytest.mark.parametrize('operator_data_importer, classification_data',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,86222222222226,,223338000000\n'
                                     '20160203,35111111111110,111015111111111,\n'
                                     '20160203,35900000000000,310035111111111,743614000000',
                             cc=['%'],
                             mcc_mnc_pairs=[{'mcc': '%', 'mnc': '%'}],
                             operator='operator1',
                             extract=False,
                             perform_null_checks=False,
                             perform_unclean_checks=False),
                           'classification_state/imei_api_class_state.csv')],
                         indirect=True)
def test_notification_listgen_with_null_imsi_msisdn(postgres, operator_data_importer, mocked_config,
                                                    classification_data, logger, db_conn, tmpdir):
    """Test that dirbs-listgen generates per-operator notification lists and no operator_undetermined lists."""
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)
    # Run dirbs-listgen using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date=20170101'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160203')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    # Check IMSI matching operator1 prefix is added to correct notification list
    fn = find_file_in_dir('*notifications_operator1.csv', dir_path)
    with open(fn, 'r') as file:
        rows = [tuple(map(str, i.split(',')))[:1] for i in file]
        # Check IMEI with NULL IMSI does not end up on the notification list
        assert ('86222222222226',) not in rows
        # Check IMEI with NULL MSISDN  does not end up on the notification list
        assert ('35111111111110',) not in rows
        # Check IMEI with valid IMSI/MSISDN does end up on the notification list
        assert ('35900000000000',) in rows

    file_list = os.listdir(dir_path)
    assert not fnmatch.filter(file_list, '*exceptions_operator_undetermined.csv')
    assert not fnmatch.filter(file_list, '*notifications_operator_undetermined.csv')
    assert len(file_list) > 0


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state.csv'],
                         indirect=True)
def test_blacklist_listgen(postgres, classification_data, db_conn, tmpdir, mocked_config):
    """Test that dirbs-listgen generates a single blacklist."""
    # Run dirbs-listgen using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir.mkdir('run1'))
    result = runner.invoke(dirbs_classify_cli, ['--curr-date=20170101'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    # Test empty blacklist
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160101')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    file_list = os.listdir(dir_path)
    assert fnmatch.filter(file_list, '*blacklist.csv')

    fn = find_file_in_dir('*blacklist.csv', dir_path)
    with open(fn, 'r') as file:
        rows = file.readlines()
        # Check if the file has a single row
        assert len(rows) == 1
        # Check that single row is the header
        assert 'imei,block_date,reasons\n' in rows

    # Test non-empty blacklist
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_2', mocked_config, date='20170101')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    file_list = os.listdir(dir_path)
    assert fnmatch.filter(file_list, '*blacklist.csv')

    fn = find_file_in_dir('*blacklist.csv', dir_path)
    with open(fn, 'r') as file:
        rows = [tuple(map(str, i.split(',')))[:1] for i in file]
        # Check IMEI with block date less than current date end up on the blacklist
        assert ('86222222222226',) in rows
        assert ('35000000000000',) in rows
        assert ('35900000000000',) in rows

        # Check IMEI with a non-NULL end date do not up on the blacklist
        assert ('35111111111110',) not in rows


@pytest.mark.parametrize('operator_data_importer, pairing_list_importer',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,811111013136460,111018001111111,223338000000\n'
                                     '20160203,311111060451100,111025111111111,223355000000\n'
                                     '20160203,411111013659809,310035111111111,743614000000',
                             operator='operator1',
                             cc=['22', '74'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}, {'mcc': '111', 'mnc': '02'},
                                            {'mcc': '310', 'mnc': '03'}],
                             extract=False),
                           PairListParams(
                               content='imei,imsi,msisdn\n'
                                       '811111013136460,111018001111111,888888888888881\n'
                                       '311111060451100,111025111111111,888888888888882\n'
                                       '411111013659809,310035111111111,888888888888883'))],
                         indirect=True)
def test_exception_listgen_with_luhn_check_digit(postgres, operator_data_importer, mocked_config, monkeypatch,
                                                 pairing_list_importer, logger, db_conn, tmpdir):
    """Test luhn check digit for exception list.

    Verify that dirbs-listgen generates per-operator exception lists containing IMEIs with luhn
    check digit for IMEI.
    """
    # The config setting to generate the Luhn digits is turned on in this test
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)
    import_data(pairing_list_importer, 'pairing_list', 3, db_conn, logger)
    monkeypatch.setattr(mocked_config.listgen_config, 'generate_check_digit', True)
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160203')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)

    # Check luhn check digit is in IMEI in exception list
    imei_luhn_one = luhn.append('81111101313646')
    imei_luhn_two = luhn.append('31111106045110')
    imei_luhn_three = luhn.append('41111101365980')

    # Check IMSI matching operator1 prefix is added to correct exception list
    fn = find_file_in_dir('*exceptions_operator1.csv', dir_path)
    with open(fn, 'r') as file:
        rows = file.readlines()
        assert (imei_luhn_one + ',111018001111111,888888888888881\n') in rows
        # Check IMSI not conforming to any operator prefix within config file,
        # but associated IMEI seen in operator data; is added to correct exception list
        assert (imei_luhn_three + ',310035111111111,888888888888883\n') in rows

    # Check IMSI matching operator2 prefix is added to correct exception list
    fn = find_file_in_dir('*exceptions_operator2.csv', dir_path)
    with open(fn, 'r') as file:
        rows = file.readlines()
        assert (imei_luhn_two + ',111025111111111,888888888888882\n') in rows


def test_luhn_check_digit_function(db_conn):
    """Test luhn check digit function."""
    imei_to_process = ['81111101313646', '31111106045110', '41111101365980']
    imei_lunh_from_module_list = [luhn.append(i) for i in imei_to_process]
    imei_lunh_from_sql_function_list = []
    with db_conn, db_conn.cursor() as cursor:
        for i in imei_to_process:
            cursor.callproc('luhn_check_digit_append', [i])
            imei_luhn_check_digit_append = cursor.fetchone().luhn_check_digit_append
            imei_lunh_from_sql_function_list.append(imei_luhn_check_digit_append)
            cursor.callproc('luhn_check_digit_verify', [imei_luhn_check_digit_append])
            assert cursor.fetchone().luhn_check_digit_verify is True

    assert imei_lunh_from_module_list == imei_lunh_from_sql_function_list


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_restrict_valid_imeis.csv'],
                         indirect=True)
def test_blacklist_listgen_output_invalid_imeis_and_luhn(postgres, mocked_config, monkeypatch,
                                                         classification_data, db_conn, tmpdir, logger):
    """Test luhn check digit for black list.

    Verify that list-gen is restricted to valid IMEIs if output_invalid_imeis is true.
    Verify that dirbs-listgen generates a single blacklist containing IMEIs with luhn check digit for IMEI
    if generate_check_digit is true and only if IMEIs are valid.
    """
    # The config setting to generate the Luhn digits is turned on in this test
    # restrict_valid_imeis.csv rows:
    # imei_norm, cond_name, start_date, end_date, block_date
    # 119300000000001, duplicate_mk1, '2016-01-01',, '2016-04-01' -- IMEI too long
    # 21AA0000000000, duplicate_mk1, '2016-01-01',, '2016-04-01'  -- IMEI hex
    # 312222222222BB, duplicate_mk1, '2016-01-01',, '2016-04-01'  -- IMEI hex
    # 41222222222226, duplicate_mk1, '2016-01-01',, '2016-04-01'  -- valid
    monkeypatch.setattr(mocked_config.listgen_config, 'generate_check_digit', True)
    monkeypatch.setattr(mocked_config.listgen_config, 'output_invalid_imeis', False)

    runner = CliRunner()
    result = runner.invoke(dirbs_classify_cli, ['--curr-date=20170101'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    # Test empty blacklist
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160101')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    fn = find_file_in_dir('*blacklist.csv', dir_path)
    with open(fn, 'r') as file:
        rows = file.readlines()
        # Check if the file has a single row
        assert len(rows) == 1
        # Check that single row is the header
        assert 'imei,block_date,reasons\n' in rows

    # Test non-empty blacklist
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_2', mocked_config, date='20170101')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    fn = find_file_in_dir('*blacklist.csv', dir_path)
    with open(fn, 'r') as file:
        rows = [tuple(map(str, i.split(',')))[:1] for i in file]

        # Append luhn check digit is in valid IMEI
        imei_luhn_one = luhn.append('41222222222226')
        assert rows == [('imei',), (imei_luhn_one,)]


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state.csv'],
                         indirect=True)
def test_blacklist_listgen_with_luhn_check_digit(postgres, mocked_config, monkeypatch,
                                                 classification_data, db_conn, tmpdir):
    """Test luhn check digit for black list.

    Verify that dirbs-listgen generates a single blacklist containing IMEIs with luhn check digit for IMEI
    if requested in yaml file using generate_check_digit param.
    """
    # The config setting to generate the Luhn digits is turned on in this test
    monkeypatch.setattr(mocked_config.listgen_config, 'generate_check_digit', True)
    runner = CliRunner()
    result = runner.invoke(dirbs_classify_cli, ['--curr-date=20170101'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    # Test empty blacklist
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160101')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    fn = find_file_in_dir('*blacklist.csv', dir_path)
    with open(fn, 'r') as file:
        rows = file.readlines()
        # Check if the file has a single row
        assert len(rows) == 1
        # Check that single row is the header
        assert 'imei,block_date,reasons\n' in rows

    # Test non-empty blacklist
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_2', mocked_config, date='20170101')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    fn = find_file_in_dir('*blacklist.csv', dir_path)
    with open(fn, 'r') as file:
        rows = [tuple(map(str, i.split(',')))[:1] for i in file]

        # Check luhn check digit is in IMEI with block date less than current date on the blacklist
        imei_luhn_one = luhn.append('86222222222226')
        imei_luhn_two = luhn.append('35000000000000')
        imei_luhn_three = luhn.append('35900000000000')

        assert (imei_luhn_one,) in rows
        assert (imei_luhn_two,) in rows
        assert (imei_luhn_three,) in rows


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v4.csv'],
                         indirect=True)
def test_listgen_with_only_informational_conditions(postgres, mocked_config, monkeypatch,
                                                    classification_data, db_conn, tmpdir):
    """Test that dirbs-listgen handles empty blocking_conditions."""
    # All conditions in the yaml have blocking set to False
    cond_dict_list = [{'label': 'local_stolen',
                       'reason': 'IMEI found on local stolen list',
                       'grace_period_days': 0,
                       'blocking': False,
                       'max_allowed_matching_ratio': 1.0,
                       'dimensions': [{'module': 'stolen_list'}]
                       },
                      {'label': 'malformed_imei',
                       'reason': 'Invalid characters detected in IMEI',
                       'grace_period_days': 30,
                       'blocking': False,
                       'dimensions': [{'module': 'inconsistent_rat'}]
                       }]

    # Run dirbs-listgen using db args from the temp postgres instance
    runner = CliRunner()
    monkeypatch.setattr(mocked_config, 'conditions', from_cond_dict_list_to_cond_list(cond_dict_list))
    result = runner.invoke(dirbs_classify_cli, ['--curr-date=20170101'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20170101')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    for f in glob.glob(os.path.join(dir_path, '*.csv')):
        with open(os.path.join(dir_path, f), 'r') as file:
            rows = [tuple(map(str, i.split(',')))[:1] for i in file]
            assert rows == [('imei',)]


all_cond_except_gsma_dict_list = [{'label': 'local_stolen',
                                   'reason': 'IMEI found on local stolen list',
                                   'grace_period_days': 0,
                                   'blocking': True,
                                   'max_allowed_matching_ratio': 1.0,
                                   'dimensions': [{'module': 'stolen_list'}]
                                   },
                                  {'label': 'malformed_imei',
                                   'reason': 'Invalid characters detected in IMEI',
                                   'grace_period_days': 30,
                                   'blocking': False,
                                   'dimensions': [{'module': 'inconsistent_rat'}]
                                   },
                                  {'label': 'inconsistent_rat',
                                   'reason': 'IMEI RAT inconsistent with device capability',
                                   'grace_period_days': 0,
                                   'blocking': False,
                                   'max_allowed_matching_ratio': 1.0,
                                   'dimensions': [{'module': 'malformed_imei'}]
                                   },
                                  {'label': 'duplicate_mk1',
                                   'reason': 'Duplicate IMEI detected',
                                   'grace_period_days': 60,
                                   'blocking': True,
                                   'sticky': True,
                                   'dimensions': [{'module': 'duplicate_threshold',
                                                   'parameters': {'threshold': 10, 'period_days': 120}},
                                                  {'module': 'duplicate_daily_avg',
                                                   'parameters': {'threshold': 4.0,
                                                                  'period_days': 30,
                                                                  'min_seen_days': 5}}]
                                   },
                                  {'label': 'not_on_registration_list',
                                   'reason': 'IMEI not found on local registration list',
                                   'grace_period_days': 0,
                                   'blocking': True,
                                   'max_allowed_matching_ratio': 1.0,
                                   'dimensions': [{'module': 'not_on_registration_list'}]
                                   }]


@pytest.mark.parametrize('operator_data_importer, classification_data',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,86222222222226,111018001111111,223338000000\n'
                                     '20160203,35111111111110,111015111111111,223355000000\n'
                                     '20160203,35900000000000,310035111111111,743614000000',
                             operator='operator1',
                             cc=['22', '74'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}, {'mcc': '310', 'mnc': '03'}],
                             extract=False),
                           'classification_state/imei_api_class_state.csv')],
                         indirect=True)
def test_notification_listgen_ignores_invalid_conditions(postgres, classification_data, monkeypatch,
                                                         operator_data_importer, mocked_config, logger, db_conn,
                                                         tmpdir):
    """Test that dirbs-listgen ignores IMEIs with invalid conditions in notification list."""
    # Only IMEI 86222222222226,duplicate_mk1 is in notification-list because duplicate_mk1 condition is valid.
    # valid conditions : all except gsma_not_found
    # imei_api_class_state.csv relevant rows:
    # imei_norm,cond_name,start_date,end_date,block_date
    # 86222222222226,gsma_not_found,'2016-01-01',,'2016-04-01'
    # 86222222222226,duplicate_mk1,'2016-01-01',,'2016-04-01'
    # 35111111111110,gsma_not_found,'2016-01-01',,'2016-04-01'
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)
    monkeypatch.setattr(mocked_config, 'conditions', from_cond_dict_list_to_cond_list(all_cond_except_gsma_dict_list))

    # Run dirbs-listgen using db args from the temp postgres instance
    runner = CliRunner()
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date=20170101'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160203')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)

    fn = find_file_in_dir('*notifications_operator1.csv', dir_path)
    with open(fn, 'r') as file:
        rows = [tuple(map(str, i.split(',')))[:4] for i in file]
        assert ('86222222222226', '111018001111111', '223338000000', '20160401') in rows
        assert ('35111111111110', '111015111111111', '223355000000', '20160401') not in rows


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v4.csv'],
                         indirect=True)
def test_blacklist_listgen_ignores_invalid_condition(postgres, monkeypatch, mocked_config,
                                                     classification_data, db_conn, tmpdir):
    """Test that dirbs-listgen ignores IMEIs with invalid conditions in black-list."""
    # Only IMEI 86222222222226,duplicate_mk1 is not in the black-list because
    # gsma_not_found cond is not a valid condition
    # valid conditions : all except gsma_not_found
    # imei_api_class_state_v4.csv relevant rows:
    # imei_norm, cond_name, start_date, end_date, block_date
    # 86222222222226, gsma_not_found, '2016-01-01',, '2016-04-01'
    # 86222222222226, duplicate_mk1, '2016-01-01',, '2016-04-01'
    # 35111111111110, gsma_not_found, '2016-01-01',, '2016-04-01'
    # 35000000000000, gsma_not_found, '2016-01-01',, '2016-04-01'
    # 35900000000000, gsma_not_found, '2016-01-01',, '2016-04-01'
    # Run dirbs-listgen using db args from the temp postgres instance
    monkeypatch.setattr(mocked_config, 'conditions', from_cond_dict_list_to_cond_list(all_cond_except_gsma_dict_list))
    runner = CliRunner()
    result = runner.invoke(dirbs_classify_cli, ['--curr-date=20170101'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20170101')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    fn = find_file_in_dir('*blacklist.csv', dir_path)
    with open(fn, 'r') as file:
        rows = [tuple(map(str, i.split(',')))[:1] for i in file]
        assert ('35000000000000',) not in rows
        assert ('35900000000000',) not in rows
        assert ('86222222222226',) in rows


@pytest.mark.parametrize('operator_data_importer, classification_data',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,86222222222226,111018001111111,223338000000\n'
                                     '20160203,35111111111110,111015111111111,223355000000\n'
                                     '20160203,35900000000000,310035111111111,743614000000',
                             operator='operator1',
                             cc=['22', '74'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}, {'mcc': '310', 'mnc': '03'}],
                             extract=False),
                           'classification_state/imei_api_class_state.csv')],
                         indirect=True)
def test_notification_listgen_with_luhn_check_digit(postgres, operator_data_importer, mocked_config, monkeypatch,
                                                    classification_data, logger, db_conn, tmpdir):
    """Test luhn check digit in notification list.

    Verify that dirbs-listgen generates IMEIs with Luhn check digit in notification list if specified in
    config settings.
    """
    # The config setting to generate the Luhn digits is turned on in this test
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)
    # Run dirbs-listgen using db args from the temp postgres instance
    runner = CliRunner()
    monkeypatch.setattr(mocked_config.listgen_config, 'generate_check_digit', True)
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date=20170101'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160203')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    # Check luhn check digit is in IMEI for notification list
    imei_luhn_one = luhn.append('86222222222226')
    imei_luhn_two = luhn.append('35111111111110')

    # Check IMSI matching operator1 prefix is added to correct notification list
    fn = find_file_in_dir('*notifications_operator1.csv', dir_path)
    with open(fn, 'r') as file:
        rows = [tuple(map(str, i.split(',')))[:4] for i in file]
        assert (imei_luhn_one, '111018001111111', '223338000000', '20160401') in rows
        assert (imei_luhn_two, '111015111111111', '223355000000', '20160401') in rows


@pytest.mark.parametrize('operator_data_importer, classification_data',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160203,3511AAB1111110,111015111111111,223355000000\n'
                                     '20160203,3BAA0000000000,310035111111111,743614000000',
                             operator='operator1',
                             cc=['22', '74'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}, {'mcc': '310', 'mnc': '03'}],
                             perform_unclean_checks=False,
                             extract=False),
                           'classification_state/imei_api_class_state_hex_char.csv')],
                         indirect=True)
def test_listgen_luhn_check_hex_character(postgres, operator_data_importer, mocked_config, monkeypatch,
                                          classification_data, logger, db_conn, tmpdir):
    """Test that dirbs-listgen does not add check digit to hexadecimal IMEIs even if generate_check_digit is true."""
    # The config setting to generate the Luhn digits is turned on in this test
    # verify that hex IMEIs don't have a 15 digit Luhn check added
    import_data(operator_data_importer, 'operator_data', 2, db_conn, logger)
    monkeypatch.setattr(mocked_config.listgen_config, 'generate_check_digit', True)
    # Run dirbs-listgen using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date=20170101'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20160203')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)

    # Check IMSI matching operator1 prefix is added to correct notification list without Luhn check digit
    fn = find_file_in_dir('*notifications_operator1.csv', dir_path)
    with open(fn, 'r') as file:
        rows = [tuple(map(str, i.split(',')))[:4] for i in file]
        assert ('3511AAB1111110', '111015111111111', '223355000000', '20160401') in rows
        assert ('3BAA0000000000', '310035111111111', '743614000000', '20160401') in rows


@pytest.mark.parametrize('pairing_list_importer, classification_data',
                         [(PairListParams(
                             content='imei,imsi,msisdn\n'
                                     '86222222222226,111018001111111,999999999999911\n'
                                     '35000000000000,310035111111111,999999999999912'),
                           'classification_state/imei_api_class_state.csv')],
                         indirect=True)
def test_blacklist_with_pairing_list(postgres, pairing_list_importer, classification_data, logger, db_conn, tmpdir,
                                     mocked_config):
    """Test that dirbs-listgen generates a single blacklist and includes IMEIs on pairing list."""
    import_data(pairing_list_importer, 'pairing_list', 2, db_conn, logger)
    # Run dirbs-listgen using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_classify_cli, ['--curr-date=20170101'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20170101')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    fn = find_file_in_dir('*blacklist.csv', dir_path)
    with open(fn, 'r') as file:
        rows = [tuple(map(str, i.split(',')))[:1] for i in file]
        # Check IMEI with block date less than current date end up on the blacklist
        assert ('86222222222226',) in rows
        assert ('35000000000000',) in rows
        assert ('35900000000000',) in rows

        # Check IMEI with a non-NULL end date do not up on the blacklist
        assert ('35111111111110',) not in rows


def test_cli_arg_no_cleanup(tmpdir, db_conn, mocked_config):
    """Test cleanup option for list-gen.

    That all tables are cleaned up when --no-cleanup is not used
    That tables are not cleaned up when --no-cleanup is used
    """
    with db_conn, db_conn.cursor() as cursor:
        run_id, _ = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, no_clean_up=True)
        cursor.execute("""SELECT TABLE_NAME
                            FROM information_schema.tables
                           WHERE TABLE_NAME LIKE 'listgen_temp_%'
                        ORDER BY TABLE_NAME""")
        res = [x.table_name for x in cursor.fetchall()]
        assert len(res) == 158
        expected_res = ['listgen_temp_1_delta_blacklist',
                        'listgen_temp_1_delta_blacklist_0_24',
                        'listgen_temp_1_delta_blacklist_25_49',
                        'listgen_temp_1_delta_blacklist_50_74',
                        'listgen_temp_1_delta_blacklist_75_99',
                        'listgen_temp_1_delta_exceptions_lists',
                        'listgen_temp_1_delta_exceptions_lists_operator1',
                        'listgen_temp_1_delta_exceptions_lists_operator1_0_24',
                        'listgen_temp_1_delta_exceptions_lists_operator1_25_49',
                        'listgen_temp_1_delta_exceptions_lists_operator1_50_74',
                        'listgen_temp_1_delta_exceptions_lists_operator1_75_99',
                        'listgen_temp_1_delta_exceptions_lists_operator2',
                        'listgen_temp_1_delta_exceptions_lists_operator2_0_24',
                        'listgen_temp_1_delta_exceptions_lists_operator2_25_49',
                        'listgen_temp_1_delta_exceptions_lists_operator2_50_74',
                        'listgen_temp_1_delta_exceptions_lists_operator2_75_99',
                        'listgen_temp_1_delta_exceptions_lists_operator3',
                        'listgen_temp_1_delta_exceptions_lists_operator3_0_24',
                        'listgen_temp_1_delta_exceptions_lists_operator3_25_49',
                        'listgen_temp_1_delta_exceptions_lists_operator3_50_74',
                        'listgen_temp_1_delta_exceptions_lists_operator3_75_99',
                        'listgen_temp_1_delta_exceptions_lists_operator4',
                        'listgen_temp_1_delta_exceptions_lists_operator4_0_24',
                        'listgen_temp_1_delta_exceptions_lists_operator4_25_49',
                        'listgen_temp_1_delta_exceptions_lists_operator4_50_74',
                        'listgen_temp_1_delta_exceptions_lists_operator4_75_99',
                        'listgen_temp_1_delta_notifications_lists',
                        'listgen_temp_1_delta_notifications_lists_operator1',
                        'listgen_temp_1_delta_notifications_lists_operator1_0_24',
                        'listgen_temp_1_delta_notifications_lists_operator1_25_49',
                        'listgen_temp_1_delta_notifications_lists_operator1_50_74',
                        'listgen_temp_1_delta_notifications_lists_operator1_75_99',
                        'listgen_temp_1_delta_notifications_lists_operator2',
                        'listgen_temp_1_delta_notifications_lists_operator2_0_24',
                        'listgen_temp_1_delta_notifications_lists_operator2_25_49',
                        'listgen_temp_1_delta_notifications_lists_operator2_50_74',
                        'listgen_temp_1_delta_notifications_lists_operator2_75_99',
                        'listgen_temp_1_delta_notifications_lists_operator3',
                        'listgen_temp_1_delta_notifications_lists_operator3_0_24',
                        'listgen_temp_1_delta_notifications_lists_operator3_25_49',
                        'listgen_temp_1_delta_notifications_lists_operator3_50_74',
                        'listgen_temp_1_delta_notifications_lists_operator3_75_99',
                        'listgen_temp_1_delta_notifications_lists_operator4',
                        'listgen_temp_1_delta_notifications_lists_operator4_0_24',
                        'listgen_temp_1_delta_notifications_lists_operator4_25_49',
                        'listgen_temp_1_delta_notifications_lists_operator4_50_74',
                        'listgen_temp_1_delta_notifications_lists_operator4_75_99',
                        'listgen_temp_1_new_blacklist',
                        'listgen_temp_1_new_blacklist_0_24',
                        'listgen_temp_1_new_blacklist_25_49',
                        'listgen_temp_1_new_blacklist_50_74',
                        'listgen_temp_1_new_blacklist_75_99',
                        'listgen_temp_1_new_blocking_conditions_table',
                        'listgen_temp_1_new_exceptions_lists',
                        'listgen_temp_1_new_exceptions_lists_operator1',
                        'listgen_temp_1_new_exceptions_lists_operator1_0_24',
                        'listgen_temp_1_new_exceptions_lists_operator1_25_49',
                        'listgen_temp_1_new_exceptions_lists_operator1_50_74',
                        'listgen_temp_1_new_exceptions_lists_operator1_75_99',
                        'listgen_temp_1_new_exceptions_lists_operator2',
                        'listgen_temp_1_new_exceptions_lists_operator2_0_24',
                        'listgen_temp_1_new_exceptions_lists_operator2_25_49',
                        'listgen_temp_1_new_exceptions_lists_operator2_50_74',
                        'listgen_temp_1_new_exceptions_lists_operator2_75_99',
                        'listgen_temp_1_new_exceptions_lists_operator3',
                        'listgen_temp_1_new_exceptions_lists_operator3_0_24',
                        'listgen_temp_1_new_exceptions_lists_operator3_25_49',
                        'listgen_temp_1_new_exceptions_lists_operator3_50_74',
                        'listgen_temp_1_new_exceptions_lists_operator3_75_99',
                        'listgen_temp_1_new_exceptions_lists_operator4',
                        'listgen_temp_1_new_exceptions_lists_operator4_0_24',
                        'listgen_temp_1_new_exceptions_lists_operator4_25_49',
                        'listgen_temp_1_new_exceptions_lists_operator4_50_74',
                        'listgen_temp_1_new_exceptions_lists_operator4_75_99',
                        'listgen_temp_1_new_mcc_mnc_table',
                        'listgen_temp_1_new_notifications_imeis',
                        'listgen_temp_1_new_notifications_imeis_0_24',
                        'listgen_temp_1_new_notifications_imeis_25_49',
                        'listgen_temp_1_new_notifications_imeis_50_74',
                        'listgen_temp_1_new_notifications_imeis_75_99',
                        'listgen_temp_1_new_notifications_lists',
                        'listgen_temp_1_new_notifications_lists_operator1',
                        'listgen_temp_1_new_notifications_lists_operator1_0_24',
                        'listgen_temp_1_new_notifications_lists_operator1_25_49',
                        'listgen_temp_1_new_notifications_lists_operator1_50_74',
                        'listgen_temp_1_new_notifications_lists_operator1_75_99',
                        'listgen_temp_1_new_notifications_lists_operator2',
                        'listgen_temp_1_new_notifications_lists_operator2_0_24',
                        'listgen_temp_1_new_notifications_lists_operator2_25_49',
                        'listgen_temp_1_new_notifications_lists_operator2_50_74',
                        'listgen_temp_1_new_notifications_lists_operator2_75_99',
                        'listgen_temp_1_new_notifications_lists_operator3',
                        'listgen_temp_1_new_notifications_lists_operator3_0_24',
                        'listgen_temp_1_new_notifications_lists_operator3_25_49',
                        'listgen_temp_1_new_notifications_lists_operator3_50_74',
                        'listgen_temp_1_new_notifications_lists_operator3_75_99',
                        'listgen_temp_1_new_notifications_lists_operator4',
                        'listgen_temp_1_new_notifications_lists_operator4_0_24',
                        'listgen_temp_1_new_notifications_lists_operator4_25_49',
                        'listgen_temp_1_new_notifications_lists_operator4_50_74',
                        'listgen_temp_1_new_notifications_lists_operator4_75_99',
                        'listgen_temp_1_new_notifications_triplets',
                        'listgen_temp_1_new_notifications_triplets_0_24',
                        'listgen_temp_1_new_notifications_triplets_25_49',
                        'listgen_temp_1_new_notifications_triplets_50_74',
                        'listgen_temp_1_new_notifications_triplets_75_99',
                        'listgen_temp_1_new_pairings_imei_imsis',
                        'listgen_temp_1_new_pairings_imei_imsis_0_24',
                        'listgen_temp_1_new_pairings_imei_imsis_25_49',
                        'listgen_temp_1_new_pairings_imei_imsis_50_74',
                        'listgen_temp_1_new_pairings_imei_imsis_75_99',
                        'listgen_temp_1_old_blacklist',
                        'listgen_temp_1_old_blacklist_0_24',
                        'listgen_temp_1_old_blacklist_25_49',
                        'listgen_temp_1_old_blacklist_50_74',
                        'listgen_temp_1_old_blacklist_75_99',
                        'listgen_temp_1_old_exceptions_lists',
                        'listgen_temp_1_old_exceptions_lists_operator1',
                        'listgen_temp_1_old_exceptions_lists_operator1_0_24',
                        'listgen_temp_1_old_exceptions_lists_operator1_25_49',
                        'listgen_temp_1_old_exceptions_lists_operator1_50_74',
                        'listgen_temp_1_old_exceptions_lists_operator1_75_99',
                        'listgen_temp_1_old_exceptions_lists_operator2',
                        'listgen_temp_1_old_exceptions_lists_operator2_0_24',
                        'listgen_temp_1_old_exceptions_lists_operator2_25_49',
                        'listgen_temp_1_old_exceptions_lists_operator2_50_74',
                        'listgen_temp_1_old_exceptions_lists_operator2_75_99',
                        'listgen_temp_1_old_exceptions_lists_operator3',
                        'listgen_temp_1_old_exceptions_lists_operator3_0_24',
                        'listgen_temp_1_old_exceptions_lists_operator3_25_49',
                        'listgen_temp_1_old_exceptions_lists_operator3_50_74',
                        'listgen_temp_1_old_exceptions_lists_operator3_75_99',
                        'listgen_temp_1_old_exceptions_lists_operator4',
                        'listgen_temp_1_old_exceptions_lists_operator4_0_24',
                        'listgen_temp_1_old_exceptions_lists_operator4_25_49',
                        'listgen_temp_1_old_exceptions_lists_operator4_50_74',
                        'listgen_temp_1_old_exceptions_lists_operator4_75_99',
                        'listgen_temp_1_old_notifications_lists',
                        'listgen_temp_1_old_notifications_lists_operator1',
                        'listgen_temp_1_old_notifications_lists_operator1_0_24',
                        'listgen_temp_1_old_notifications_lists_operator1_25_49',
                        'listgen_temp_1_old_notifications_lists_operator1_50_74',
                        'listgen_temp_1_old_notifications_lists_operator1_75_99',
                        'listgen_temp_1_old_notifications_lists_operator2',
                        'listgen_temp_1_old_notifications_lists_operator2_0_24',
                        'listgen_temp_1_old_notifications_lists_operator2_25_49',
                        'listgen_temp_1_old_notifications_lists_operator2_50_74',
                        'listgen_temp_1_old_notifications_lists_operator2_75_99',
                        'listgen_temp_1_old_notifications_lists_operator3',
                        'listgen_temp_1_old_notifications_lists_operator3_0_24',
                        'listgen_temp_1_old_notifications_lists_operator3_25_49',
                        'listgen_temp_1_old_notifications_lists_operator3_50_74',
                        'listgen_temp_1_old_notifications_lists_operator3_75_99',
                        'listgen_temp_1_old_notifications_lists_operator4',
                        'listgen_temp_1_old_notifications_lists_operator4_0_24',
                        'listgen_temp_1_old_notifications_lists_operator4_25_49',
                        'listgen_temp_1_old_notifications_lists_operator4_50_74',
                        'listgen_temp_1_old_notifications_lists_operator4_75_99']
        expected_res = {x.format(run_id) for x in expected_res}
        assert set(res) == expected_res
        _cli_listgen_helper(db_conn, tmpdir, 'run_2', mocked_config)
        cursor.execute("""SELECT TABLE_NAME
                            FROM information_schema.tables
                           WHERE TABLE_NAME LIKE 'listgen_temp_%'""")
        res = [x.table_name for x in cursor.fetchall()]
        assert len(res) == 158


@pytest.mark.parametrize('operator_data_importer, registration_list_importer, stolen_list_importer',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161203,86222222222226,111018001111111,223338000000\n'
                                     '20161203,35111111111110,111015111111111,223355000000\n'
                                     '20161203,35900000000000,310035111111111,743614000000',
                             operator='operator1',
                             cc=['22', '74'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}, {'mcc': '310', 'mnc': '03'}],
                             extract=False),
                           RegistrationListParams(content='approved_imei,make,model,status,model_number,'
                                                          'brand_name,device_type,radio_interface,device_id\n'
                                                          '35900000000000,   ,   ,whitelist,,,,,23422'),
                           StolenListParams(content='IMEI,reporting_date,status\n'
                                                    '35111111111110,20160930,blacklist\n'
                                                    '35900000000000,20160930,blacklist\n'))],
                         indirect=True)
def test_amnesty_enabled_listgen(postgres, operator_data_importer, stolen_list_importer, registration_list_importer,
                                 mocked_config, monkeypatch, logger, db_conn, tmpdir):
    """Test notification list and blacklist are correctly generated  when amnesty is enabled."""
    import_data(operator_data_importer, 'operator_data', 3, db_conn, logger)
    import_data(stolen_list_importer, 'stolen_list', 2, db_conn, logger)
    import_data(registration_list_importer, 'registration_list', 1, db_conn, logger)

    # Set amnesty config parameters
    monkeypatch.setattr(mocked_config.amnesty_config, 'amnesty_enabled', True)
    monkeypatch.setattr(mocked_config.amnesty_config, 'evaluation_period_end_date', datetime.date(2017, 1, 1))
    monkeypatch.setattr(mocked_config.amnesty_config, 'amnesty_period_end_date', datetime.date(2017, 2, 1))

    cond_list = [{
        'label': 'not_on_registration_list',
        'reason': 'not_registered',
        'max_allowed_matching_ratio': 1.0,
        'grace_period_days': 10,
        'blocking': True,
        'amnesty_eligible': True,
        'dimensions': [{'module': 'not_on_registration_list'}]},
        {
        'label': 'local_stolen',
        'reason': 'stolen',
        'max_allowed_matching_ratio': 1.0,
        'grace_period_days': 20,
        'blocking': True,
        'amnesty_eligible': False,
        'dimensions': [{'module': 'stolen_list'}]}]

    # Step 1: Test listgen works fine when in eval period.
    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                               curr_date='20170101')

    _, output_dir = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20170101')

    # Verify non-amnesty triplets are in notification list when in eval period.
    # IMEI '86222222222226' is eligible for amnesty and should not be in notification list.
    rows = _read_rows_from_file('notifications_operator1.csv', tmpdir, output_dir=output_dir)
    rows = [tuple(map(str, i.split(',')))[:6] for i in rows]
    assert len(rows) == 3
    # Verify the correct block_date is stored for IMEI meeting only non-amnesty condition
    assert ('35900000000000', '310035111111111', '743614000000', '20170121', 'stolen', 'False\n') in rows
    assert ('35111111111110', '111015111111111', '223355000000', '20170121', 'stolen', 'False\n') in rows

    # Verify delta notification list is generated correctly.
    rows = _read_rows_from_file('notifications_operator1_delta', tmpdir, output_dir=output_dir)
    rows = [tuple(map(str, i.split(',')))[:7] for i in rows]
    assert len(rows) == 3
    assert ('35900000000000', '310035111111111', '743614000000', '20170121', 'stolen', 'False', 'new\n') in rows
    assert ('35111111111110', '111015111111111', '223355000000', '20170121', 'stolen', 'False', 'new\n') in rows

    # Verify blacklist is empty.
    rows = _read_rows_from_file('blacklist.csv', tmpdir, output_dir=output_dir)
    assert len(rows) == 1

    # Step 2: Test listgen works fine when in amnesty period.
    invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch, db_conn=db_conn,
                                               curr_date='20170102')

    _, output_dir = _cli_listgen_helper(db_conn, tmpdir, 'run_2', mocked_config, date='20170102')

    # Verify amnesty triplets are in notification list also when in amnesty period.
    # IMEI '86222222222226' is eligible for amnesty and should be in notification list.
    rows = _read_rows_from_file('notifications_operator1.csv', tmpdir, output_dir=output_dir)
    rows = [tuple(map(str, i.split(',')))[:6] for i in rows]
    assert len(rows) == 4
    assert ('35900000000000', '310035111111111', '743614000000', '20170121', 'stolen', 'False\n') in rows
    assert ('35111111111110', '111015111111111', '223355000000', '20170121',
            'not_registered|stolen', 'False\n') in rows
    assert ('86222222222226', '111018001111111', '223338000000', '20170201', 'not_registered', 'True\n') in rows

    # Verify delta notification list is generated correctly.
    # Verify changed notification is generated as new reason is added for the IMEI.
    rows = _read_rows_from_file('notifications_operator1_delta', tmpdir, output_dir=output_dir)
    rows = [tuple(map(str, i.split(',')))[:7] for i in rows]
    assert len(rows) == 3
    assert ('86222222222226', '111018001111111', '223338000000', '20170201', 'not_registered', 'True', 'new\n') in rows
    assert ('35111111111110', '111015111111111', '223355000000', '20170121',
            'not_registered|stolen', 'False', 'changed\n') in rows

    # Verify blacklist is empty.
    rows = _read_rows_from_file('blacklist.csv', tmpdir, output_dir=output_dir)
    assert len(rows) == 1

    # Step 3: Verify non-amnesty IMEIs are added to blacklist after they are past the block_date.
    _, output_dir = _cli_listgen_helper(db_conn, tmpdir, 'run_3', mocked_config, date='20170122')

    # Verify amnesty triplets are in notification list when in amnesty period.
    rows = _read_rows_from_file('notifications_operator1.csv', tmpdir, output_dir=output_dir)
    rows = [tuple(map(str, i.split(',')))[:6] for i in rows]
    assert len(rows) == 2
    assert ('86222222222226', '111018001111111', '223338000000', '20170201', 'not_registered', 'True\n') in rows

    # Verify delta notification list is generated correctly.
    rows = _read_rows_from_file('notifications_operator1_delta', tmpdir, output_dir=output_dir)
    rows = [tuple(map(str, i.split(',')))[:7] for i in rows]
    assert len(rows) == 3
    assert ('35900000000000', '310035111111111', '743614000000', '20170121',
            'stolen', 'False', 'blacklisted\n') in rows
    assert ('35111111111110', '111015111111111', '223355000000', '20170121',
            'not_registered|stolen', 'False', 'blacklisted\n') in rows

    # Verify blacklist is non-empty.
    # IMEIs '35900000000000' and '35111111111110' are stolen and past block date so they should be in blacklist.
    rows = _read_rows_from_file('blacklist.csv', tmpdir, output_dir=output_dir)
    assert len(rows) == 3
    rows = [tuple(map(str, i.split(',')))[:3] for i in rows]
    assert ('35900000000000', '20170121', 'stolen\n') in rows
    assert ('35111111111110', '20170121', 'stolen\n') in rows

    # Verify delta blacklist is generated correctly.
    rows = _read_rows_from_file('blacklist_delta', tmpdir, output_dir=output_dir)
    rows = [tuple(map(str, i.split(',')))[:4] for i in rows]
    assert len(rows) == 3
    assert ('35900000000000', '20170121', 'stolen', 'blocked\n') in rows
    assert ('35111111111110', '20170121', 'stolen', 'blocked\n') in rows

    # Step 4: Verify amnesty IMEIs are added to blacklist after amnesty period has ended.
    _, output_dir = _cli_listgen_helper(db_conn, tmpdir, 'run_4', mocked_config, date='20170202')

    # Verify notification list is empty
    rows = _read_rows_from_file('notifications_operator1.csv', tmpdir, output_dir=output_dir)
    rows = [tuple(map(str, i.split(',')))[:6] for i in rows]
    assert len(rows) == 1

    # Verify delta notification list is generated correctly.
    rows = _read_rows_from_file('notifications_operator1_delta', tmpdir, output_dir=output_dir)
    rows = [tuple(map(str, i.split(',')))[:7] for i in rows]
    assert len(rows) == 2
    assert ('86222222222226', '111018001111111', '223338000000', '20170201', 'not_registered',
            'True', 'blacklisted\n') in rows

    # Verify blacklist is non-empty.
    # Amnesty IMEI '86222222222226' is now past block_date and should be in blacklist too.
    rows = _read_rows_from_file('blacklist.csv', tmpdir, output_dir=output_dir)
    assert len(rows) == 4
    rows = [tuple(map(str, i.split(',')))[:3] for i in rows]
    assert ('35900000000000', '20170121', 'stolen\n') in rows
    assert ('35111111111110', '20170121', 'not_registered|stolen\n') in rows
    assert ('86222222222226', '20170201', 'not_registered\n') in rows

    # Verify delta blacklist is generated correctly.
    # Note changed notification is generated as IMEI '35111111111110' was also eligible for amnesty
    # and is now past amnesty_end_date, so new reason gets added for it.
    rows = _read_rows_from_file('blacklist_delta', tmpdir, output_dir=output_dir)
    rows = [tuple(map(str, i.split(',')))[:4] for i in rows]
    assert len(rows) == 3
    assert ('86222222222226', '20170201', 'not_registered', 'blocked\n') in rows
    assert ('35111111111110', '20170121', 'not_registered|stolen', 'changed\n') in rows


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(content='imei,imsi,msisdn\n'
                                                 '345267865342911,111093876523467,999999999999931\n'
                                                 '345267865342922,111093876523468,999999999999932\n'
                                                 '345267865342933,111093876523469,999999999999933\n')],
                         indirect=True)
@pytest.mark.parametrize('barred_list_importer',
                         [BarredListParams(content='imei\n'
                                                   '345267865342922')],
                         indirect=True)
def test_exception_list_barred_imeis_restriction(postgres, pairing_list_importer, barred_list_importer,
                                                 mocked_config, logger, db_conn, tmpdir, monkeypatch):
    """Verify that the barred_imeis restriction functionality works correctly on exceptions lists."""
    pairing_list_importer.import_data()
    barred_list_importer.import_data()

    # enable the check to allow barred imeis in exception list which are also in pairing list
    options_list = []
    monkeypatch.setattr(mocked_config.listgen_config, 'include_barred_imeis', True)
    output_dir = str(tmpdir)
    options_list.append(output_dir)
    runner = CliRunner()
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # find the listgen directory and verify the content
    fn = find_subdirectory_in_dir('listgen__*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    fl = find_file_in_dir('*exceptions_operator1.zip', dir_path)

    # extract the zip file found in directory and verify
    expected_list = [['imei', 'imsi', 'msisdn'],
                     ['34526786534291', '111093876523467', '999999999999931'],
                     ['34526786534292', '111093876523468', '999999999999932'],
                     ['34526786534293', '111093876523469', '999999999999933']]

    with zipfile.ZipFile(fl) as zipf_ref:
        ext_path = tmpdir.mkdir('exceptions_lists')
        zipf_ref.extractall(ext_path)
        excp_file = find_file_in_dir('*exceptions_operator1.csv', ext_path)
        with open(excp_file, 'r') as exp_file:
            reader = csv.reader(exp_file)
            rows = list(reader)
            assert all([x in rows for x in expected_list])

    # Now turn off barred imeis in exceptions lists
    options_list = []
    monkeypatch.setattr(mocked_config.listgen_config, 'include_barred_imeis', False)
    output_dir = str(tmpdir.mkdir('restricted_lists'))
    options_list.append(output_dir)
    runner = CliRunner()
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # find the listgen directory and verify the content
    fn = find_subdirectory_in_dir('listgen__*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    fl = find_file_in_dir('*exceptions_operator1.zip', dir_path)

    # extract the zip file found in directory and verify
    expected_list = [['imei', 'imsi', 'msisdn'],
                     ['34526786534291', '111093876523467', '999999999999931'],
                     ['34526786534293', '111093876523469', '999999999999933']]

    with zipfile.ZipFile(fl) as zipf_ref:
        ext_path = tmpdir.mkdir('exceptions_lists_restricted')
        zipf_ref.extractall(ext_path)
        excp_file = find_file_in_dir('*exceptions_operator1.csv', ext_path)
        with open(excp_file, 'r') as exp_file:
            reader = csv.reader(exp_file)
            rows = list(reader)
            assert all([x in rows for x in expected_list])


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(content='imei,imsi,msisdn\n'
                                                 '345267865342941,111093876523467,999999999999921\n'
                                                 '345267865342952,111093876523468,999999999999922\n'
                                                 '345267865342963,111093876523469,999999999999923\n')],
                         indirect=True)
@pytest.mark.parametrize('barred_tac_list_importer',
                         [BarredTacListParams(content='tac\n'
                                                      '34526786')],
                         indirect=True)
def test_exception_list_barred_tac_restriction(postgres, pairing_list_importer, barred_tac_list_importer,
                                               mocked_config, logger, db_conn, tmpdir, monkeypatch):
    """Verify that the barred restriction on exceptions lists also appears on barred tacs."""
    pairing_list_importer.import_data()
    barred_tac_list_importer.import_data()

    # enable the check to allow barred imeis in exception list which are also in pairing list
    options_list = []
    monkeypatch.setattr(mocked_config.listgen_config, 'include_barred_imeis', True)
    output_dir = str(tmpdir)
    options_list.append(output_dir)
    runner = CliRunner()
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # find the listgen directory and verify the content
    fn = find_subdirectory_in_dir('listgen__*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    fl = find_file_in_dir('*exceptions_operator1.zip', dir_path)

    # extract the zip file found in directory and verify
    expected_list = [['imei', 'imsi', 'msisdn'],
                     ['34526786534294', '111093876523467', '999999999999921'],
                     ['34526786534295', '111093876523468', '999999999999922'],
                     ['34526786534296', '111093876523469', '999999999999923']]

    with zipfile.ZipFile(fl) as zipf_ref:
        ext_path = tmpdir.mkdir('exceptions_lists_tacs')
        zipf_ref.extractall(ext_path)
        excp_file = find_file_in_dir('*exceptions_operator1.csv', ext_path)
        with open(excp_file, 'r') as exp_file:
            reader = csv.reader(exp_file)
            rows = list(reader)
            print(rows)
            assert all([x in rows for x in expected_list])

    # Now turn off barred imeis in exceptions lists
    options_list = []
    monkeypatch.setattr(mocked_config.listgen_config, 'include_barred_imeis', False)
    output_dir = str(tmpdir.mkdir('restricted_lists_tacs'))
    options_list.append(output_dir)
    runner = CliRunner()
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # find the listgen directory and verify the content
    fn = find_subdirectory_in_dir('listgen__*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    fl = find_file_in_dir('*exceptions_operator1.zip', dir_path)

    # extract the zip file found in directory and verify
    expected_list = [['imei', 'imsi', 'msisdn']]

    with zipfile.ZipFile(fl) as zipf_ref:
        ext_path = tmpdir.mkdir('exceptions_lists_restricted')
        zipf_ref.extractall(ext_path)
        excp_file = find_file_in_dir('*exceptions_operator1.csv', ext_path)
        with open(excp_file, 'r') as exp_file:
            reader = csv.reader(exp_file)
            rows = list(reader)
            assert all([x in rows for x in expected_list])


@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state.csv'],
                         indirect=True)
def test_per_condition_blacklist_gen(postgres, classification_data, db_conn, tmpdir, mocked_config):
    """Test that dirbs-listgen generates blacklist only for the specified condition."""
    runner = CliRunner()
    output_dir = str(tmpdir.mkdir('per_cond_run'))
    result = runner.invoke(dirbs_classify_cli, ['--curr-date=20170101'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # find blacklist for all conditions in clasification_state
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_1', mocked_config, date='20170101')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    file_list = os.listdir(dir_path)
    assert fnmatch.filter(file_list, '*blacklist.csv')
    fn = find_file_in_dir('*blacklist.csv', dir_path)
    with open(fn, 'r') as file:
        rows = [tuple(map(str, i.split(',')))[:1] for i in file]
        assert ('35000000000000',) in rows
        assert ('35900000000000',) in rows
        assert ('86222222222226',) in rows

    # find blacklist for only specified condition
    _, output_dir, = _cli_listgen_helper(db_conn, tmpdir, 'run_2', mocked_config, date='20170101',
                                         conditions='gsma_not_found')
    dir_path = find_subdirectory_in_dir('listgen*', output_dir)
    file_list = os.listdir(dir_path)
    assert fnmatch.filter(file_list, '*blacklist.csv')
    fn = find_file_in_dir('*blacklist.csv', dir_path)
    with open(fn, 'r') as file:
        rows = [tuple(map(str, i.split(',')))[:1] for i in file]
        assert ('35000000000000',) not in rows
        assert ('35900000000000',) not in rows
        assert ('86222222222226',) not in rows


def test_sanity_checks_operators(per_test_postgres, mocked_config, tmpdir, logger, monkeypatch):
    """Verify that the sanity checks are performed on operators."""
    options_list = []
    options_list.extend(['--curr-date', '20161130'])
    options_list.extend(['--disable-sanity-checks'])  # we disable sanity checks first to establish base for next one
    output_dir = str(tmpdir.mkdir('sanity_operators'))
    options_list.append(output_dir)
    runner = CliRunner()
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # monkey patch operators config
    options_list = []
    options_list.extend(['--curr-date', '20161130'])
    options_list.append(output_dir)
    operator_conf = [{
        'id': 'op_listgen',
        'name': 'First Operator',
        'mcc_mnc_pairs': [{
            'mcc': '112',
            'mnc': '09'
        }]
    }]

    operator_conf = from_op_dict_list_to_op_list(operator_conf)
    monkeypatch.setattr(mocked_config.region_config, 'operators', operator_conf)
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1


def test_sanity_checks_amnesty(per_test_postgres, mocked_config, tmpdir, logger, monkeypatch):
    """Verify that sanity checks are performed on amnesty configs."""
    options_list = []
    options_list.extend(['--curr-date', '20161130'])
    options_list.extend(['--disable-sanity-checks'])  # we disable sanity checks first to establish base for next one
    output_dir = str(tmpdir.mkdir('sanity_amnesty'))
    options_list.append(output_dir)
    runner = CliRunner()
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # monkey patch operators config
    options_list = []
    options_list.extend(['--curr-date', '20161130'])
    options_list.append(output_dir)
    amnesty_config = {
        'amnesty_enabled': False,
        'evaluation_period_end_date': 19400202,
        'amnesty_period_end_date': 19400302
    }

    amnesty_config = from_amnesty_dict_to_amnesty_conf(amnesty_config)
    monkeypatch.setattr(mocked_config, 'amnesty_config', amnesty_config)
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1


def test_sanity_checks_conditions(per_test_postgres, mocked_config, tmpdir, logger, monkeypatch):
    """Verify that the sanity checks are performed on blocking conditions."""
    options_list = []
    options_list.extend(['--curr-date', '20161130'])
    options_list.extend(['--disable-sanity-checks'])  # we disable sanity checks first to establish base for next one
    output_dir = str(tmpdir.mkdir('sanity_conditions'))
    options_list.append(output_dir)
    runner = CliRunner()
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # monkey patch operators config
    options_list = []
    options_list.extend(['--curr-date', '20161130'])
    options_list.append(output_dir)
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
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1


def test_sanity_checks_loopback_days(per_test_postgres, mocked_config, tmpdir, logger, monkeypatch):
    """Verify that sanity checks are performed on loopback days."""
    options_list = []
    options_list.extend(['--curr-date', '20161130'])
    options_list.extend(['--disable-sanity-checks'])  # we disable sanity checks first to establish base for next one
    output_dir = str(tmpdir.mkdir('sanity_conditions'))
    options_list.append(output_dir)
    runner = CliRunner()
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # monkey patch operators config
    options_list = []
    options_list.extend(['--curr-date', '20161130'])
    options_list.append(output_dir)
    monkeypatch.setattr(mocked_config.listgen_config, 'lookback_days', 90)
    result = runner.invoke(dirbs_listgen_cli, options_list, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 1
