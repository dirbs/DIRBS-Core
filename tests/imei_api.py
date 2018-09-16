"""
IMEI API unit tests.

Copyright (c) 2018 Qualcomm Technologies, Inc.

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted (subject to the limitations in the disclaimer below) provided that the
following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this list of conditions
   and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions
   and the following disclaimer in the documentation and/or other materials provided with the distribution.
 * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse
   or promote products derived from this software without specific prior written permission.

NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED
BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import json

from flask import url_for
import pytest

from dirbs.importer.operator_data_importer import OperatorDataImporter
from _fixtures import *    # noqa: F403, F401
from _importer_params import GSMADataParams, OperatorDataParams, RegistrationListParams, PairListParams
from _helpers import get_importer


def check_in_registration_list_helper(imei_list, expect_to_find_in_reg, api_version, flask_app):
    """Helper function to make a request and check in_registration_list value in the response."""
    for i in imei_list:
        rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=i))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['realtime_checks']['in_registration_list'] is expect_to_find_in_reg


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list.csv')],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='sample_gsma_import_list_anonymized.txt',
                                         extract=False)],
                         indirect=True)
def test_check_in_registration_list(flask_app, registration_list_importer, gsma_tac_db_importer,
                                    api_version, monkeypatch, mocked_config):
    """Test Depot not known yet.

    Verify that IMEI API response contains a Real-time check for IMEI in registration list.
    """
    # APPROVED_IMEI
    # 10000000000000
    # 10000000000001
    # 10000000000002 ....
    # Verify that 10000000000000 (14 digits) in reg_list
    # Verify that 1000000000000200 (16 digits) in reg_list
    imei_list = ['10000000000000', '10000000000001', '1000000000000200']
    check_in_registration_list_helper(imei_list, False, api_version, flask_app)
    registration_list_importer.import_data()
    check_in_registration_list_helper(imei_list, True, api_version, flask_app)
    imei_list = ['20000000000000']
    check_in_registration_list_helper(imei_list, False, api_version, flask_app)

    # Verify API returns correct result for exempted device types
    monkeypatch.setattr(mocked_config.region_config, 'exempted_device_types', ['Vehicle', 'Dongle'])
    gsma_tac_db_importer.import_data()
    # Following IMEIs are not in registration_list but belong to exempted device types
    imei_list = ['012344022302145', '012344035454564']
    check_in_registration_list_helper(imei_list, True, api_version, flask_app)
    # Following IMEIs are not in registration_list and do not belong to exempted device types
    imei_list = ['012344014741025']
    check_in_registration_list_helper(imei_list, False, api_version, flask_app)


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(content='approved_imei,make,model,status\n'
                                                         '10000000000000,,,whitelist\n'
                                                         '10000000000001,,,whitelist\n'
                                                         '10000000000002,,,something_else\n')],
                         indirect=True)
def test_registration_list_status_filter(flask_app, registration_list_importer, api_version):
    """Test Depot not known yet.

    Verify IMEI API 'in_registration_list' realtime check does not filter for non-whitelisted statuses.
    """
    # 10000000000002 is in registration_list but status is not whitelist and is not filtered
    registration_list_importer.import_data()
    imei_list = ['10000000000000', '10000000000001']
    check_in_registration_list_helper(imei_list, True, api_version, flask_app)
    check_in_registration_list_helper(['10000000000002'], False, api_version, flask_app)


def check_output_data(flask_app, i, api_version, include_seen_with_bool=True,
                      class_block_gsma_bool=False, class_block_dupl_bool=False,
                      real_invalid_imei_bool=False, real_gsma_bool=False, class_block_stolen_bool=False,
                      class_informative_malf_bool=False, include_paired_with_bool=True):
    """Helper function used to DRY out IMEI API tests."""
    rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=i,
                               include_seen_with=include_seen_with_bool, include_paired_with=include_paired_with_bool))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    expected_norm = i if len(i) < 14 else i[:14]
    assert data['imei_norm'] == expected_norm
    assert data['classification_state']['blocking_conditions']['gsma_not_found'] is class_block_gsma_bool
    assert data['classification_state']['blocking_conditions']['duplicate_mk1'] is class_block_dupl_bool
    assert data['realtime_checks']['invalid_imei'] is real_invalid_imei_bool
    assert data['classification_state']['blocking_conditions']['local_stolen'] is class_block_stolen_bool
    assert data['classification_state']['informative_conditions']['malformed_imei'] is class_informative_malf_bool
    assert data['realtime_checks']['gsma_not_found'] is real_gsma_bool
    assert data['realtime_checks']['in_registration_list'] is False
    return data


def test_imei_too_long(flask_app, api_version):
    """Test Depot ID 96792/9.

    Verify that IMEI API should validate that
    a supplied IMEI is less than or equal to 16 chars and
    return an HTTP 400 error code if not.
    """
    for i in ['1', '123456', '1234567890ABCDEF']:
        rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=i))
        assert rv.status_code == 200
    for i in ['1234567890ABCDEFG', '1234567890ABCDEFG3']:
        rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=i))
        assert rv.status_code == 400
        assert b'Bad IMEI format (too long)' in rv.data


def test_empty_imei(flask_app, api_version):
    """Test Depot ID not known yet.

    Verify that IMEI API should return a 404 status for a zero-length IMEI.
    """
    """ IMEI API should return a 404 status for a zero-length IMEI """
    rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=''))
    assert rv.status_code == 404


def test_invalid_imei_realtime_checks(flask_app, api_version):
    """Test Depot ID 96548/5.

    Verify IMEI API should calculate some
    realtime checks on an IMEI so that the API returns useful
    info even if an IMEI has never been seen and classfied.
    """
    check_output_data(flask_app, '123456', api_version, real_gsma_bool=True, real_invalid_imei_bool=True)
    check_output_data(flask_app, '3884773337002633', api_version, real_gsma_bool=True)


def test_imei_normalisation(flask_app, api_version):
    """Test Depot ID 96549/6.

    Verify IMEI API should normalise an input IMEI.
    """
    for i in ['0117220037002633', '1234567890123456', '123456789012345']:
        rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=i))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        expected_norm = i if len(i) < 14 else i[:14]
        assert data['imei_norm'] == expected_norm


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='testData1-gsmatac_operator1_operator4_anonymized.txt',
                                         extract=False)],
                         indirect=True)
@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state.csv'],
                         indirect=True)
@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                                             extract=False,
                                             perform_unclean_checks=False,
                                             perform_region_checks=False,
                                             perform_home_network_check=False)],
                         indirect=True)
def test_unobserved_valid_imeis(flask_app, gsma_tac_db_importer, operator_data_importer, classification_data,
                                db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd, api_version):
    """Test Depot ID 96544/1.

    Verify the IMEI API supports HTTP GET and responds with correct
    HTTP Status codes and response body.
    """
    gsma_tac_db_importer.import_data()
    operator_data_importer.import_data()
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                          extract=False,
                          operator='operator4',
                          perform_unclean_checks=False,
                          perform_region_checks=False,
                          perform_home_network_check=False)) as new_imp:
        new_imp.import_data()

    rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei='3884773337002633'))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['imei_norm'] == '38847733370026'
    for k, v in data['classification_state']['blocking_conditions'].items():
        assert v is False
    for k, v in data['classification_state']['informative_conditions'].items():
        assert v is False
    assert data['realtime_checks']['invalid_imei'] is False


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                                             extract=False,
                                             perform_unclean_checks=False,
                                             perform_region_checks=False,
                                             perform_home_network_check=False)],
                         indirect=True)
@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state.csv'],
                         indirect=True)
def test_observed_imei(flask_app, operator_data_importer, classification_data,
                       db_conn, tmpdir, logger, api_version):
    """Test Depot ID 96550/7.

    Verify IMEI API should return IMSI-MSISDN pairings that a IMEI has been
    seen with within the data retention window.
    """
    operator_data_importer.import_data()
    data = check_output_data(flask_app, '3884773337002633', api_version, real_gsma_bool=True)
    assert data['seen_with'] == [{'imsi': '11104803062043', 'msisdn': '22300049781840'}]


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                                             extract=False,
                                             perform_unclean_checks=False,
                                             perform_region_checks=False,
                                             perform_home_network_check=False)],
                         indirect=True)
@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v1.csv'],
                         indirect=True)
def test_observed_imei_two(flask_app, operator_data_importer,
                           db_conn, tmpdir, logger, classification_data, api_version):
    """Test Depot ID 96551/8.

    Verify IMEI API should return the classification state for all configured conditions.
    """
    operator_data_importer.import_data()
    data = check_output_data(flask_app, '3884773337002633', api_version,
                             class_block_dupl_bool=True, real_gsma_bool=True)
    assert data['seen_with'] == [{'imsi': '11104803062043', 'msisdn': '22300049781840'}]


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='tac_api_gsma_db.txt')],
                         indirect=True)
@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state.csv'],
                         indirect=True)
def test_unobserved_imei_in_gsma(flask_app, gsma_tac_db_importer, classification_data, api_version):
    """Test Depot ID not known yet.

    Verify IMEI API should Test IMEI API return for IMEI 35567907123456.
    """
    gsma_tac_db_importer.import_data()
    for i in ['21154034123456', '21154034123456A', '2115403412345612']:
        rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=i, include_seen_with=True))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['imei_norm'] == '21154034123456'
        assert data['seen_with'] == []
        for k, v in data['classification_state']['blocking_conditions'].items():
            assert v is False
        for k, v in data['classification_state']['informative_conditions'].items():
            assert v is False
        assert data['realtime_checks']['invalid_imei'] is False
        # Sample GSMA data has this TAC in it
        assert data['realtime_checks']['gsma_not_found'] is False


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                                             extract=False,
                                             perform_unclean_checks=False,
                                             perform_region_checks=False,
                                             perform_home_network_check=False)],
                         indirect=True)
@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v1.csv'],
                         indirect=True)
def test_seen_with(flask_app, operator_data_importer,
                   db_conn, tmpdir, logger, classification_data, api_version):
    """Test Depot ID not known yet.

    Verify IMEI API respects the include_seen_with flag and only returns seen_with data iff include_seen_with is True.
    """
    operator_data_importer.import_data()
    data = check_output_data(flask_app, '3884773337002638', api_version,
                             class_block_dupl_bool=True, real_gsma_bool=True)
    assert data['seen_with'] == [{'imsi': '11104803062043', 'msisdn': '22300049781840'}]
    data = check_output_data(flask_app, '3884773337002638', api_version, include_seen_with_bool=False,
                             class_block_dupl_bool=True, real_gsma_bool=True)
    assert data.get('seen_with', None) is None


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(content='imei,imsi\n'
                                                 '38847733370026,111018001111111\n'
                                                 '38847733370026,111015113222222\n'
                                                 '38847733370020,111015113333333')],
                         indirect=True)
@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                                             extract=False,
                                             perform_unclean_checks=False,
                                             perform_region_checks=False,
                                             perform_home_network_check=False)],
                         indirect=True)
@pytest.mark.parametrize('classification_data',
                         ['classification_state/imei_api_class_state_v1.csv'],
                         indirect=True)
def test_paired_with(flask_app, operator_data_importer, pairing_list_importer,
                     db_conn, tmpdir, logger, classification_data, api_version):
    """Test Depot ID not known yet.

    Verify IMEI API respects the include_paired_with flag
    and only returns paired_with data iff include_paired_with is True.
    """
    pairing_list_importer.import_data()
    data = check_output_data(flask_app, '38847733370026', api_version, include_paired_with_bool=False,
                             class_block_dupl_bool=True, real_gsma_bool=True)
    assert data.get('paired_with', None) is None
    assert data['is_paired']
    data = check_output_data(flask_app, '35000000000000', api_version, include_paired_with_bool=False,
                             class_block_dupl_bool=True, real_gsma_bool=True)
    assert data.get('paired_with', None) is None
    assert not data['is_paired']
    data = check_output_data(flask_app, '38847733370026', api_version,
                             class_block_dupl_bool=True, real_gsma_bool=True)
    assert set(data['paired_with']) == {'111015113222222', '111018001111111'}
    assert data['is_paired']
    data = check_output_data(flask_app, '35000000000000', api_version,
                             class_block_dupl_bool=True, real_gsma_bool=True)
    assert data['paired_with'] == []
    assert not data['is_paired']


def test_put_not_allowed(flask_app, db_conn, tmpdir, logger, api_version):
    """Test Depot ID 96545/2.

    Verify the IMEI API does not support HTTP PUT and returns HTTP 405 METHOD NOT ALLOWED.
    """
    for i in ['3884773337002633']:
        rv = flask_app.put(url_for('{0}.imei_api'.format(api_version), imei=i))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data


def test_post_not_allowed(flask_app, db_conn, tmpdir, logger, api_version):
    """Test Depot ID 96545/2.

    Verify the IMEI API does not support HTTP POST and returns HTTP 405 METHOD NOT ALLOWED.
    """
    for i in ['3884773337002633']:
        rv = flask_app.post(url_for('{0}.imei_api'.format(api_version), imei=i))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data


def test_delete_not_allowed(flask_app, db_conn, tmpdir, logger, api_version):
    """Test Depot ID 96545/2.

    Verify the IMEI API does not support HTTP DELETE and returns HTTP 405 METHOD NOT ALLOWED.
    """
    for i in ['3884773337002633']:
        rv = flask_app.delete(url_for('{0}.imei_api'.format(api_version), imei=i))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data


def test_response_headers(flask_app, api_version):
    """Verify the security headers are set properly on returned response."""
    rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei='123456789012345'))
    assert rv.status_code == 200
    assert rv.headers.get('X-Frame-Options') == 'DENY'
    assert rv.headers.get('X-Content-Type-Options') == 'nosniff'


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161122,01376803870943,123456789012345,123456789012345\n'
                                     '20161122,64220297727231,123456789012345,123456789012345\n'
                                     '20161121,64220299727231,125456789012345,123456789012345\n'
                                     '20161121,64220498727231,123456789012345,123456789012345',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
def test_check_ever_observed_on_network(flask_app, operator_data_importer, api_version):
    """Test Depot not known yet.

    Verify that IMEI API response contains a Real-time check for IMEI was ever observed on the network.
    """
    # helper function to make a request and check ever_observed_on_network value in the response
    def check_in_network_imeis_table_helper(expect_to_find_in_network_imeis):
        for i in ['01376803870943', '64220297727231', '64220299727231', '64220498727231']:
            rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=i))
            assert rv.status_code == 200
            data = json.loads(rv.data.decode('utf-8'))
            assert data['realtime_checks']['ever_observed_on_network'] is expect_to_find_in_network_imeis

    check_in_network_imeis_table_helper(False)
    operator_data_importer.import_data()
    check_in_network_imeis_table_helper(True)
