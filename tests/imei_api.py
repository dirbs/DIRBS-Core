"""
IMEI API unit tests.

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

import json

from flask import url_for
import pytest

from dirbs.importer.operator_data_importer import OperatorDataImporter
from _fixtures import *  # noqa: F403, F401
from _importer_params import GSMADataParams, OperatorDataParams, RegistrationListParams, PairListParams, \
    StolenListParams
from _helpers import get_importer


def check_in_registration_list_helper(imei_list, expect_to_find_in_reg, api_version, flask_app):
    """Helper function to make a request and check in_registration_list value in the response."""
    if api_version == 'v1':
        for i in imei_list:
            rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=i))
            assert rv.status_code == 200
            data = json.loads(rv.data.decode('utf-8'))
            assert data['realtime_checks']['in_registration_list'] is expect_to_find_in_reg
    else:
        assert False  # Fail if passed api versions other than 1.0


def test_imei_get_api_responses(api_version, flask_app):
    """Test Depot not known yet.

    Verify IMEI API responses of api/v1/imei/<imei>, api/v2/imei/<imei>.
    """
    imei = '20000000000000'
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=imei))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert type(data) is dict
        assert type(data['realtime_checks']) is dict
        assert type(data['realtime_checks']['ever_observed_on_network']) is bool
        assert type(data['realtime_checks']['in_registration_list']) is bool
        assert type(data['realtime_checks']['invalid_imei']) is bool
        assert type(data['realtime_checks']['gsma_not_found']) is bool
        assert type(data['classification_state']) is dict
        assert type(data['is_paired']) is bool
        assert data['imei_norm'] == imei[:14]
    else:
        rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version),
                                   imei=imei,
                                   include_registration_status=True,
                                   include_stolen_status=True))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert type(data) is dict
        assert type(data['classification_state']) is dict
        assert type(data['classification_state']['informative_conditions']) is list
        assert type(data['classification_state']['blocking_conditions']) is list
        assert type(data['imei_norm']) is str
        assert type(data['registration_status']) is dict
        assert type(data['stolen_status']) is dict
        assert type(data['realtime_checks']) is dict
        assert type(data['realtime_checks']['ever_observed_on_network']) is bool
        assert type(data['realtime_checks']['invalid_imei']) is bool
        assert type(data['realtime_checks']['is_exempted_device']) is bool
        assert type(data['realtime_checks']['is_paired']) is bool


def test_imei_api_reg_status_is_conditional(flask_app):
    """
    Test depot not known yet.

    Verify that the imei-api registration and stolen status fields are optional.
    """
    # registration and stolen status are not available by default
    rv = flask_app.get(url_for('v2.imei_get_api', imei='123456789012345'))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data.get('registration_status') is None
    assert data.get('stolen_status') is None

    # registration and stolen fields are included when found query params
    rv = flask_app.get(url_for('v2.imei_get_api',
                               imei='123456789054321',
                               include_registration_status=True,
                               include_stolen_status=True))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data.get('registration_status') is not None
    assert data.get('stolen_status') is not None


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
    imei_list = ['10000000000000', '10000000000001', '1000000000000200']

    if api_version == 'v1':
        # APPROVED_IMEI
        # 10000000000000
        # 10000000000001
        # 10000000000002 ....
        # Verify that 10000000000000 (14 digits) in reg_list
        # Verify that 1000000000000200 (16 digits) in reg_list
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
    else:  # api version 2.0 tests
        # verify that imeis are in registration list
        registration_list_importer.import_data()
        for imei in imei_list:
            rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version),
                                       imei=imei,
                                       include_registration_status=True,
                                       include_stolen_status=True))
            assert rv.status_code == 200
            data = json.loads(rv.data.decode('utf-8'))
            assert data['imei_norm'] == imei[:14]
            assert data['registration_status']['provisional_only'] is False
            assert data['registration_status']['status'] == 'whitelist'

        # verify that imei is not registered
        imei_not_reg = '20000000000000'
        rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version),
                                   imei=imei_not_reg,
                                   include_registration_status=True,
                                   include_stolen_status=True))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['imei_norm'] == imei_not_reg[:14]
        assert data['registration_status']['provisional_only'] is None
        assert data['registration_status']['status'] is None

        # verify that api returns correct result for exempted device types
        monkeypatch.setattr(mocked_config.region_config, 'exempted_device_types', ['Vehicle', 'Dongle'])
        gsma_tac_db_importer.import_data()

        # following imeis are not in registration list but belong to exempted device types
        imei_list = ['012344022302145', '012344035454564']
        for imei in imei_list:
            rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version),
                                       imei=imei,
                                       include_registration_status=True,
                                       include_stolen_status=True))
            assert rv.status_code == 200
            data = json.loads(rv.data.decode('utf-8'))
            assert data['realtime_checks']['is_exempted_device'] is True

        # following imei is not in registration list and don't belong to exempted device type
        imei = '012344014741025'
        rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version),
                                   imei=imei,
                                   include_registration_status=True,
                                   include_stolen_status=True))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['realtime_checks']['is_exempted_device'] is False


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(content='approved_imei,make,model,status,'
                                                         'model_number,brand_name,device_type,'
                                                         'radio_interface,device_id\n'
                                                         '10000000000000,,,whitelist,,,,,1\n'
                                                         '10000000000001,,,whitelist,,,,,2\n'
                                                         '10000000000002,,,something_else,,,,,3\n')],
                         indirect=True)
def test_registration_list_status_filter(flask_app, registration_list_importer, api_version):
    """Test Depot not known yet.

    Verify IMEI API 'in_registration_list' realtime check does not filter for non-whitelisted statuses.
    """
    if api_version == 'v1':
        # 10000000000002 is in registration_list but status is not whitelist and is not filtered
        registration_list_importer.import_data()
        imei_list = ['10000000000000', '10000000000001']
        check_in_registration_list_helper(imei_list, True, api_version, flask_app)
        check_in_registration_list_helper(['10000000000002'], False, api_version, flask_app)
    else:  # api version 2.0
        # 10000000000002 is in registration_list but status is not whitelist and is not filtered
        registration_list_importer.import_data()
        imei_list = ['10000000000000', '10000000000001']
        for imei in imei_list:
            rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version),
                                       imei=imei,
                                       include_registration_status=True,
                                       include_stolen_status=True))
            assert rv.status_code == 200
            data = json.loads(rv.data.decode('utf-8'))
            assert data['registration_status']['status'] is not None

        rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version),
                                   imei='10000000000002',
                                   include_registration_status=True,
                                   include_stolen_status=True))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['registration_status']['status'] == 'something_else'


def check_output_data(flask_app, i, api_version, include_seen_with_bool=True,
                      class_block_gsma_bool=False, class_block_dupl_bool=False,
                      real_invalid_imei_bool=False, real_gsma_bool=False, class_block_stolen_bool=False,
                      class_informative_malf_bool=False, include_paired_with_bool=True):
    """Helper function used to DRY out IMEI API tests."""
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=i,
                                   include_seen_with=include_seen_with_bool,
                                   include_paired_with=include_paired_with_bool))
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
    imei_list_1 = ['1', '123456', '1234567890ABCDEF']
    imei_list_2 = ['1234567890ABCDEFG', '1234567890ABCDEFG3']

    if api_version == 'v1':
        for i in imei_list_1:
            rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=i))
            assert rv.status_code == 200
        for i in imei_list_2:
            rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=i))
            assert rv.status_code == 400
            assert b'Bad IMEI format (too long)' in rv.data
    else:  # verion 2.0 tests
        for i in imei_list_1:
            rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei=i))
            assert rv.status_code == 200
        for i in imei_list_2:
            rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei=i))
            assert rv.status_code == 400
            assert b'Bad IMEI format (too long)' in rv.data


def test_empty_imei(flask_app, api_version):
    """Test Depot ID not known yet.

    Verify that IMEI API should return a 404 status for a zero-length IMEI.
    """
    """ IMEI API should return a 404 status for a zero-length IMEI """
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=''))
        assert rv.status_code == 404
    else:  # version 2.0 tests
        rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei=''))
        assert rv.status_code == 404


def test_invalid_imei_realtime_checks(flask_app, api_version):
    """Test Depot ID 96548/5.

    Verify IMEI API should calculate some
    realtime checks on an IMEI so that the API returns useful
    info even if an IMEI has never been seen and classfied.
    """
    if api_version == 'v1':
        check_output_data(flask_app, '123456', api_version, real_gsma_bool=True, real_invalid_imei_bool=True)
        check_output_data(flask_app, '3884773337002633', api_version, real_gsma_bool=True)
    else:  # api version 2.0
        imei = '123456'
        rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei=imei))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['realtime_checks']['invalid_imei'] is True

        imei = '3884773337002633'
        rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei=imei))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['realtime_checks']['invalid_imei'] is False


def test_imei_normalisation(flask_app, api_version):
    """Test Depot ID 96549/6.

    Verify IMEI API should normalise an input IMEI.
    """
    imei_list = ['0117220037002633', '1234567890123456', '123456789012345']
    if api_version == 'v1':
        for i in imei_list:
            rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei=i))
            assert rv.status_code == 200
            data = json.loads(rv.data.decode('utf-8'))
            expected_norm = i if len(i) < 14 else i[:14]
            assert data['imei_norm'] == expected_norm
    else:  # version 2.0 tests
        for i in imei_list:
            rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei=i))
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

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei='3884773337002633'))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['imei_norm'] == '38847733370026'
        for k, v in data['classification_state']['blocking_conditions'].items():
            assert v is False
        for k, v in data['classification_state']['informative_conditions'].items():
            assert v is False
        assert data['realtime_checks']['invalid_imei'] is False
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei='3884773337002633'))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['imei_norm'] == '38847733370026'
        for condition in data['classification_state']['blocking_conditions']:
            assert type(condition['condition_name']) is str
            assert condition['condition_met'] is False

        for condition in data['classification_state']['informative_conditions']:
            assert type(condition['condition_name']) is str
            assert condition['condition_met'] is False


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
    if api_version == 'v1':
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
    if api_version == 'v1':
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
    if api_version == 'v1':
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
    if api_version == 'v1':
        operator_data_importer.import_data()
        data = check_output_data(flask_app, '3884773337002638', api_version,
                                 class_block_dupl_bool=True, real_gsma_bool=True)
        assert data['seen_with'] == [{'imsi': '11104803062043', 'msisdn': '22300049781840'}]
        data = check_output_data(flask_app, '3884773337002638', api_version, include_seen_with_bool=False,
                                 class_block_dupl_bool=True, real_gsma_bool=True)
        assert data.get('seen_with', None) is None


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(content='imei,imsi,msisdn\n'
                                                 '38847733370026,111018001111111,333222111555555\n'
                                                 '38847733370026,111015113222222,333222111555556\n'
                                                 '38847733370020,111015113333333,333222111555557')],
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
    if api_version == 'v1':
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
    else:  # api version 2.0
        imei = '38847733370026'
        rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei=imei))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['realtime_checks']['is_paired'] is True
        for condition in data['classification_state']['blocking_conditions']:
            if condition['condition_name'] == 'duplicate_mk1':
                assert condition['condition_met'] is True

        imei = '35000000000000'
        rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei=imei))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['realtime_checks']['is_paired'] is False
        for condition in data['classification_state']['blocking_conditions']:
            if condition['condition_name'] == 'duplicate_mk1':
                assert condition['condition_met'] is True

        imei = '38847733370026'
        rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei=imei))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['realtime_checks']['is_paired'] is True
        for condition in data['classification_state']['blocking_conditions']:
            if condition['condition_name'] == 'duplicate_mk1':
                assert condition['condition_met'] is True


def test_put_not_allowed(flask_app, db_conn, tmpdir, logger, api_version):
    """Test Depot ID 96545/2.

    Verify the IMEI API does not support HTTP PUT and returns HTTP 405 METHOD NOT ALLOWED.
    """
    global endpoint_name
    imei_list = ['3884773337002633']
    if api_version == 'v1':
        endpoint_name = 'imei_api'
    else:  # api version 2.0
        endpoint_name = 'imei_get_api'

    for i in imei_list:
        rv = flask_app.put(url_for('{0}.{1}'.format(api_version, endpoint_name), imei=i))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data


def test_post_not_allowed(flask_app, db_conn, tmpdir, logger, api_version):
    """Test Depot ID 96545/2.

    Verify the IMEI API does not support HTTP POST and returns HTTP 405 METHOD NOT ALLOWED.
    """
    global endpoint_name
    imei_list = ['3884773337002633']
    if api_version == 'v1':
        endpoint_name = 'imei_api'
    else:  # api version 2.0
        endpoint_name = 'imei_get_api'

    for i in imei_list:
        rv = flask_app.post(url_for('{0}.{1}'.format(api_version, endpoint_name), imei=i))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data


def test_delete_not_allowed(flask_app, db_conn, tmpdir, logger, api_version):
    """Test Depot ID 96545/2.

    Verify the IMEI API does not support HTTP DELETE and returns HTTP 405 METHOD NOT ALLOWED.
    """
    global endpoint_name
    imei_list = ['3884773337002633']
    if api_version == 'v1':
        endpoint_name = 'imei_api'
    else:  # api version 2.0
        endpoint_name = 'imei_get_api'

    for i in imei_list:
        rv = flask_app.delete(url_for('{0}.{1}'.format(api_version, endpoint_name), imei=i))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data


def test_response_headers(flask_app, api_version):
    """Verify the security headers are set properly on returned response."""
    global endpoint_name  # endpoint name for api version 1.0
    if api_version == 'v1':
        endpoint_name = 'imei_api'
    else:  # api version 2.0
        endpoint_name = 'imei_get_api'

    rv = flask_app.get(url_for('{0}.{1}'.format(api_version, endpoint_name), imei='123456789012345'))
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

    if api_version == 'v1':
        check_in_network_imeis_table_helper(False)
        operator_data_importer.import_data()
        check_in_network_imeis_table_helper(True)
    else:
        imei_list = ['01376803870943', '64220297727231', '64220299727231', '64220498727231']
        for i in imei_list:
            rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei=i))
            assert rv.status_code == 200
            data = json.loads(rv.data.decode('utf-8'))
            assert data['realtime_checks']['ever_observed_on_network'] is False

        operator_data_importer.import_data()
        for i in imei_list:
            rv = flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei=i))
            assert rv.status_code == 200
            data = json.loads(rv.data.decode('utf-8'))
            assert data['realtime_checks']['ever_observed_on_network'] is True


def test_imei_pairing_api_response_structure(flask_app):
    """Test Depot not known yet.

    Verify that whether IMEI-Pairings API returns correct response structure.
    """
    rv = flask_app.get(url_for('v2.imei_get_pairings_api', imei='64220297727231'))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['imei_norm'] == '64220297727231'
    assert type(data['_keys']) is dict
    assert type(data['pairs']) is list
    assert type(data['_keys']['current_key']) is str
    assert type(data['_keys']['next_key']) is str
    assert type(data['_keys']['result_size']) is int


def test_post_method_not_allowed_on_pairings_api(flask_app):
    """Test Depot not known yet.

    Verify that post method is not allowed on IMEI-Pairings API.
    """
    rv = flask_app.post(url_for('v2.imei_get_pairings_api', imei='64220297727231'))
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_put_method_not_allowed_on_pairings_api(flask_app):
    """Test Depot not known yet.

    Verify that post method is not allowed on IMEI-Pairings API.
    """
    rv = flask_app.put(url_for('v2.imei_get_pairings_api', imei='64220297727231'))
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_delete_method_not_allowed_on_pairings_api(flask_app):
    """Test Depot not known yet.

    Verify that post method is not allowed on IMEI-Pairings API.
    """
    rv = flask_app.delete(url_for('v2.imei_get_pairings_api', imei='64220297727231'))
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_options_method_not_allowed_on_pairings_api(flask_app):
    """Test Depot not known yet.

    Verify that post method is not allowed on IMEI-Pairings API.
    """
    rv = flask_app.options(url_for('v2.imei_get_pairings_api', imei='64220297727231'))
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_long_short_imeis_on_pairings_api(flask_app):
    """Test Depot not known yet.

    Verify that IMEI-Pairings should return proper responses as per input format.
    """
    imei_list_1 = ['1', '123456', '1234567890ABCDEF']
    imei_list_2 = ['1234567890ABCDEFG', '1234567890ABCDEFG3']

    # short or valid IMEI formats
    for imei in imei_list_1:
        rv = flask_app.get(url_for('v2.imei_get_pairings_api', imei=imei))
        assert rv.status_code == 200

    # long imei formats
    for imei in imei_list_2:
        rv = flask_app.get(url_for('v2.imei_get_pairings_api', imei=imei))
        assert rv.status_code == 400
        assert b'Bad IMEI format (too long)' in rv.data


def test_empty_imei_on_pairings_api(flask_app):
    """Test Depot not known yet.

    Verify that IMEI-Pairings API should return 404 status for a zero-lenth IMEI.
    """
    rv = flask_app.get(url_for('v2.imei_get_pairings_api', imei=''))
    print(rv.data)
    assert rv.status_code == 308


def test_imei_normalisation_on_pairings_api(flask_app):
    """Test Depot not known yet.

    Verify that IMEI-Pairings API should normalize the input IMEI.
    """
    imei = '0117220037002633'
    rv = flask_app.get(url_for('v2.imei_get_pairings_api', imei=imei))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['imei_norm'] == imei[:14]


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(content='imei,imsi,msisdn\n'
                                                 '38847733370026,111018001111111,111112222233334\n'
                                                 '38847733370026,111015113222222,111112222233335\n'
                                                 '38847733370020,111015113333333,111112222233336\n'
                                                 '38847733370026,111016111111111,111112222233337\n'
                                                 '38847733370026,111016222222222,111112222233338\n'
                                                 '38847733370026,111016333333333,111112222233339')],
                         indirect=True)
@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(content='approved_imei,make,model,status,'
                                                         'model_number,brand_name,device_type,'
                                                         'radio_interface,device_id\n'
                                                         '38847733370026,,,whitelist,,,,,1\n'
                                                         '38847733370020,,,whitelist,,,,,2\n'
                                                         '10000000000002,,,something_else,,,,,3\n')],
                         indirect=True)
def test_pagination_on_pairings_api(flask_app, registration_list_importer, pairing_list_importer):
    """Test Depot not known yet.

    Verify that IMEI-Pairings API should support pagination.
    """
    registration_list_importer.import_data()
    pairing_list_importer.import_data()
    imei = '38847733370026'
    rv = flask_app.get(url_for('v2.imei_get_pairings_api', imei=imei))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert len(data['pairs']) == data['_keys']['result_size']

    # query string params, 2 result per page starting from result 1
    offset = 1
    limit = 2
    rv = flask_app.get(url_for('v2.imei_get_pairings_api', imei=imei, offset=offset, limit=limit))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['imei_norm'] == imei
    assert len(data['pairs']) == limit
    assert data['_keys']['current_key'] == '1'
    assert data['_keys']['next_key'] == str(int(offset) + int(limit))

    # query string params, 3 results per page starting from 2
    offset = 2
    limit = 3
    rv = flask_app.get(url_for('v2.imei_get_pairings_api', imei=imei, offset=offset, limit=limit))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['imei_norm'] == imei
    assert len(data['pairs']) == limit
    assert data['_keys']['result_size'] == 5
    assert data['_keys']['current_key'] == str(offset)
    assert data['_keys']['next_key'] == str(int(offset) + int(limit))


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(content='approved_imei,make,model,status,'
                                                         'model_number,brand_name,device_type,'
                                                         'radio_interface,device_id\n'
                                                         '38847733370026,,,whitelist,,,,,1\n'
                                                         '38847733370020,,,,,,,,2\n'
                                                         '10000000000002,,,something_else,,,,,3\n')],
                         indirect=True)
def test_imei_api_registration_status(flask_app, registration_list_importer):
    """Test Depot not known yet.

    Verify IMEI-API registration_status checks.
    """
    registration_list_importer.import_data()
    imei = '38847733370026'
    rv = flask_app.get(url_for('v2.imei_get_api',
                               imei=imei,
                               include_registration_status=True,
                               include_stolen_status=True))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['registration_status']['status'] == 'whitelist'
    assert data['registration_status']['provisional_only'] is False
    assert data['stolen_status']['status'] is None
    assert data['stolen_status']['provisional_only'] is None

    imei = '38847733370020'
    rv = flask_app.get(url_for('v2.imei_get_api',
                               imei=imei,
                               include_registration_status=True,
                               include_stolen_status=True))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['registration_status']['status'] is None
    assert data['registration_status']['provisional_only'] is False
    assert data['stolen_status']['status'] is None
    assert data['stolen_status']['provisional_only'] is None

    imei = '10000000000002'
    rv = flask_app.get(url_for('v2.imei_get_api',
                               imei=imei,
                               include_registration_status=True,
                               include_stolen_status=True))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['registration_status']['status'] == 'something_else'
    assert data['registration_status']['provisional_only'] is True
    assert data['stolen_status']['status'] is None
    assert data['stolen_status']['provisional_only'] is None


@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(content='imei,reporting_date,status\n'
                                                   '622222222222222,20160426,blacklist\n'
                                                   '122222222222223,20160425,\n'
                                                   '238888888888884,20160425,something_else')],
                         indirect=True)
def test_imei_api_stolen_status(flask_app, stolen_list_importer):
    """Test Depot not known yet.

    Verify IMEI-API stolen_status checks.
    """
    stolen_list_importer.import_data()
    imei = '622222222222222'
    rv = flask_app.get(url_for('v2.imei_get_api',
                               imei=imei,
                               include_registration_status=True,
                               include_stolen_status=True))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['registration_status']['status'] is None
    assert data['registration_status']['provisional_only'] is None
    assert data['stolen_status']['status'] == 'blacklist'
    assert data['stolen_status']['provisional_only'] is False

    imei = '122222222222223'
    rv = flask_app.get(url_for('v2.imei_get_api',
                               imei=imei,
                               include_registration_status=True,
                               include_stolen_status=True))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['registration_status']['status'] is None
    assert data['registration_status']['provisional_only'] is None
    assert data['stolen_status']['status'] is None
    assert data['stolen_status']['provisional_only'] is False

    imei = '122222222222223'
    rv = flask_app.get(url_for('v2.imei_get_api',
                               imei=imei,
                               include_registration_status=True,
                               include_stolen_status=True))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['registration_status']['status'] is None
    assert data['registration_status']['provisional_only'] is None
    assert data['stolen_status']['status'] is None
    assert data['stolen_status']['provisional_only'] is False

    imei = '238888888888884'
    rv = flask_app.get(url_for('v2.imei_get_api',
                               imei=imei,
                               include_registration_status=True,
                               include_stolen_status=True))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['registration_status']['status'] is None
    assert data['registration_status']['provisional_only'] is None
    assert data['stolen_status']['status'] == 'something_else'
    assert data['stolen_status']['provisional_only'] is True


def test_imei_subscribers_api_response_structure(flask_app):
    """Test Depot not known yet.

    Verify that whether IMEI-Subscribers API returns correct response structure.
    """
    rv = flask_app.get(url_for('v2.imei_get_subscribers_api', imei='64220297727231'))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['imei_norm'] == '64220297727231'
    assert type(data['_keys']) is dict
    assert type(data['subscribers']) is list
    assert type(data['_keys']['current_key']) is str
    assert type(data['_keys']['next_key']) is str
    assert type(data['_keys']['result_size']) is int


def test_post_method_not_allowed_on_subscribers_api(flask_app):
    """Test Depot not known yet.

    Verify that post method is not allowed on IMEI-Pairings API.
    """
    rv = flask_app.post(url_for('v2.imei_get_subscribers_api', imei='64220297727231'))
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_put_method_not_allowed_on_subscribers_api(flask_app):
    """Test Depot not known yet.

    Verify that post method is not allowed on IMEI-Pairings API.
    """
    rv = flask_app.put(url_for('v2.imei_get_subscribers_api', imei='64220297727231'))
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_delete_method_not_allowed_on_subscribers_api(flask_app):
    """Test Depot not known yet.

    Verify that post method is not allowed on IMEI-Pairings API.
    """
    rv = flask_app.delete(url_for('v2.imei_get_subscribers_api', imei='64220297727231'))
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_options_method_not_allowed_on_subscribers_api(flask_app):
    """Test Depot not known yet.

    Verify that post method is not allowed on IMEI-Pairings API.
    """
    rv = flask_app.options(url_for('v2.imei_get_subscribers_api', imei='64220297727231'))
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_long_short_imei_format_on_subscribers_api(flask_app):
    """Test Depot not known yet.

    Verify IMEI-Subscribers API responses on long and short IMEI formats.
    """
    imei_list_1 = ['1', '123456', '1234567890ABCDEF']
    imei_list_2 = ['1234567890ABCDEFG', '1234567890ABCDEFG3']

    # short or valid imei formats
    for imei in imei_list_1:
        rv = flask_app.get(url_for('v2.imei_get_subscribers_api', imei=imei))
        assert rv.status_code == 200

    # long imei formats
    for imei in imei_list_2:
        rv = flask_app.get(url_for('v2.imei_get_subscribers_api', imei=imei))
        assert rv.status_code == 400
        assert b'Bad IMEI format (too long)' in rv.data


def test_empty_imei_on_subscribers_api(flask_app):
    """Test Depot not known yet.

    Verify that IMEI-SUbscribers API should return
    """
    rv = flask_app.get(url_for('v2.imei_get_subscribers_api', imei=''))
    assert rv.status_code == 308


def test_imei_normalisation_on_subscribers_api(flask_app):
    """Test Depot not known yet.

    Verify that IMEI_Subscribers API should normalize the input IMEI.
    """
    imei = '0117220037002633'
    rv = flask_app.get(url_for('v2.imei_get_subscribers_api', imei=imei))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['imei_norm'] == imei[:14]


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161122,01376803870943,123456789012345,123456789012345\n'
                                     '20161112,01376803870943,111018001111111,345266728277662\n'
                                     '20161109,01376803870943,111021600211121,546367736265242\n'
                                     '20161108,01376803870943,111031600211121,345637778287832\n'
                                     '20161107,01376803870943,111041600211121,321312332221122\n'
                                     '20161106,01376803870943,111051600212221,657737388998778',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
def test_pagination_on_subscribers_api(flask_app, operator_data_importer):
    """Test Depot not known yet.

    Verify pagination support on IMEI-Subscribers API.
    """
    operator_data_importer.import_data()
    rv = flask_app.get(url_for('v2.imei_get_subscribers_api', imei='01376803870943'))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['imei_norm'] == '01376803870943'
    assert data['_keys']['result_size'] == len(data['subscribers'])
    assert data['_keys']['current_key'] == '0'
    assert data['_keys']['next_key'] == '10'

    # params offset 1, limit 2
    offset = 1
    limit = 2
    rv = flask_app.get(url_for('v2.imei_get_subscribers_api', imei='01376803870943', offset=offset, limit=limit))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert len(data['subscribers']) == limit
    assert data['_keys']['current_key'] == str(offset)
    assert data['_keys']['next_key'] == str(int(offset) + int(limit))
    assert data['_keys']['result_size'] == 6

    # params offset 3, limit 2
    offset = 3
    limit = 2
    rv = flask_app.get(url_for('v2.imei_get_subscribers_api', imei='01376803870943', offset=offset, limit=limit))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert len(data['subscribers']) == limit
    assert data['_keys']['current_key'] == str(offset)
    assert data['_keys']['next_key'] == str(int(offset) + int(limit))

    # params offset 1, limit 3, order descending
    offset = 1
    limit = 6
    order = 'DESC'
    rv = flask_app.get(url_for('v2.imei_get_subscribers_api',
                               imei='01376803870943', offset=offset,
                               limit=limit, order=order))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['subscribers'][0]['last_seen'] == '2016-11-12'
    assert data['subscribers'][4]['last_seen'] == '2016-11-06'

    # params offset 1, limit 3, order ascending
    offset = 1
    limit = 6
    order = 'ASC'
    rv = flask_app.get(url_for('v2.imei_get_subscribers_api',
                               imei='01376803870943', offset=offset,
                               limit=limit, order=order))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['subscribers'][0]['last_seen'] == '2016-11-07'
    assert data['subscribers'][4]['last_seen'] == '2016-11-22'


def test_batch_imei_api_response_structure(flask_app):
    """Test Depot not known yet.

    Verify Batch-IMEI API response structure.
    """
    imeis = ['64220297727231', '64220297727231']
    headers = {'content-type': 'application/json'}
    rv = flask_app.post(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imeis}), headers=headers)
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert type(data) == dict
    assert type(data['results']) == list

    for item in data['results']:
        assert type(item['imei_norm']) == str
        assert type(item['classification_state']) == dict
        assert type(item['classification_state']['informative_conditions']) == list
        assert type(item['classification_state']['blocking_conditions']) == list
        assert type(item['realtime_checks']) == dict
        assert type(item['realtime_checks']['ever_observed_on_network']) == bool
        assert type(item['realtime_checks']['is_exempted_device']) == bool
        assert type(item['realtime_checks']['invalid_imei']) == bool
        assert type(item['realtime_checks']['is_paired']) == bool

    # verify that the registration and stolen status is optional
    payload = {
        'imeis': imeis,
        'include_registration_status': True,
        'include_stolen_status': True
    }
    rv = flask_app.post(url_for('v2.imei_batch_api'), data=json.dumps(payload), headers=headers)
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    for item in data['results']:
        assert type(item['imei_norm']) == str
        assert type(item['classification_state']) == dict
        assert type(item['classification_state']['informative_conditions']) == list
        assert type(item['classification_state']['blocking_conditions']) == list
        assert type(item['registration_status']) == dict
        assert type(item['stolen_status']) == dict
        assert type(item['realtime_checks']) == dict
        assert type(item['realtime_checks']['ever_observed_on_network']) == bool
        assert type(item['realtime_checks']['is_exempted_device']) == bool
        assert type(item['realtime_checks']['invalid_imei']) == bool
        assert type(item['realtime_checks']['is_paired']) == bool


def test_get_method_not_allowed_on_batch_imei_api(flask_app):
    """Test Depot not known yet.

    Verify that GET Method is not allowed on IMEI-Batch API.
    """
    imeis = ['64220297727231', '64220297727231']
    headers = {'content-type': 'application/json'}
    rv = flask_app.get(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imeis}), headers=headers)
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_put_method_not_allowed_on_batch_imei_api(flask_app):
    """Test Depot not known yet.

    Verify that PUT Method is not allowed on IMEI-Batch API.
    """
    imeis = ['64220297727231', '64220297727231']
    headers = {'content-type': 'application/json'}
    rv = flask_app.put(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imeis}), headers=headers)
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_delete_method_not_allowed_on_batch_imei_api(flask_app):
    """Test Depot not known yet.

    Verify that DELETE Method is not allowed on IMEI-Batch API.
    """
    imeis = ['64220297727231', '64220297727231']
    headers = {'content-type': 'application/json'}
    rv = flask_app.delete(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imeis}), headers=headers)
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_options_method_not_allowed_on_batch_imei_api(flask_app):
    """Test Depot not known yet.

    Verify that OPTIONS Method is not allowed on IMEI-Batch API.
    """
    imeis = ['64220297727231', '64220297727231']
    headers = {'content-type': 'application/json'}
    rv = flask_app.options(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imeis}), headers=headers)
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_long_short_imei_formats_on_batch_imei_api(flask_app):
    """Test Depot not known yet.

    Verify Batch-IMEI API reponses on long and short IMEI formats.
    """
    imei_list_1 = ['1', '123456', '1234567890ABCDEF']
    imei_list_2 = ['1234567890ABCDEFG', '1234567890ABCDEFG3']
    headers = {'content-type': 'application/json'}

    # short imei formats allowed
    rv = flask_app.post(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imei_list_1}), headers=headers)
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert len(data['results']) == 3

    # longer imei formats not allowed
    rv = flask_app.post(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imei_list_2}), headers=headers)
    assert rv.status_code == 400
    assert b'Bad IMEI format (too long)' in rv.data


def test_limit_on_imei_on_batch_imei_api(flask_app):
    """Test Depot not known yet.

    Verify limit on IMEI-Batch API.
    """
    imeis = []
    for imei in range(1000):
        imeis.append('1234567890123456')

    rv = flask_app.post(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imeis}),
                        content_type='application/json')
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert len(data['results']) == 1000

    imeis.append('234563789656545')
    rv = flask_app.post(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imeis}),
                        content_type='application/json')
    assert rv.status_code == 400


def test_empty_imeis_on_batch_imei_api(flask_app):
    """Test Depot not known yet.

    Verify that IMEI-Batch API don't accepts empty imei formats
    """
    imei_list = ['', '', '']
    headers = {'content-type': 'application/json'}
    rv = flask_app.post(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imei_list}), headers=headers)
    assert rv.status_code == 400
    assert b'Bad IMEI format (empty imei).' in rv.data


def test_whitespace_imeis_on_batch_imei_api(flask_app):
    """Test Depot not known yet.

    Verify that IMEI-Batch API don't accepts whitespace or tabs imeis.
    """
    # whitespces
    imei_list = [' ', '  ', '   ']
    headers = {'content-type': 'application/json'}
    rv = flask_app.post(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imei_list}), headers=headers)
    assert rv.status_code == 400
    assert b'Bad IMEI format (whitespces not allowed).' in rv.data

    # tabs
    imei_list = ['  ', '        ', '            ']
    rv = flask_app.post(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imei_list}), headers=headers)
    assert rv.status_code == 400
    assert b'Bad IMEI format (whitespces not allowed).' in rv.data


def test_invalid_imei_realtime_checks_batch_imei_api(flask_app):
    """Test Depot not known yet.

    Verify that IMEI-Batch api should calculate some
    realtime checks on IMEIs so that API responds with
    some useful information.
    """
    imei_list = ['123456']
    headers = {'content-type': 'application/json'}
    rv = flask_app.post(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imei_list}), headers=headers)
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['results'][0]['realtime_checks']['invalid_imei'] is True

    imei_list = ['1234567890123456']
    rv = flask_app.post(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imei_list}), headers=headers)
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['results'][0]['realtime_checks']['invalid_imei'] is False


def test_imei_normalisation_on_batch_imei_api(flask_app):
    """Test Depot not known yet.

    Verify that Batch-IMEI API should normalize an input IMEI.
    """
    imei_list = ['0117220037002633']
    headers = {'content-type': 'application/json'}
    rv = flask_app.post(url_for('v2.imei_batch_api'), data=json.dumps({'imeis': imei_list}), headers=headers)
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert len(data['results'][0]['imei_norm']) == 14


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(filename='sample_registration_list.csv')],
                         indirect=True)
def test_imei_info_api_response(flask_app, registration_list_importer):
    """Test Depot not known yet.

    Verify correct response structure for IMEI-Info api.
    """
    response_struct_keys = ['imei_norm', 'status', 'make', 'model', 'model_number', 'brand_name',
                            'device_type', 'radio_interface', 'associated_imeis']
    registration_list_importer.import_data()
    rv = flask_app.get(url_for('v2.imei_info_api', imei='10000000000000'))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert type(data) is dict
    for key, value in data.items():
        assert key in response_struct_keys


@pytest.mark.parametrize('registration_list_importer',
                         [RegistrationListParams(content='approved_imei,make,model,status,'
                                                         'model_number,brand_name,device_type,'
                                                         'radio_interface,device_id\n'
                                                         '10000000000000,samsung,s9,whitelist,'
                                                         'sw928,galaxy,smart phone,2g 3g 4g,23e\n')],
                         indirect=True)
def test_imei_info_api(flask_app, registration_list_importer):
    """Test Depot ID not known yet.

    Verify that IMEI-Info api is returning correct response as expected.
    """
    # test with existing imei in database
    imei = '10000000000000'
    registration_list_importer.import_data()
    rv = flask_app.get(url_for('v2.imei_info_api', imei=imei))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data.get('imei_norm') == imei
    assert data.get('make') == 'samsung'
    assert data.get('model') == 's9'
    assert data.get('status') == 'whitelist'
    assert data.get('model_number') == 'sw928'
    assert data.get('brand_name') == 'galaxy'
    assert data.get('device_type') == 'smart phone'
    assert data.get('radio_interface') == '2g 3g 4g'

    # test with non existing imei in database, response should be {}
    imei = '123456789012345'
    rv = flask_app.get(url_for('v2.imei_info_api', imei=imei))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data == {}


def test_imei_info_api_with_empty_imei(flask_app):
    """Test Depot ID not known yet.

    Verify that IMEI-Info api returns 404 on zero length imei.
    """
    imei = ''
    rv = flask_app.get(url_for('v2.imei_info_api', imei=imei))
    assert rv.status_code == 308


def test_imei_info_api_with_long_imeis(flask_app):
    """Test Depot ID not known yet.

    Verify that IMEI-Info api returns 400 on long imeis.
    """
    imeis = ['1234567890ABCDEFG', '1234567890ABCDEFG3']
    for imei in imeis:
        rv = flask_app.get(url_for('v2.imei_info_api', imei=imei))
        assert rv.status_code == 400
        assert b'Bad IMEI format (too long)' in rv.data


def test_imei_info_api_with_short_valid_imeis(flask_app):
    """Test Depot ID not known yet.

    Verify that IMEI-Info api returns 200 for short and valid imeis.
    """
    imeis = ['1', '123456', '1234567890ABCDEF']
    for imei in imeis:
        rv = flask_app.get(url_for('v2.imei_info_api', imei=imei))
        assert rv.status_code == 200


def test_post_method_not_allowed_on_imei_info_api(flask_app):
    """Test Depot not known yet.

    Verify that GET Method is not allowed on IMEI-Info API.
    """
    imei = '64220297727231'
    rv = flask_app.post(url_for('v2.imei_info_api', imei=imei))
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_put_method_not_allowed_on_imei_info_api(flask_app):
    """Test Depot not known yet.

    Verify that PUT Method is not allowed on IMEI-Info API.
    """
    imei = '64220297727231'
    rv = flask_app.put(url_for('v2.imei_info_api', imei=imei))
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_delete_method_not_allowed_on_imei_info_api(flask_app):
    """Test Depot not known yet.

    Verify that DELETE Method is not allowed on IMEI-Info API.
    """
    imei = '64220297727231'
    rv = flask_app.delete(url_for('v2.imei_info_api', imei=imei))
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data


def test_options_method_not_allowed_on_imei_info_api(flask_app):
    """Test Depot not known yet.

    Verify that OPTIONS Method is not allowed on IMEI-Info API.
    """
    imei = '64220297727231'
    rv = flask_app.options(url_for('v2.imei_info_api', imei=imei))
    assert rv.status_code == 405
    assert b'The method is not allowed for the requested URL' in rv.data
