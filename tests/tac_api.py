"""
tac api data import unit tests.

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

from _fixtures import *  # noqa: F403, F401
from _importer_params import GSMADataParams


def test_long_short_and_non_numeric_tac(flask_app, api_version):
    """Test Depot ID 96788/5.

    Verify that TAC API returns a 400 status for short and non-numeric,
    shorter and longer tacs.
    """
    if api_version == 'v1':
        # non-numeric tacs
        for t in ['abc', '1abc', 'abcdefgh', '1234ABCD', '12345678ABSDEF']:
            rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac=t))
            assert rv.status_code == 400
            assert b'Bad TAC format' in rv.data

        # tacs less than 8 chars long
        for t in ['1', '00', '1234567']:
            rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac=t))
            assert rv.status_code == 400
            assert b'Bad TAC format' in rv.data

        # tacs longer than 8 chars long
        for t in ['123456789', '012345678', '0123456780']:
            rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac=t))
            assert rv.status_code == 400
            assert b'Bad TAC format' in rv.data
    else:  # api version 2
        # non-numeric tacs for tac get api
        non_numeric_tacs = ['abc', '1abc', 'abcdefgh', '1234ABCD', '12345678ABSDEF']
        for t in non_numeric_tacs:
            rv = flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac=t))
            assert rv.status_code == 400

        # non-numeric tacs for tac post api
        headers = {'content-type': 'application/json'}
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': non_numeric_tacs}), headers=headers)
        assert rv.status_code == 400

        # tacs less than 8 chars long
        invalid_tacs = ['1', '00', '1234567']
        for t in invalid_tacs:
            rv = flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac=t))
            assert rv.status_code == 400

        # tacs less than 8 chars long for post api
        headers = {'content-type': 'application/json'}
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': invalid_tacs}), headers=headers)
        assert rv.status_code == 400

        # tacs longer than 8 chars long
        invalid_tacs = ['123456789', '012345678', '0123456780']
        for t in invalid_tacs:
            rv = flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac=t))
            assert rv.status_code == 400

        # tacs longer than 8 chars for post api
        headers = {'content-type': 'application/json'}
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': invalid_tacs}), headers=headers)
        assert rv.status_code == 400


def test_empty_tac(flask_app, api_version):
    """Test Depot ID 96557/8.

    Verify TAC API return a 404 status for a zero-length tac.
    """
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac=''))
        assert rv.status_code == 404
    else:  # api version 2
        rv = flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac=''))
        assert rv.status_code == 404

        # empty tacs for post api
        empty_tacs = ['', '', '', '']
        headers = {'content-type': 'application/json'}
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': empty_tacs}), headers=headers)
        assert rv.status_code == 400

        # one empty tac for post api
        empty_tacs = ['']
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': empty_tacs}), headers=headers)
        assert rv.status_code == 400


def test_valid_missing_tac(flask_app, api_version):
    """Test Depot ID 96558/6.

    Verify that TAC API should return a 200 status for valid
    tacs that are not in GSMA, but the GSMA field should be null.
    """
    missing_tac = '12345678'
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac='12345678'))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['tac'] == missing_tac
        assert data['gsma'] is None
    else:  # api version 2
        rv = flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac=missing_tac))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['tac'] == missing_tac
        assert data['gsma'] is None

        # valid more than one missing tacs for post api
        missing_tacs = ['12345678', '12345677']
        headers = {'content-type': 'application/json'}
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': missing_tacs}), headers=headers)
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert len(data['results']) == 2

        # only one missing valid tac for post api
        missing_tacs = ['12345678']
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)),
                            data=json.dumps({'tacs': missing_tacs}), headers=headers)
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['results'][0]['tac'] == missing_tacs[0]
        assert data['results'][0]['gsma'] is None


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(
                             filename='testData1-gsmatac_operator1_operator4_anonymized.txt')],
                         indirect=True)
def test_valid_tac(flask_app, gsma_tac_db_importer, api_version):
    """Test Depot ID 96559/7 - 96553/1.

    Verify that TAC API should return correct GSMA data
    for a known TAC (38826033).
    """
    gsma_tac_db_importer.import_data()
    valid_tac = '38826033'

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac=valid_tac))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['tac'] == valid_tac
        gsma_data = data['gsma']
        assert gsma_data is not None
        assert gsma_data == {
            'allocation_date': '2001-01-01',
            'bands': '79786815f4e5ab4e775c44f4e5aa237c52147d75',
            'bluetooth': 'Not Known',
            'brand_name': '6fefdf8fdf21220bd9a56f58c1134b33c4e75a40',
            'country_code': '01592d51db5afd0165cb73baca5c0b340c4889f1',
            'device_type': 'a7920de2f4e1473556e8f373b8312a1c3044ef1c',
            'fixed_code': '187a332db248392c0ce1501765301bc6cf780b10',
            'internal_model_name': 'f3e808a9e81ac355d0e86b08a4e35953f14381ef',
            'manufacturer': 'ec307432a3d742bc70041ab01f6740a57f34ba53',
            'manufacturer_code': 'd91c5ff622f821641812336fcc7d964b5f80a0a3',
            'marketing_name': 'f3e808a9e81ac355d0e86b08a4e35953f14381ef',
            'model_name': 'f3e808a9e81ac355d0e86b08a4e35953f14381ef',
            'nfc': 'Y',
            'operating_system': '6fefdf8fdf21220bd9a56f58c1134b33c4e75a40',
            'radio_interface': '0654a028e5aea48c8fbb09871b8f397a186c883b',
            'wlan': 'N'
        }
    elif api_version == 'v2':
        rv = flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac=valid_tac))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['tac'] == valid_tac
        gsma_data = data['gsma']
        assert gsma_data is not None

        assert gsma_data == {
            'allocation_date': '2001-01-01',
            'bands': '79786815f4e5ab4e775c44f4e5aa237c52147d75',
            'bluetooth': 'Not Known',
            'brand_name': '6fefdf8fdf21220bd9a56f58c1134b33c4e75a40',
            'country_code': '01592d51db5afd0165cb73baca5c0b340c4889f1',
            'device_type': 'a7920de2f4e1473556e8f373b8312a1c3044ef1c',
            'fixed_code': '187a332db248392c0ce1501765301bc6cf780b10',
            'internal_model_name': 'f3e808a9e81ac355d0e86b08a4e35953f14381ef',
            'manufacturer': 'ec307432a3d742bc70041ab01f6740a57f34ba53',
            'marketing_name': 'f3e808a9e81ac355d0e86b08a4e35953f14381ef',
            'model_name': 'f3e808a9e81ac355d0e86b08a4e35953f14381ef',
            'nfc': 'Y',
            'operating_system': '6fefdf8fdf21220bd9a56f58c1134b33c4e75a40',
            'radio_interface': '0654a028e5aea48c8fbb09871b8f397a186c883b',
            'wlan': 'N'
        }


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(
                             filename='testData1-gsmatac_operator1_operator4_anonymized.txt')],
                         indirect=True)
def test_batch_tacs(flask_app, gsma_tac_db_importer, api_version):
    """
    Verify TAC batch API.

    Verify that TAC API should return correct GSMA data
    for a batch of known tacs (21260934, 21782434, 38245933, 38709433).
    """
    gsma_tac_db_importer.import_data()
    valid_tacs = {
        'tacs': [
            '21260934',
            '21782434',
            '38245933',
            '38709433'
        ]
    }

    if api_version == 'v1':
        rv = flask_app.post(url_for('{0}.tac_api'.format(api_version), tac='12345678'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data
    else:  # api version 2
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)), data=json.dumps(valid_tacs),
                            content_type='application/json')
        data = json.loads(rv.data.decode('utf-8'))

        assert rv.status_code == 200
        assert data is not None
        assert len(data['results']) == 4

        for item in data['results']:
            gsma_data = item.get('gsma')

            assert gsma_data is not None
            assert item.get('tac') in valid_tacs.get('tacs')

        # batch tacs limit
        tac = '12345678'
        tacs = [tac for t in range(1001)]
        rv = flask_app.post(url_for('{0}.tac_post_api'.format(api_version)), data=json.dumps({'tacs': tacs}),
                            content_type='application/json')
        assert rv.status_code == 400
        assert b"Bad \'tacs\':\'[\'Min 1 and Max 1000 TACs are allowed\']\' argument format" in rv.data


def test_method_put_not_allowed(flask_app, api_version):
    """Test Depot ID 96554/2.

    Verify the TAC API does not support HTTP PUT and returns HTTP 405 METHOD NOT ALLOWED.
    """
    if api_version == 'v1':
        rv = flask_app.put(url_for('{0}.tac_api'.format(api_version), tac='35567907'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data
    else:  # api version 2
        rv = flask_app.put(url_for('{0}.tac_get_api'.format(api_version), tac='35567907'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data

        headers = {'content-type': 'application/json'}
        data = ['12345678', '12345678']
        rv = flask_app.put(url_for('{0}.tac_post_api'.format(api_version)),
                           data=json.dumps({'tacs': data}), headers=headers)
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data


def test_method_post_not_allowed(flask_app, api_version):
    """Test Depot ID 96555/3.

    Verify the TAC API does not support HTTP POST and returns HTTP 405 METHOD NOT ALLOWED.
    """
    if api_version == 'v1':
        rv = flask_app.post(url_for('{0}.tac_api'.format(api_version), tac='35567907'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data
    else:  # api version 2, method allowed
        rv = flask_app.post(url_for('{0}.tac_get_api'.format(api_version), tac='35567907'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data


def test_method_delete_not_allowed(flask_app, api_version):
    """Test Depot ID 96556/3.

    Verify the TAC API does not support HTTP DELETE and returns HTTP 405 METHOD NOT ALLOWED.
    """
    if api_version == 'v1':
        rv = flask_app.delete(url_for('{0}.tac_api'.format(api_version), tac='35567907'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data
    else:  # api version 2
        rv = flask_app.delete(url_for('{0}.tac_get_api'.format(api_version), tac='35567907'))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data

        headers = {'content-type': 'application/json'}
        rv = flask_app.delete(url_for('{0}.tac_post_api'.format(api_version)), headers=headers)
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data


def test_same_tacs_batch_tac(flask_app):
    """Test Depot ID not known.

    Verify that when same tacs are entered to batch tac api, it returns respnse for unique tacs.
    """
    tacs = ['12345678', '12345678', '22222222', '11111111', '11111111']
    headers = {'content-type': 'application/json'}
    rv = flask_app.post(url_for('v2.tac_post_api'), data=json.dumps({'tacs': tacs}), headers=headers,
                        content_type='application/json')
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8')).get('results')
    assert len(data) == 3

    for item in data:
        assert item.get('tac') in tacs
