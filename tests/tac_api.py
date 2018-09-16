"""
tac api data import unit tests.

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

from _fixtures import *    # noqa: F403, F401
from _importer_params import GSMADataParams


def test_long_short_and_non_numeric_tac(flask_app, api_version):
    """Test Depot ID 96788/5.

    Verify that TAC API returns a 400 status for short and non-numeric,
    shorter and longer tacs.
    """
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


def test_empty_tac(flask_app, api_version):
    """Test Depot ID 96557/8.

    Verify TAC API return a 404 status for a zero-length tac.
    """
    rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac=''))
    assert rv.status_code == 404


def test_valid_missing_tac(flask_app, api_version):
    """Test Depot ID 96558/6.

    Verify that TAC API should return a 200 status for valid
    tacs that are not in GSMA, but the GSMA field should be null.
    """
    missing_tac = '12345678'
    rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac='12345678'))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['tac'] == missing_tac
    assert data['gsma'] is None


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
    rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac=valid_tac))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['tac'] == valid_tac
    gsma_data = data['gsma']
    assert gsma_data is not None
    print(gsma_data)
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


def test_method_put_not_allowed(flask_app, api_version):
    """Test Depot ID 96554/2.

    Verify the TAC API does not support HTTP PUT and returns HTTP 405 METHOD NOT ALLOWED.
    """
    rv = flask_app.put(url_for('{0}.tac_api'.format(api_version), tac='35567907'))
    assert rv.status_code == 405
    assert b'Method Not Allowed' in rv.data


def test_method_post_not_allowed(flask_app, api_version):
    """Test Depot ID 96555/3.

    Verify the TAC API does not support HTTP POST and returns HTTP 405 METHOD NOT ALLOWED.
    """
    rv = flask_app.post(url_for('{0}.tac_api'.format(api_version), tac='35567907'))
    assert rv.status_code == 405
    assert b'Method Not Allowed' in rv.data


def test_method_delete_not_allowed(flask_app, api_version):
    """Test Depot ID 96556/3.

    Verify the TAC API does not support HTTP DELETE and returns HTTP 405 METHOD NOT ALLOWED.
    """
    rv = flask_app.delete(url_for('{0}.tac_api'.format(api_version), tac='35567907'))
    assert rv.status_code == 405
    assert b'Method Not Allowed' in rv.data
