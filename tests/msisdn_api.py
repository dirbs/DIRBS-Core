"""
MSISDN API unit tests.

Copyright (c) 2018 Qualcomm Technologies, Inc.

 All rights reserved.



 Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
 limitations in the disclaimer below) provided that the following conditions are met:


 * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 disclaimer.

 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 disclaimer in the documentation and/or other materials provided with the distribution.

 * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
 products derived from this software without specific prior written permission.

 NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
 THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 POSSIBILITY OF SUCH DAMAGE.

"""

import json

from flask import url_for
import pytest

from _fixtures import *    # noqa: F403, F401
from _importer_params import GSMADataParams, OperatorDataParams


def test_msisdn_too_long(flask_app, api_version):
    """Test Depot ID not know yet.

    Verify that MSISDN API should validate that
    a supplied MSISDN is less than or equal to 15 chars and
    return an HTTP 400 error code if not.
    """
    for i in ['1', '123456', '12345678901234', '123456789012345']:
        rv = flask_app.get(url_for('{0}.msisdn_api'.format(api_version), msisdn=i))
        assert rv.status_code == 200
    for i in ['1234567890123456789', '123456789012345678901']:
        rv = flask_app.get(url_for('{0}.msisdn_api'.format(api_version), msisdn=i))
        assert rv.status_code == 400
        assert b'Bad MSISDN format (too long)' in rv.data


def test_msisdn_chars(flask_app, api_version):
    """Test Depot ID not know yet.

    Verify that MSISDN API should validate that
    a supplied MSISDN contains only numbers.
    """
    for i in ['A123456AAAAA', '12345678901234A', '*1234567890123A']:
        rv = flask_app.get(url_for('{0}.msisdn_api'.format(api_version), msisdn=i))
        assert rv.status_code == 400
        assert b'Bad MSISDN format (can only contain digit characters)' in rv.data


def test_empty_msisdn(flask_app, api_version):
    """Test Depot ID not known yet.

    Verify that MSISDN API should return a 404 status for a zero-length MSISDN.
    """
    """ MSISDN API should return a 404 status for a zero-length MSISDN """
    rv = flask_app.get(url_for('{0}.msisdn_api'.format(api_version), msisdn=''))
    assert rv.status_code == 404


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                                             extract=False,
                                             perform_unclean_checks=False,
                                             perform_region_checks=False,
                                             perform_home_network_check=False)],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(
                             filename='testData1-gsmatac_operator1_operator4_anonymized.txt')],
                         indirect=True)
def test_observed_msisdn(flask_app, operator_data_importer, gsma_tac_db_importer,
                         db_conn, tmpdir, logger, api_version):
    """Test Depot ID not known yet.

    Verify MSISDN API should return IMSI, GSMA Manufacturer, GSMA Model Name fot the current MSISDN.
    """
    # operator input file contains imei_norm = 38847733370026 with msisdn=22300049781840
    # gsma input file contains tac 38847733 with manufacturer = 1d4e632daf5249ba6f4165cca4cb4ff5025ddae6
    operator_data_importer.import_data()
    gsma_tac_db_importer.import_data()
    imei_norm = '38847733370026'
    msisdn = '22300049781840'
    imsi = '11104803062043'
    gsma_manufacturer = '1d4e632daf5249ba6f4165cca4cb4ff5025ddae6'
    gsma_model_name = 'ef12302c27d9b8a5a002918bd643dcd412d2db66'
    rv = flask_app.get(url_for('{0}.msisdn_api'.format(api_version), msisdn=msisdn))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))[0]
    assert data['imei_norm'] == imei_norm
    assert data['imsi'] == imsi
    assert data['gsma_manufacturer'] == gsma_manufacturer
    assert data['gsma_model_name'] == gsma_model_name
    return data


def test_put_not_allowed(flask_app, db_conn, tmpdir, logger, api_version):
    """Test Depot ID not known yet.

    Verify the MSISDN API does not support HTTP PUT and returns HTTP 405 METHOD NOT ALLOWED.
    """
    for i in ['3884773337002633']:
        rv = flask_app.put(url_for('{0}.msisdn_api'.format(api_version), msisdn=i))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data


def test_post_not_allowed(flask_app, db_conn, tmpdir, logger, api_version):
    """Test Depot ID not known yet.

    Verify the MSISDN API does not support HTTP POST and returns HTTP 405 METHOD NOT ALLOWED.
    """
    for i in ['3884773337002633']:
        rv = flask_app.post(url_for('{0}.msisdn_api'.format(api_version), msisdn=i))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data


def test_delete_not_allowed(flask_app, db_conn, tmpdir, logger, api_version):
    """Test Depot ID not known yet.

    Verify the MSISDN API does not support HTTP DELETE and returns HTTP 405 METHOD NOT ALLOWED.
    """
    for i in ['3884773337002633']:
        rv = flask_app.delete(url_for('{0}.msisdn_api'.format(api_version), msisdn=i))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data


def test_response_headers(flask_app, api_version):
    """Verify the security headers are set properly on returned response."""
    rv = flask_app.get(url_for('{0}.msisdn_api'.format(api_version), msisdn='123456789012345'))
    assert rv.status_code == 200
    assert rv.headers.get('X-Frame-Options') == 'DENY'
    assert rv.headers.get('X-Content-Type-Options') == 'nosniff'
