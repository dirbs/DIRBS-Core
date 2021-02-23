"""
version_metadata api data import unit tests.

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

import pytest
from flask import url_for

from dirbs import db_schema_version as code_db_schema_version
from dirbs import __version__ as dirbs_core_version
from dirbs import report_schema_version
from _importer_params import GSMADataParams
from _fixtures import *    # noqa: F403, F401


def test_version_json_api(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that version_metadata returns a JSON containing db metadata such as:
    schema_version, code_version, report_stats_schema_version.
    """
    with db_conn.cursor() as cursor:
        cursor.execute("""SELECT * FROM schema_metadata """)
        schema_metadata = cursor.fetchone()
        schema_version = schema_metadata.version

    rv = flask_app.get(url_for('{0}.version_api'.format(api_version)))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['db_schema_version'] == schema_version
    assert data['code_db_schema_version'] == code_db_schema_version

    if api_version == 'v2':
        assert data['source_code_version'] == dirbs_core_version
        assert data['report_schema_version'] == report_schema_version


def test_method_delete_not_allowed(flask_app, api_version):
    """Test Depot ID not known yet.

    Verify the version API does not support HTTP DELETE and returns HTTP 405 METHOD NOT ALLOWED.
    """
    rv = flask_app.delete(url_for('{0}.version_api'.format(api_version)))
    assert rv.status_code == 405
    assert b'Method Not Allowed' in rv.data


def test_method_post_not_allowed(flask_app, api_version):
    """Test Depot ID not known yet.

    Verify the version API does not support HTTP POST and returns HTTP 405 METHOD NOT ALLOWED.
    """
    rv = flask_app.delete(url_for('{0}.version_api'.format(api_version)))
    assert rv.status_code == 405
    assert b'Method Not Allowed' in rv.data


def test_method_put_not_allowed(flask_app, api_version):
    """Test Depot ID not known yet.

    Verify the version API does not support HTTP PUT and returns HTTP 405 METHOD NOT ALLOWED.
    """
    rv = flask_app.delete(url_for('{0}.version_api'.format(api_version)))
    assert rv.status_code == 405
    assert b'Method Not Allowed' in rv.data


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='sample_gsma_import_list_anonymized.txt',
                                         extract=False)],
                         indirect=True)
def test_exempted_device_types_raises_exception(flask_app, db_conn, api_version, monkeypatch,
                                                mocked_config, gsma_tac_db_importer):
    """Verify the exempted_device_type_config is validated for API requests."""
    gsma_tac_db_importer.import_data()
    db_conn.commit()

    # Verify an exception is thrown if an invalid device type is specified in config.
    monkeypatch.setattr(mocked_config.region_config, 'exempted_device_types', ['Vehicle', 'Car'])
    with pytest.raises(Exception):
        rv = flask_app.get(url_for('{0}.version_api'.format(api_version)))
        assert rv.status_code == 503

    # Verify API works for valid device types
    monkeypatch.setattr(mocked_config.region_config, 'exempted_device_types', ['Vehicle', 'Dongle'])
    rv = flask_app.get(url_for('{0}.version_api'.format(api_version)))
    assert rv.status_code == 200
