"""
Catalog API unit tests.

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
from os import path
from datetime import datetime
import zipfile

from flask import url_for
from click.testing import CliRunner

from _fixtures import *  # noqa: F403, F401
from dirbs.cli.catalog import CatalogAttributes
from dirbs.cli.importer import cli as dirbs_import_cli
from dirbs.cli.catalog import cli as dirbs_catalog_cli
from _helpers import job_metadata_importer
from dirbs.config import CatalogConfig


def _populate_data_catalog_table(conn, filename, file_type, size, modified_time, is_valid_zip,
                                 is_valid_format, md5, extra_attributes=None, first_seen=None, last_seen=None):
    """Helper function to insert dummy data in to the data_catalog table."""
    if extra_attributes is None:
        extra_attributes = {}
    if first_seen is None:
        first_seen = datetime.now()
    if last_seen is None:
        last_seen = datetime.now()
    with conn.cursor() as cursor:
        cursor.execute("""INSERT INTO data_catalog(filename, file_type, compressed_size_bytes, modified_time,
                                      is_valid_zip, is_valid_format, md5, extra_attributes, first_seen, last_seen)
                               VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                       [filename, file_type, size, modified_time, is_valid_zip,
                        is_valid_format, md5, json.dumps(extra_attributes), first_seen, last_seen])
        conn.commit()


def _dummy_data_generator(conn):
    """Helper function to generate sample data for the unit tests."""
    data_files = []
    f = CatalogAttributes('operator_file.zip', 'operator', '2017-01-01 00:00:00', 46445454332,
                          True, True, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', {'filename_check': True})
    data_files.append(f)
    f = CatalogAttributes('gsma_file.zip', 'gsma_tac', '2016-07-07 00:00:00', 2342344,
                          False, False, '40a37f83-21cb-4ab4-bba5-4032b1347273')
    data_files.append(f)
    f = CatalogAttributes('stolen_file.zip', 'stolen_list', '2016-09-09 00:00:00', 54543,
                          True, True, '014a3782-9826-4665-8830-534013b59cc5')
    data_files.append(f)

    for f in data_files:
        _populate_data_catalog_table(conn, f.filename, f.file_type, f.compressed_size_bytes, f.modified_time,
                                     f.is_valid_zip, f.is_valid_format, f.md5, f.extra_attributes,
                                     first_seen='2017-10-31 00:00:00', last_seen='2017-10-31 00:00:00')

    f = CatalogAttributes('pairing_file.zip', 'pairing_list', '2016-08-08 00:00:00', 1564624,
                          True, False, 'd0481db2-bdc8-43da-a69e-ea7006bd7a7c')
    _populate_data_catalog_table(conn, f.filename, f.file_type, f.compressed_size_bytes, f.modified_time,
                                 f.is_valid_zip, f.is_valid_format, f.md5, f.extra_attributes,
                                 first_seen='2017-12-12 00:00:00', last_seen='2017-12-12 00:00:00')


def test_invalid_max_results(flask_app):
    """Verify the API returns 400 for non-numeric max_results argument."""
    rv = flask_app.get(url_for('v1.catalog_api', max_results='abc'))
    assert rv.status_code == 400
    assert b"Bad \'max_results\':\'[\'Not a valid integer.\']\' argument format. Accepts only integer" in rv.data


def test_invalid_file_type(flask_app, api_version):
    """Verify the API returns 400 for invalid file_type argument."""
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), file_type='invalid_file'))
        assert rv.status_code == 400
        assert b"Bad \'file_type\':\'[\'Not a valid choice.\']\' argument format. Accepts only one of " \
               b"[\'operator\', \'gsma_tac\', \'stolen_list\', \'pairing_list\', \'registration_list\', " \
               b"\'golden_list\']" in rv.data in rv.data
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), file_type='invalid_file'))
        assert rv.status_code == 400
        assert b"Bad \'file_type\':\'[\'Not a valid choice.\']\' argument format. Accepts only one of " \
               b"[\'operator\', \'gsma_tac\', \'stolen_list\', \'pairing_list\', \'registration_list\', " \
               b"\'golden_list\']" in rv.data


def test_invalid_is_valid_zip(flask_app, api_version):
    """Verify the API returns 400 for non-boolean is_valid_zip argument."""
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), is_valid_zip='10'))
        assert rv.status_code == 400
        assert b"[\'Not a valid boolean.\']\' argument format. Accepts only one of [\'0\', \'1\', \'true\', " \
               b"\'false\']" in rv.data
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), is_valid_zip='10'))
        assert rv.status_code == 400
        assert b"[\'Not a valid boolean.\']\' argument format. Accepts only one of [\'0\', \'1\', \'true\', " \
               b"\'false\']" in rv.data


def test_invalid_modified_since(flask_app, api_version):
    """Verify the API returns 400 for invalid date in modified_since argument."""
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), modified_since='abc'))
        assert rv.status_code == 400
        assert b"Bad \'modified_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), modified_since='2016-01-01'))
        assert rv.status_code == 400
        assert b"Bad \'modified_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), modified_since='20170101 00:00:00'))
        assert rv.status_code == 400
        assert b"Bad \'modified_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), modified_since='20161313'))
        assert rv.status_code == 400
        assert b"Bad \'modified_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), modified_since='abc'))
        assert rv.status_code == 400
        assert b"Bad \'modified_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), modified_since='2016-01-01'))
        assert rv.status_code == 400
        assert b"Bad \'modified_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), modified_since='20170101 00:00:00'))
        assert rv.status_code == 400
        assert b"Bad \'modified_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), modified_since='20161313'))
        assert rv.status_code == 400
        assert b"Bad \'modified_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data


def test_invalid_cataloged_since(flask_app, api_version):
    """Verify the API returns 400 for invalid date in cataloged_since argument."""
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), cataloged_since='abc'))
        assert rv.status_code == 400
        assert b"Bad \'cataloged_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), cataloged_since='2016-01-01'))
        assert rv.status_code == 400
        assert b"Bad \'cataloged_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), cataloged_since='20170101 00:00:00'))
        assert rv.status_code == 400
        assert b"Bad \'cataloged_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), cataloged_since='20161313'))
        assert rv.status_code == 400
        assert b"Bad \'cataloged_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), cataloged_since='abc'))
        assert rv.status_code == 400
        assert b"Bad \'cataloged_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), cataloged_since='2016-01-01'))
        assert rv.status_code == 400
        assert b"Bad \'cataloged_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), cataloged_since='20170101 00:00:00'))
        assert rv.status_code == 400
        assert b"Bad \'cataloged_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), cataloged_since='20161313'))
        assert rv.status_code == 400
        assert b"Bad \'cataloged_since\':\'[\'Not a valid datetime.\']\' argument format" in rv.data


def test_valid_max_results(flask_app, db_conn):
    """Verify the API (version 1.0) returns 200 and valid JSON containing appropriate number of items.

    Items returned are less than or equal to the max_results argument specified.
    """
    _dummy_data_generator(db_conn)
    rv = flask_app.get(url_for('v1.catalog_api', max_results=2))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert len(data) == 2
    rv = flask_app.get(url_for('v1.catalog_api', max_resultsZ=200))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert len(data) == 4


def test_valid_file_type(flask_app, db_conn, api_version):
    """Verify the API returns 200 and valid JSON containing appropriate files.

    The files should belong to the file_type specified in the argument.
    """
    _dummy_data_generator(db_conn)
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), file_type='gsma_tac'))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert len(data) == 1
        assert data[0]['filename'] == 'gsma_file.zip'
        assert data[0]['file_type'] == 'gsma_tac'
        assert data[0]['compressed_size_bytes'] == 2342344
        assert not data[0]['is_valid_zip']
        assert not data[0]['is_valid_format']
        assert data[0]['md5'] == '40a37f83-21cb-4ab4-bba5-4032b1347273'
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), file_type='gsma_tac'))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['files']
        assert len(data) == 1
        assert data[0]['filename'] == 'gsma_file.zip'
        assert data[0]['file_type'] == 'gsma_tac'
        assert data[0]['compressed_size_bytes'] == 2342344
        assert not data[0]['is_valid_zip']
        assert not data[0]['is_valid_format']
        assert data[0]['md5'] == '40a37f83-21cb-4ab4-bba5-4032b1347273'


def test_valid_is_valid_zip(flask_app, db_conn, api_version):
    """Verify the API returns 200 and valid JSON containing only valid zips."""
    _dummy_data_generator(db_conn)
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), is_valid_zip=False))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert len(data) == 1
        assert data[0]['filename'] == 'gsma_file.zip'
        assert data[0]['file_type'] == 'gsma_tac'
        assert data[0]['compressed_size_bytes'] == 2342344
        assert not data[0]['is_valid_zip']
        assert not data[0]['is_valid_format']
        assert data[0]['md5'] == '40a37f83-21cb-4ab4-bba5-4032b1347273'
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), is_valid_zip=True))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert len(data) == 3
        assert b'40a37f83-21cb-4ab4-bba5-4032b1347273' not in rv.data
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), is_valid_zip=False))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['files']
        assert len(data) == 1
        assert data[0]['filename'] == 'gsma_file.zip'
        assert data[0]['file_type'] == 'gsma_tac'
        assert data[0]['compressed_size_bytes'] == 2342344
        assert not data[0]['is_valid_zip']
        assert not data[0]['is_valid_format']
        assert data[0]['md5'] == '40a37f83-21cb-4ab4-bba5-4032b1347273'
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), is_valid_zip=True))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['files']
        assert len(data) == 3
        assert b'40a37f83-21cb-4ab4-bba5-4032b1347273' not in rv.data


def test_valid_modified_since(flask_app, db_conn, api_version):
    """Verify the API returns 200 and valid JSON containing appropriate files.

    The files returned should have modified time before the specified time.
    """
    _dummy_data_generator(db_conn)
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), modified_since='20170101'))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert len(data) == 1
        assert data[0]['filename'] == 'operator_file.zip'
        assert data[0]['file_type'] == 'operator'
        assert data[0]['compressed_size_bytes'] == 46445454332
        assert data[0]['is_valid_zip']
        assert data[0]['is_valid_format']
        assert data[0]['md5'] == 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), modified_since='20170101'))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['files']
        assert len(data) == 1
        assert data[0]['filename'] == 'operator_file.zip'
        assert data[0]['file_type'] == 'operator'
        assert data[0]['compressed_size_bytes'] == 46445454332
        assert data[0]['is_valid_zip']
        assert data[0]['is_valid_format']
        assert data[0]['md5'] == 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'


def test_valid_cataloged_since(flask_app, db_conn, api_version):
    """Verify the API returns 200 and valid JSON containing appropriate files.

    The files returned should have last_seen time before the specified time.
    """
    _dummy_data_generator(db_conn)
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), cataloged_since='20171101'))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert len(data) == 1
        assert data[0]['filename'] == 'pairing_file.zip'
        assert data[0]['file_type'] == 'pairing_list'
        assert data[0]['compressed_size_bytes'] == 1564624
        assert data[0]['is_valid_zip']
        assert not data[0]['is_valid_format']
        assert data[0]['md5'] == 'd0481db2-bdc8-43da-a69e-ea7006bd7a7c'
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), cataloged_since='20171101'))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['files']
        assert len(data) == 1
        assert data[0]['filename'] == 'pairing_file.zip'
        assert data[0]['file_type'] == 'pairing_list'
        assert data[0]['compressed_size_bytes'] == 1564624
        assert data[0]['is_valid_zip']
        assert not data[0]['is_valid_format']
        assert data[0]['md5'] == 'd0481db2-bdc8-43da-a69e-ea7006bd7a7c'


def test_api_with_no_arguments(flask_app, db_conn, api_version):
    """Verify the API returns 200 and all the specified files."""
    _dummy_data_generator(db_conn)
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version)))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert len(data) == 4
        assert b'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' in rv.data
        assert b'40a37f83-21cb-4ab4-bba5-4032b1347273' in rv.data
        assert b'014a3782-9826-4665-8830-534013b59cc5' in rv.data
        assert b'd0481db2-bdc8-43da-a69e-ea7006bd7a7c' in rv.data
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version)))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['files']
        assert len(data) == 4
        assert b'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' in rv.data
        assert b'40a37f83-21cb-4ab4-bba5-4032b1347273' in rv.data
        assert b'014a3782-9826-4665-8830-534013b59cc5' in rv.data
        assert b'd0481db2-bdc8-43da-a69e-ea7006bd7a7c' in rv.data


def test_api_with_multiple_arguments(flask_app, db_conn, api_version):
    """Verify the API returns 200 and valid JSON containing the appropriate files.

    The files returned should satisfy all the specified arguments.
    """
    _dummy_data_generator(db_conn)
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version), modified_since='20160901',
                                   is_valid_zip=True))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert len(data) == 2
        assert data[0]['filename'] == 'stolen_file.zip'
        assert data[0]['file_type'] == 'stolen_list'
        assert data[0]['compressed_size_bytes'] == 54543
        assert data[0]['is_valid_zip']
        assert data[0]['is_valid_format']
        assert data[0]['md5'] == '014a3782-9826-4665-8830-534013b59cc5'
        assert data[1]['filename'] == 'operator_file.zip'
        assert data[1]['file_type'] == 'operator'
        assert data[1]['compressed_size_bytes'] == 46445454332
        assert data[1]['is_valid_zip']
        assert data[1]['is_valid_format']
        assert data[1]['md5'] == 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version), modified_since='20160901',
                                   is_valid_zip=True))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['files']
        assert len(data) == 2


def test_put_not_allowed(flask_app, db_conn, tmpdir, logger, api_version):
    """Verify the Catalog API does not support HTTP PUT and returns HTTP 405 METHOD NOT ALLOWED."""
    if api_version == 'v1':
        rv = flask_app.put(url_for('{0}.catalog_api'.format(api_version), file_type='operator'))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data
    else:  # api version 2.0
        rv = flask_app.put(url_for('{0}.catalog_get_api'.format(api_version), file_type='operator'))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data


def test_post_not_allowed(flask_app, db_conn, tmpdir, logger, api_version):
    """Verify the Catalog API does not support HTTP POST and returns HTTP 405 METHOD NOT ALLOWED."""
    if api_version == 'v1':
        rv = flask_app.post(url_for('{0}.catalog_api'.format(api_version), file_type='operator'))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data
    else:  # api version 2.0
        rv = flask_app.post(url_for('{0}.catalog_get_api'.format(api_version), file_type='operator'))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data


def test_delete_not_allowed(flask_app, db_conn, tmpdir, logger, api_version):
    """Verify the Catalog API does not support HTTP DELETE and returns HTTP 405 METHOD NOT ALLOWED."""
    if api_version == 'v1':
        rv = flask_app.delete(url_for('{0}.catalog_api'.format(api_version), file_type='operator'))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data
    else:  # api version 2.0
        rv = flask_app.delete(url_for('{0}.catalog_get_api'.format(api_version), file_type='operator'))
        assert rv.status_code == 405
        assert b'The method is not allowed for the requested URL' in rv.data


def test_import_status(db_conn, mocked_config, tmpdir, monkeypatch, flask_app, api_version,
                       logger, mocked_statsd, metadata_db_conn):
    """Test import status info in catalog api.

    - import_status:
        - ever_imported_successfully: true or false
        - most_recent_import: status
    Generate an MD5 hash of the file during import and store it in job_metadata.
    Then, when cataloging, look at the most recent import job in the job_metadata table where
    the file had the same MD5 hash and lookup the status.
    ever_imported_successfully will be true if there is any successfull import - joining on files md5
    most_recent_import returns the status of the most recent import - joining on files md5
    """
    # Step 1
    # try to import something successfully to get most_recent_import = success
    # and test the md5 created in the abstract importer using dirbs-import cli command
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/operator')
    valid_csv_operator_data_file_name = 'operator1_20160701_20160731.csv'
    valid_csv_operator_data_file = path.join(data_dir, valid_csv_operator_data_file_name)

    # create a zip file inside a temp dir
    valid_zip_operator_data_file_path = \
        str(tmpdir.join('operator1_20160701_20160731.zip'))
    with zipfile.ZipFile(valid_zip_operator_data_file_path, 'w') as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is the
        # archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_operator_data_file, valid_csv_operator_data_file_name)

    runner = CliRunner()
    result = runner.invoke(dirbs_import_cli, ['operator', 'Operator1', '--disable-rat-import',
                                              '--disable-region-check', '--disable-home-check',
                                              valid_zip_operator_data_file_path],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    catalog_config_dict = {
        'prospectors': [
            {
                'file_type': 'operator',
                'paths': [valid_zip_operator_data_file_path],
                'schema_filename': 'OperatorImportSchema_v2.csvs'
            }
        ],
        'perform_prevalidation': False
    }

    catalog_config = CatalogConfig(ignore_env=True, **catalog_config_dict)
    monkeypatch.setattr(mocked_config, 'catalog_config', catalog_config)
    # Run dirbs-catalog using db args from the temp postgres instance
    runner = CliRunner()
    result = runner.invoke(dirbs_catalog_cli, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # call apis
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version)))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data[0]['import_status']['most_recent_import'] == 'success'
        assert data[0]['import_status']['ever_imported_successfully'] is True

        with db_conn.cursor() as cursor:
            cursor.execute('SELECT md5 FROM data_catalog')
            md5 = cursor.fetchone().md5

        # Step 2
        with db_conn.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE job_metadata')

        # status error
        job_metadata_importer(db_conn=db_conn, command='dirbs-import',
                              run_id=10, subcommand='operator',
                              status='error',
                              start_time='2017-08-15 01:15:39.54785+00',
                              extra_metadata={'input_file_md5': md5})

        # status in progress, most recent
        job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=11, subcommand='operator',
                              status='running',
                              start_time='2017-08-15 01:15:40.54785+00',
                              extra_metadata={'input_file_md5': md5})

        # call API
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version)))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data[0]['import_status']['most_recent_import'] == 'running'
        assert data[0]['import_status']['ever_imported_successfully'] is False
        assert len(data) == 1

        # Step 3 try a different order
        with db_conn.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE job_metadata')

        job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=13, subcommand='gsma', status='success',
                              start_time='2017-08-15 01:15:39.54785+00',
                              extra_metadata={'input_file_md5': md5})

        # status in progress, most recent
        job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=14, subcommand='gsma', status='error',
                              start_time='2017-08-15 01:15:40.54785+00',
                              extra_metadata={'input_file_md5': md5})

        # call API
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version)))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data[0]['import_status']['most_recent_import'] == 'error'
        assert data[0]['import_status']['ever_imported_successfully'] is True
        assert len(data) == 1
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version)))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['files']
        assert data[0]['import_status']['most_recent_import'] == 'success'
        assert data[0]['import_status']['ever_imported_successfully'] is True

        with db_conn.cursor() as cursor:
            cursor.execute('SELECT md5 FROM data_catalog')
            md5 = cursor.fetchone().md5

        # Step 2
        with db_conn.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE job_metadata')

        # status error
        job_metadata_importer(db_conn=db_conn, command='dirbs-import',
                              run_id=10, subcommand='operator',
                              status='error',
                              start_time='2017-08-15 01:15:39.54785+00',
                              extra_metadata={'input_file_md5': md5})

        # status in progress, most recent
        job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=11, subcommand='operator',
                              status='running',
                              start_time='2017-08-15 01:15:40.54785+00',
                              extra_metadata={'input_file_md5': md5})

        # call API
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version)))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['files']
        assert data[0]['import_status']['most_recent_import'] == 'running'
        assert data[0]['import_status']['ever_imported_successfully'] is False
        assert len(data) == 1

        # Step 3 try a different order
        with db_conn.cursor() as cursor:
            cursor.execute('TRUNCATE TABLE job_metadata')

        job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=13, subcommand='gsma', status='success',
                              start_time='2017-08-15 01:15:39.54785+00',
                              extra_metadata={'input_file_md5': md5})

        # status in progress, most recent
        job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=14, subcommand='gsma', status='error',
                              start_time='2017-08-15 01:15:40.54785+00',
                              extra_metadata={'input_file_md5': md5})

        # call API
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version)))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['files']
        assert data[0]['import_status']['most_recent_import'] == 'error'
        assert data[0]['import_status']['ever_imported_successfully'] is True
        assert len(data) == 1


def test_num_records_uncompressed_size(mocked_config, tmpdir, monkeypatch,
                                       flask_app, api_version):
    """Test import status info in catalog api.

    - num_records: the number of lines in the file minus the header.
    - uncompressed_size_bytes.
    """
    # import operator status success
    here = path.abspath(path.dirname(__file__))
    data_dir = path.join(here, 'unittest_data/operator')
    valid_csv_operator_data_file_name = 'operator1_20160701_20160731.csv'
    valid_csv_operator_data_file = path.join(data_dir, valid_csv_operator_data_file_name)

    # create a zip file inside a temp dir
    valid_zip_operator_data_file_path = \
        str(tmpdir.join('operator1_20160701_20160731.zip'))
    with zipfile.ZipFile(valid_zip_operator_data_file_path, 'w',
                         compression=zipfile.ZIP_DEFLATED) as valid_csv_operator_data_file_zfile:
        # zipfile write() method supports an extra argument (arcname) which is the
        # archive name to be stored in the zip file.
        valid_csv_operator_data_file_zfile.write(valid_csv_operator_data_file, valid_csv_operator_data_file_name)

    catalog_config_dict = {
        'prospectors': [
            {
                'file_type': 'operator',
                'paths': [valid_zip_operator_data_file_path],
                'schema_filename': 'OperatorImportSchema_v2.csvs'
            }
        ],
        'perform_prevalidation': False
    }

    catalog_config = CatalogConfig(ignore_env=True, **catalog_config_dict)
    monkeypatch.setattr(mocked_config, 'catalog_config', catalog_config)
    # Run dirbs-catalog using db args from the temp postgres instance
    runner = CliRunner()
    result = runner.invoke(dirbs_catalog_cli, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # call APIs
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.catalog_api'.format(api_version)))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data[0]['filename'] == 'operator1_20160701_20160731.zip'
        assert data[0]['num_records'] == 20
        assert data[0]['uncompressed_size_bytes'] == 1066
        assert data[0]['compressed_size_bytes'] == 400
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.catalog_get_api'.format(api_version)))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['files']
        assert data[0]['filename'] == 'operator1_20160701_20160731.zip'
        assert data[0]['num_records'] == 20
        assert data[0]['uncompressed_size_bytes'] == 1066
        assert data[0]['compressed_size_bytes'] == 400


def test_catalog_pagination(flask_app, db_conn):
    """Verify pagination support on Catalog API (version 2.0)."""
    # populate data
    _dummy_data_generator(db_conn)

    # API call, starting from first result & 4 results per page
    offset = 1
    limit = 1
    rv = flask_app.get(url_for('v2.catalog_get_api', offset=offset, limit=limit))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    keys = data['_keys']
    files = data['files']

    assert keys['result_size'] == 5
    assert keys['current_key'] == '1'
    assert keys['next_key'] == str(offset + limit)
    assert len(files) == limit

    # 2nd call, offset=2, limit=2
    offset = offset + limit
    limit = 2
    rv = flask_app.get(url_for('v2.catalog_get_api', offset=offset, limit=limit))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    keys = data['_keys']
    files = data['files']
    assert keys['result_size'] == 5
    assert keys['next_key'] == str(offset + limit)
    assert len(files) == limit
