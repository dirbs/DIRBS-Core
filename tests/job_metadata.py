"""
job_metadata api data import unit tests.

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

from _fixtures import *  # noqa: F403, F401
from _helpers import job_metadata_importer
import dirbs.metadata as metadata


def test_classification_json_api(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing job metadata
    for classification job.
    """
    extra_metadata = {
        'matched_imei_counts':
            {
                'compound_dimension': 0,
                'simple_dimension': 0
            },
        'curr_date': None,
        'conditions': [
            {
                'dimensions': [{'module': 'gsma_not_found'}],
                'grace_period_days': 30,
                'sticky': False,
                'reason': 'Violated simple dimension',
                'max_allowed_matching_ratio': 0.1,
                'label': 'simple_dimension',
                'blocking': True
            },
            {
                'dimensions': [
                    {'module': 'stolen_list'},
                    {
                        'invert': True,
                        'parameters': {
                            'threshold': 3.1,
                            'period_days': 30
                        },
                        'module': 'duplicate_daily_avg'
                    }
                ],
                'grace_period_days': 0,
                'sticky': False,
                'reason': 'Violated compound dimension',
                'max_allowed_matching_ratio': 0.1,
                'label': 'compound_dimension',
                'blocking': True
            }
        ]  # noqa: E122
    }  # noqa E127

    job_metadata_importer(db_conn=db_conn, command='dirbs-classify', run_id=1, subcommand='', status='success',
                          extra_metadata=extra_metadata)

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=1,
                                   subcommand='',
                                   status='success',
                                   show_details=True))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-classify'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'success'
        assert data['extra_metadata'] == extra_metadata
    else:  # job_metadata api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=1,
                                   subcommand='',
                                   status='success',
                                   show_details=True))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        assert data['_keys']['result_size'] == 1
        assert data['_keys']['current_key'] == '0'
        assert data['_keys']['next_key'] == '10'
        assert data['jobs'][0]['command'] == 'dirbs-classify'
        assert data['jobs'][0]['run_id'] == 1
        assert data['jobs'][0]['subcommand'] == ''
        assert data['jobs'][0]['status'] == 'success'
        assert data['jobs'][0]['extra_metadata'] == extra_metadata


def test_prune_json_api(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing job metadata
    for pruning triplets and classification_state job.
    """
    extra_metadata = {'rows_before': 0,
                      'retention_months': 6,
                      'curr_date': None,
                      'rows_after': 0}

    job_metadata_importer(db_conn=db_conn, command='dirbs-prune',
                          run_id=9, subcommand='triplets', status='success',
                          extra_metadata=extra_metadata)

    job_metadata_importer(db_conn=db_conn, command='dirbs-prune', run_id=10, subcommand='classification_state',
                          status='success', extra_metadata=extra_metadata)

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   command='dirbs-prune',
                                   status='success',
                                   show_details=True))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        triplets_data = data[0]
        assert triplets_data['command'] == 'dirbs-prune'
        assert triplets_data['run_id'] == 9
        assert triplets_data['subcommand'] == 'triplets'
        assert triplets_data['status'] == 'success'
        assert triplets_data['extra_metadata'] == extra_metadata

        class_data = data[1]
        assert class_data['command'] == 'dirbs-prune'
        assert class_data['run_id'] == 10
        assert class_data['subcommand'] == 'classification_state'
        assert class_data['status'] == 'success'
        assert class_data['extra_metadata'] == extra_metadata
    else:  # job_metadata api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   command='dirbs-prune',
                                   status='success',
                                   show_details=True))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))
        triplets_data = data['jobs'][0]
        assert triplets_data['command'] == 'dirbs-prune'
        assert triplets_data['run_id'] == 9
        assert triplets_data['subcommand'] == 'triplets'
        assert triplets_data['status'] == 'success'
        assert triplets_data['extra_metadata'] == extra_metadata

        class_data = data['jobs'][1]
        assert class_data['command'] == 'dirbs-prune'
        assert class_data['run_id'] == 10
        assert class_data['subcommand'] == 'classification_state'
        assert class_data['status'] == 'success'
        assert class_data['extra_metadata'] == extra_metadata

        assert data['_keys']['result_size'] == 2
        assert data['_keys']['current_key'] == '0'
        assert data['_keys']['next_key'] == '10'


def test_operator_import_json_api(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing job metadata
    for importing operator job.
    """
    extra_metadata = {'performance_timing': {
        'init_staging_end': '2017-08-16T01:05:17.17081+00:00',
        'init_staging_start': '2017-08-16T01:05:16.817426+00:00',
        'extract_split_start': '2017-08-16T01:05:16.10788+00:00',
        'prevalidate_upload_start': '2017-08-16T01:05:17.34236+00:00',
        'analyze_staging_end': '2017-08-16T01:05:  20.807413+00:00',
        'validation_binary_checks_end': '2017-08-16T01:05:25.565519+00:00',
        'prevalidate_upload_end': '2017-08-16T01:05:20.125746+00:00',
        'analyze_staging_start': '2017-08-16T01:05:20.296765+00:00',
        'preprocess_start': '2017-08-16T01:05:16.474489+00:00',
        'extract_split_end': '2017-08-16T01:05:16.301238+00:00',
        'preprocess_end': '2017-08-16T01:05:16.645968+00:00',
        'postprocess_staging_end': '2017-08-16T01:05:24.531709+00:00',
        'validation_threshold_checks_start': '2017-08-16T01:05:25.741384+00:00',
        'validation_binary_checks_start': '2017-08-16T01:05:24.705607+00:00',
        'postprocess_staging_start': '2017-08-16T01:05:20.978153+00:00'
    },
        'home_threshold': 0.2,
        'cc': ['22%'],
        'clean_threshold': 0.05,
        'null_msisdn_threshold': 0.05,
        'perform_leading_zero_check': True,
        'perform_file_daterange_check': True,
        'perform_null_check': True,
        'perform_clean_check': True,
        'perform_historic_imsi_check': True,
        'perform_null_imsi_check': True,
        'perform_null_msisdn_check': True,
        'perform_historic_msisdn_check': True,
        'operator_id': 'operator1',
        'input_file': '/workspace/data/operator1_home_check_exceeded_20160701_20160731.zip',
        'batch_size': 1000000,
        'mcc_mnc_pairs': [{'mnc': '01', 'mcc': '111'}],
        'perform_historic_imei_check': True,
        'null_imsi_threshold': 0.05,
        'perform_rat_import': False,
        'perform_null_imei_check': True,
        'perform_home_check': True,
        'null_imei_threshold': 0.05,
        'region_threshold': 0.1,
        'perform_region_check': False
    }  # noqa E127

    job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=1, subcommand='operator',
                          status='error', extra_metadata=extra_metadata)

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version), show_details=False))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-import'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'operator'
        assert data['status'] == 'error'
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version), show_details=False))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        print(data['command'])
        assert data['command'] == 'dirbs-import'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'operator'
        assert data['status'] == 'error'


def test_stolen_import_json_api(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing job metadata
    for importing stolen_list job.
    """
    extra_metadata = {
        'output_stats': {
            'num_records_updated': 20,
            'num_records': 20,
            'num_records_inserted': 20
        },
        'performance_timing': {
            'init_staging_end': '2017-08-22T01:42:30.695313+00:00',
            'analyze_staging_end': '2017-08-22T01:42:34.286028+00:00',
            'validation_threshold_checks_end': '2017-08-22T01:42:36.380127+00:00',
            'analyze_staging_start': '2017-08-22T01:42:33.78045+00:00',
            'preprocess_start': '2017-08-22T01:42:30.023073+00:00',
            'copy_from_staging_end': '2017-08-22T01:42:38.553902+00:00',
            'validation_binary_checks_start': '2017-08-22T01:42:35.537445+00:00',
            'validation_threshold_checks_start': '2017-08-22T01:42:36.208775+00:00',
            'output_stats_start': '2017-08-22T01:42:38.721215+00:00',
            'validation_historical_checks_end': '2017-08-22T01:42:37.049421+00:00',
            'extract_split_end': '2017-08-22T01:42:29.855514+00:00',
            'copy_from_staging_start': '2017-08-22T01:42:37.38383+00:00',
            'extract_split_start': '2017-08-22T01:42:29.674068+00:00',
            'validation_historical_checks_start': '2017-08-22T01:42:36.547579+00:00',
            'preprocess_end': '2017-08-22T01:42:30.191182+00:00',
            'postprocess_staging_end': '2017-08-22T01:42:35.370151+00:00',
            'init_staging_start': '2017-08-22T01:42:30.358302+00:00',
            'validation_binary_checks_end': '2017-08-22T01:42:36.041237+00:00',
            'output_stats_end': '2017-08-22T01:42:39.225688+00:00',
            'prevalidate_upload_end': '2017-08-22T01:42:33.612194+00:00',
            'prevalidate_upload_start': '2017-08-22T01:42:30.862953+00:00',
            'postprocess_staging_start': '2017-08-22T01:42:34.458834+00:00'
        },
        'perform_historic_check': True,
        'input_file': '/workspace/data/sample_import_list.zip',
        'batch_size': 1000000,
        'input_stats': {
            'num_records_valid': 20,
            'num_records': 20,
            'num_records_invalid': 0
        }
    }  # noqa E127

    job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=1, subcommand='stolen_list',
                          status='success', extra_metadata=extra_metadata)

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version), command='dirbs-import'))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-import'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'stolen_list'
        assert data['status'] == 'success'
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version), command='dirbs-import'))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['command'] == 'dirbs-import'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'stolen_list'
        assert data['status'] == 'success'


def test_pairing_import_json_api(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing job metadata
    for importing pairing_list job.
    """
    extra_metadata = {'perform_duplicates_check': True,
                      'perform_historic_check': True,
                      'performance_timing':
                          {'init_staging_end': '2017-08-22T01:41:59.925562+00:00',
                           'init_staging_start': '2017-08-22T01:41:59.588253+00:00',
                           'extract_split_start': '2017-08-22T01:41:58.901343+00:00',
                           'prevalidate_upload_start': '2017-08-22T01:42:00.093237+00:00',
                           'analyze_staging_end': '2017-08-22T01:42:03.478264+00:00',
                           'prevalidate_upload_end': '2017-08-22T01:42:02.788264+00:00',
                           'analyze_staging_start': '2017-08-22T01:42:02.956404+00:00',
                           'preprocess_start': '2017-08-22T01:41:59.252764+00:00',
                           'extract_split_end': '2017-08-22T01:41:59.08492+00:00',
                           'preprocess_end': '2017-08-22T01:41:59.421052+00:00',
                           'postprocess_staging_end': '2017-08-22T01:42:04.520465+00:00',
                           'validation_binary_checks_start': '2017-08-22T01:42:04.68826+00:00',
                           'postprocess_staging_start': '2017-08-22T01:42:03.646232+00:00'},
                      'batch_size': 1000000,
                      'input_file':
                          '/workspace/data/duplicate.zip'}  # noqa E127

    job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=1, subcommand='pairing_list',
                          status='error', extra_metadata=extra_metadata)

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version)))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-import'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'pairing_list'
        assert data['status'] == 'error'
        assert data['extra_metadata'] == extra_metadata
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version)))
        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['command'] == 'dirbs-import'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'pairing_list'
        assert data['status'] == 'error'
        assert data['extra_metadata'] == extra_metadata


def test_gsma_import_json_api(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing job metadata
    for importing GSMA TAC data job.
    """
    extra_metadata = {
        'output_stats': {
            'num_records_updated': 4,
            'num_records': 4,
            'num_records_inserted': 4
        },
        'performance_timing': {
            'init_staging_end': '2017-08-22T01:56:25.875908+00:00',
            'analyze_staging_end': '2017-08-22T01:56:29.386537+00:00',
            'validation_threshold_checks_end': '2017-08-22T01:56:31.231756+00:00',
            'analyze_staging_start': '2017-08-22T01:56:28.886486+00:00',
            'preprocess_start': '2017-08-22T01:56:25.192466+00:00',
            'copy_from_staging_end': '2017-08-22T01:56:33.42097+00:00',
            'validation_binary_checks_start': '2017-08-22T01:56:30.725186+00:00',
            'validation_threshold_checks_start': '2017-08-22T01:56:31.063007+00:00',
            'output_stats_start': '2017-08-22T01:56:33.589227+00:00',
            'validation_historical_checks_end': '2017-08-22T01:56:31.915001+00:00',
            'extract_split_end': '2017-08-22T01:56:25.023654+00:00',
            'copy_from_staging_start': '2017-08-22T01:56:32.250857+00:00',
            'extract_split_start': '2017-08-22T01:56:24.844737+00:00',
            'validation_historical_checks_start': '2017-08-22T01:56:31.400242+00:00',
            'preprocess_end': '2017-08-22T01:56:25.368138+00:00',
            'postprocess_staging_end': '2017-08-22T01:56:30.557336+00:00',
            'init_staging_start': '2017-08-22T01:56:25.536523+00:00',
            'validation_binary_checks_end': '2017-08-22T01:56:30.895228+00:00',
            'output_stats_end': '2017-08-22T01:56:34.097277+00:00',
            'prevalidate_upload_end': '2017-08-22T01:56:28.718421+00:00',
            'prevalidate_upload_start': '2017-08-22T01:56:26.043878+00:00',
            'postprocess_staging_start': '2017-08-22T01:56:29.554878+00:00'
        },
        'perform_historic_check': True,
        'input_file': '/workspace/data/duplicate_gsma.zip',
        'batch_size': 1000000,
        'input_stats': {
            'num_records_valid': 4,
            'num_records': 7,
            'num_records_invalid': 3
        }
    }  # noqa E127

    job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=1, subcommand='gsma_tac',
                          status='success', extra_metadata=extra_metadata)

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version), show_details=False))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-import'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'gsma_tac'
        assert data['status'] == 'success'
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version), show_details=False))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['command'] == 'dirbs-import'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'gsma_tac'
        assert data['status'] == 'success'


def test_registration_import_json_api(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing job metadata
    for importing registration_list job.
    """
    extra_metadata = {'perform_duplicates_check': True,
                      'perform_historic_check': True,
                      'performance_timing':
                          {'init_staging_end': '2017-08-22T01:43:21.386498+00:00',
                           'init_staging_start': '2017-08-22T01:43:21.035571+00:00',
                           'extract_split_start': '2017-08-22T01:43:20.35253+00:00',
                           'prevalidate_upload_start': '2017-08-22T01:43:21.554073+00:00',
                           'preprocess_start': '2017-08-22T01:43:20.699411+00:00',
                           'extract_split_end': '2017-08-22T01:43:20.531135+00:00',
                           'preprocess_end': '2017-08-22T01:43:20.867795+00:00'},
                      'batch_size': 1000000,
                      'input_file':
                          '/workspace/data/'
                          'sample_import_list.zip'}  # noqa E127

    job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=1, subcommand='registration_list',
                          status='error', extra_metadata=extra_metadata)

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version), command='dirbs-import'))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-import'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'registration_list'
        assert data['status'] == 'error'
        assert data['extra_metadata'] == extra_metadata
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version), command='dirbs-import'))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['command'] == 'dirbs-import'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'registration_list'
        assert data['status'] == 'error'
        assert data['extra_metadata'] == extra_metadata


def test_golden_import_json_api(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing job metadata
    for importing golden_list job.
    """
    extra_metadata = {
        'performance_timing': {
            'init_staging_end': '2017-08-22T01:43:05.017337+00:00',
            'init_staging_start': '2017-08-22T01:43:04.681766+00:00',
            'extract_split_start': '2017-08-22T01:43:03.993331+00:00',
            'prevalidate_upload_start': '2017-08-22T01:43:05.18436+00:00',
            'preprocess_start': '2017-08-22T01:43:04.337401+00:00',
            'extract_split_end': '2017-08-22T01:43:04.17081+00:00',
            'preprocess_end': '2017-08-22T01:43:04.504815+00:00'
        },
        'perform_historic_check': True,
        'pre_hashed': False,
        'input_file': '/workspace/data/sample_import_list.zip',
        'batch_size': 1000000
    }  # noqa E127

    job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=1, subcommand='golden_list',
                          status='error', extra_metadata=extra_metadata)

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version), show_details=True))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-import'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'golden_list'
        assert data['status'] == 'error'
        assert data['extra_metadata'] == extra_metadata
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version), show_details=True))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['command'] == 'dirbs-import'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'golden_list'
        assert data['status'] == 'error'
        assert data['extra_metadata'] == extra_metadata


def test_db_schema_json_api(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing job metadata
    for db_schema.
    """
    job_metadata_importer(db_conn=db_conn, command='dirbs-db', run_id=1, subcommand='upgrade',
                          status='success')

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version), show_details=True))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-db'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'upgrade'
        assert data['status'] == 'success'
        assert data['extra_metadata'] == {}
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version), show_details=True))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['command'] == 'dirbs-db'
        assert data['run_id'] == 1
        assert data['subcommand'] == 'upgrade'
        assert data['status'] == 'success'
        assert data['extra_metadata'] == {}


def test_list_gen_schema_json_api(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing list generation metadata.
    """
    extra_metadata = {
        'blacklist': {
            'file_size_bytes': 25,
            'md5sum': 'd623e56b7c73d27fc7ce68e3dfc6e448',
            'num_records': 0,
            'filename': '/workspace/data/20170822_021142/blacklist.csv'
        },
        'notification_lists': [
            {
                'file_size_bytes': 37,
                'md5sum': '3ac7b8ae8722e47e1ce4b0a01fe8b1e2',
                'num_records': 0,
                'filename': '/workspace/data/20170822_021142/notifications_operator1.csv'
            },
            {
                'file_size_bytes': 37,
                'md5sum': '3ac7b8ae8722e47e1ce4b0a01fe8b1e2',
                'num_records': 0,
                'filename': '/workspace/data/20170822_021142/notifications_operator2.csv'
            },
            {
                'file_size_bytes': 37,
                'md5sum': '3ac7b8ae8722e47e1ce4b0a01fe8b1e2',
                'num_records': 0,
                'filename': '/workspace/data/20170822_021142/notifications_operator3.csv'
            },
            {
                'file_size_bytes': 37,
                'md5sum': '3ac7b8ae8722e47e1ce4b0a01fe8b1e2',
                'num_records': 0,
                'filename': '/workspace/data/20170822_021142/notifications_operator4.csv'
            }
        ],
        'curr_date': None,
        'exception_lists': [
            {
                'file_size_bytes': 11,
                'md5sum': 'b9a2f42722d13636dfb6c84e2ee765fe',
                'num_records': 0,
                'filename': '/workspace/data/20170822_021142/exceptions_operator1.csv'
            },
            {
                'file_size_bytes': 11,
                'md5sum': 'b9a2f42722d13636dfb6c84e2ee765fe',
                'num_records': 0,
                'filename': '/workspace/data/20170822_021142/exceptions_operator2.csv'
            },
            {
                'file_size_bytes': 11,
                'md5sum': 'b9a2f42722d13636dfb6c84e2ee765fe',
                'num_records': 0,
                'filename': '/workspace/data/20170822_021142/exceptions_operator3.csv'
            },
            {
                'file_size_bytes': 11,
                'md5sum': 'b9a2f42722d13636dfb6c84e2ee765fe',
                'num_records': 0,
                'filename': '/workspace/data/20170822_021142/exceptions_operator4.csv'
            }
        ],
        'blocking_conditions': [
            {
                'dimensions': [{'module': 'gsma_not_found'}],
                'grace_period_days': 30,
                'sticky': False,
                'reason': 'Violated simple dimension',
                'max_allowed_matching_ratio': 0.1,
                'label': 'simple_dimension',
                'blocking': True
            },
            {
                'dimensions': [
                    {'module': 'stolen_list'},
                    {
                        'invert': True,
                        'parameters': {
                            'threshold': 3.1,
                            'period_days': 30
                        },
                        'module': 'duplicate_daily_avg'
                    }
                ],
                'grace_period_days': 0,
                'sticky': False,
                'reason': 'Violated compound dimension',
                'max_allowed_matching_ratio': 0.1,
                'label': 'compound_dimension',
                'blocking': True
            }
        ]
    }  # noqa E127

    job_metadata_importer(db_conn=db_conn, command='dirbs-listgen', run_id=1, subcommand='',
                          status='success', extra_metadata=extra_metadata)

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   show_details=False))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-listgen'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'success'
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   show_details=False))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['command'] == 'dirbs-listgen'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'success'


def test_report_schema_json_api(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing report metadata.
    """
    extra_metadata = {'refreshed_data': True,
                      'month': 2,
                      'output_dir': '/workspace/data',
                      'year': 2016}

    job_metadata_importer(db_conn=db_conn, command='dirbs-report', run_id=1, subcommand='',
                          status='error', extra_metadata=extra_metadata)

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version)))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-report'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'error'
        assert data['extra_metadata'] == extra_metadata
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version)))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['command'] == 'dirbs-report'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'error'
        assert data['extra_metadata'] == extra_metadata


def test_job_metadata_bad_pos_int_params(flask_app, db_conn, api_version):
    """Test Depot ID unknown yet.

    Verify that job_metadata API returns a 400 status for not positive integer run_id or max_result,
    """
    if api_version == 'v1':
        # not numeric run_id
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id='aaa',
                                   status='success',
                                   show_details=False))

        assert rv.status_code == 400
        assert b"Bad \'run_id\':\'{0: [\'Not a valid integer.\']}\' argument format" in rv.data

        # not positive run_id
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=-1,
                                   status='success',
                                   show_details=False))

        assert rv.status_code == 400
        assert b"Bad \'run_id\':\'{0: [\'Must be at least 1.\']}\' argument format" in rv.data

        # not numeric max_result
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=1,
                                   status='success',
                                   max_results='a',
                                   show_details=False))

        assert rv.status_code == 400
        assert b"Bad \'max_results\':\'[\'Not a valid integer.\']\' argument format. Accepts only integer" in rv.data

        # not positive max_result
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=1,
                                   status='success',
                                   max_results=0,
                                   show_details=False))

        assert rv.status_code == 400
        assert b"Bad \'max_results\':\'[\'Must be at least 1.\']\' argument format. Accepts only integer" in rv.data

        # list of max_result (will take just the first elem of the list)
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=1,
                                   status='success',
                                   max_results=[1, -2],
                                   show_details=False))

        assert rv.status_code == 200

        # set max_result to 1 and check that only one record is returned
        job_metadata_importer(db_conn=db_conn, command='dirbs-classify', run_id=1, subcommand='sub_one',
                              status='success')

        job_metadata_importer(db_conn=db_conn, command='dirbs-classify', run_id=2, subcommand='sub_two',
                              status='success')

        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   run_id=1,
                                   db_user='test-user',
                                   subcommand=['sub_one', 'sub_two'],
                                   show_details=False,
                                   max_results=1))

        assert rv.status_code == 200
        assert len(json.loads(rv.data.decode('utf-8'))) == 1
    else:  # api version 2.0
        # not numeric run_id
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id='aaa',
                                   status='success',
                                   show_details=False))

        assert rv.status_code == 400
        assert b"Bad \'run_id\':\'{0: [\'Not a valid integer.\']}\' argument format" in rv.data

        # not positive run_id
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=-1,
                                   status='success',
                                   show_details=False))

        assert rv.status_code == 400
        assert b"Bad \'run_id\':\'{0: [\'Must be at least 1.\']}\' argument format" in rv.data

        # set max_result to 1 and check that only one record is returned
        job_metadata_importer(db_conn=db_conn, command='dirbs-classify', run_id=1, subcommand='sub_one',
                              status='success')

        job_metadata_importer(db_conn=db_conn, command='dirbs-classify', run_id=2, subcommand='sub_two',
                              status='success')

        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   run_id=1,
                                   db_user='test-user',
                                   subcommand=['sub_one', 'sub_two'],
                                   show_details=False,
                                   max_results=1))

        assert rv.status_code == 200
        assert len(json.loads(rv.data.decode('utf-8'))['jobs']) == 1


def test_job_metadata_bad_params(flask_app, api_version):
    """Test Depot ID unknown yet.

    Verify that job_metadata API returns a 400 status for unknown status or not boolean show_details.
    """
    if api_version == 'v1':
        # unknown status
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version), status='unknown'))
        assert rv.status_code == 400
        assert b"Bad \'status\':\'{0: [\'Not a valid choice.\']}\' argument format" in rv.data

        # list of status containing an unknown status
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version), status=['error', 'unknown']))
        assert rv.status_code == 400
        assert b"Bad \'status\':\'{1: [\'Not a valid choice.\']}\' argument format" in rv.data

        # not boolean show_details
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   show_details='not_boolean'))

        assert rv.status_code == 400
        assert b"Bad \'show_details\':\'[\'Not a valid boolean.\']\' argument format. " \
               b"Accepts only one of [\'0\', \'1\', \'true\', \'false\']" in rv.data
    else:  # api version 2.0
        # unknown status
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version), status='unknown'))
        assert rv.status_code == 400
        assert b"Bad \'status\':\'{0: [\'Not a valid choice.\']}\' argument format" in rv.data

        # list of status containing an unknown status
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version), status=['error', 'unknown']))
        assert rv.status_code == 400
        assert b"Bad \'status\':\'{1: [\'Not a valid choice.\']}\' argument format" in rv.data

        # not boolean show_details
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   show_details='not_boolean'))

        assert rv.status_code == 400
        assert b"Bad \'show_details\':\'[\'Not a valid boolean.\']\' argument format. " \
               b"Accepts only one of [\'0\', \'1\', \'true\', \'false\']" in rv.data


def test_json_show_details(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing job metadata
    with extra information if show_details is set to true.
    """
    extra_metadata = {
        'matched_imei_counts': {
            'compound_dimension': 0,
            'simple_dimension': 0
        },
        'conditions': [
            {
                'label': 'simple_dimension',
                'blocking': True,
                'sticky': False,
                'reason': 'Violated simple dimension',
                'max_allowed_matching_ratio': 0.1,
                'dimensions': [{'module': 'gsma_not_found'}],
                'grace_period_days': 30
            },
            {
                'label': 'compound_dimension',
                'blocking': True,
                'sticky': False,
                'reason': 'Violated compound dimension',
                'max_allowed_matching_ratio': 0.1,
                'dimensions': [
                    {'module': 'stolen_list'},
                    {
                        'invert': True,
                        'module': 'duplicate_daily_avg',
                        'parameters': {
                            'period_days': 30,
                            'threshold': 3.1
                        }
                    }
                ],
                'grace_period_days': 0
            }
        ],
        'curr_date': None
    }

    job_metadata_importer(db_conn=db_conn, command='dirbs-classify', run_id=1, subcommand='',
                          status='success', extra_metadata=extra_metadata)

    if api_version == 'v1':
        # Step 1 show_details=True
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=1,
                                   status='success',
                                   show_details=True))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-classify'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'success'
        assert data['extra_metadata'] == extra_metadata

        # Step 2 show_details=False
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=1,
                                   status='success',
                                   max_results=10,
                                   show_details=False))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-classify'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'success'
        assert 'extra_metadata' not in data
    else:  # api version 2.0
        # Step 1 show_details=True
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=1,
                                   status='success',
                                   show_details=True))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['command'] == 'dirbs-classify'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'success'
        assert data['extra_metadata'] == extra_metadata

        # Step 2 show_details=False
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=1,
                                   status='success',
                                   max_results=10,
                                   show_details=False))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['command'] == 'dirbs-classify'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'success'
        assert 'extra_metadata' not in data


def test_json_no_record_for_get_params(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata should return an empty JSON if params are well formatted
    but not stored in the job_metadata table.
    """
    job_metadata_importer(db_conn=db_conn, command='dirbs-classify', run_id=1, subcommand='',
                          status='success', extra_metadata={'metadata': 'metadata'})

    if api_version == 'v1':
        # Add row into job_metadata table with run_id=1 and get url for param run_id=2.
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=2,
                                   db_user='test-user',
                                   status='success',
                                   max_results=10,
                                   show_details=True))

        assert rv.status_code == 200
        assert json.loads(rv.data.decode('utf-8')) == []
    else:  # api version 2.0
        # Add row into job_metadata table with run_id=1 and get url for param run_id=2.
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=2,
                                   db_user='test-user',
                                   status='success',
                                   max_results=10,
                                   show_details=True))

        assert rv.status_code == 200
        assert json.loads(rv.data.decode('utf-8'))['jobs'] == []


def test_json_unknown_command_param(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata doesn't allow unknown command params.
    """
    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   command='dirbs-unknown',
                                   run_id=2,
                                   db_user='test-user',
                                   status='success',
                                   max_results=10,
                                   show_details=True))

        assert rv.status_code == 400
        assert b"Bad \'command\':\'{0: [\'Not a valid choice.\']}\' argument format" in rv.data
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   command='dirbs-unknown',
                                   run_id=2,
                                   db_user='test-user',
                                   status='success',
                                   show_details=True))

        assert rv.status_code == 400
        assert b"Bad \'command\':\'{0: [\'Not a valid choice.\']}\' argument format" in rv.data


def test_json_multiple_values_same_param(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing job metadata if get params
    consists of a list of values.
    """
    # Step 1 list of valid params: run_id=[1,2]; subcommand=['upgrade', 'operator']
    job_metadata_importer(db_conn=db_conn, command='dirbs-classify', run_id=1, subcommand='sub_one',
                          status='success')

    job_metadata_importer(db_conn=db_conn, command='dirbs-classify', run_id=2, subcommand='sub_two',
                          status='success')

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   run_id=[1, 2],
                                   db_user='test-user',
                                   subcommand=['sub_one', 'sub_two'],
                                   show_details=False))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['subcommand'] == 'sub_one'
        assert data['run_id'] == 1

        data = json.loads(rv.data.decode('utf-8'))[1]
        assert data['run_id'] == 2
        assert data['subcommand'] == 'sub_two'

        # Step 2 list with invalid params: run_id=[1,-2];
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=[1, -2],
                                   db_user='test-user',
                                   subcommand=['sub_one', 'sub_two'],
                                   status=['success', 'error'],
                                   max_results=10,
                                   show_details=False))

        assert rv.status_code == 400
        assert b"Bad \'run_id\':\'{1: [\'Must be at least 1.\']}\' argument format" in rv.data
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   run_id=[1, 2],
                                   db_user='test-user',
                                   subcommand=['sub_one', 'sub_two'],
                                   show_details=False))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['subcommand'] == 'sub_one'
        assert data['run_id'] == 1

        data = json.loads(rv.data.decode('utf-8'))['jobs'][1]
        assert data['run_id'] == 2
        assert data['subcommand'] == 'sub_two'

        # Step 2 list with invalid params: run_id=[1,-2];
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   command='dirbs-classify',
                                   run_id=[1, -2],
                                   db_user='test-user',
                                   subcommand=['sub_one', 'sub_two'],
                                   status=['success', 'error'],
                                   show_details=False))

        assert rv.status_code == 400
        assert b"Bad \'run_id\':\'{1: [\'Must be at least 1.\']}\' argument format" in rv.data


def test_json_no_run_id_param(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that if run_id is set to empty list, it will not be used to filter the results of the query.
    """
    job_metadata_importer(db_conn=db_conn, command='dirbs-classify', run_id=1, subcommand='',
                          status='success')

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version),
                                   run_id=[],
                                   show_details=False))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-classify'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'success'
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version),
                                   run_id=[],
                                   show_details=False))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['command'] == 'dirbs-classify'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'success'


def test_default_params(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify that job_metadata returns a JSON containing all job metadata
    if no request params are given.
    """
    job_metadata_importer(db_conn=db_conn, command='dirbs-classify', run_id=1, subcommand='',
                          status='success')

    if api_version == 'v1':
        rv = flask_app.get(url_for('{0}.job_metadata_api'.format(api_version)))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))[0]
        assert data['command'] == 'dirbs-classify'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'success'
        assert data['extra_metadata'] == {}
    else:  # api version 2.0
        rv = flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version)))

        assert rv.status_code == 200
        data = json.loads(rv.data.decode('utf-8'))['jobs'][0]
        assert data['command'] == 'dirbs-classify'
        assert data['run_id'] == 1
        assert data['subcommand'] == ''
        assert data['status'] == 'success'
        assert data['extra_metadata'] == {}


def test_method_delete_not_allowed(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify the job_metadata API does not support HTTP DELETE and returns HTTP 405 METHOD NOT ALLOWED.
    """
    if api_version == 'v1':
        rv = flask_app.delete(url_for('{0}.job_metadata_api'.format(api_version)))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data
    else:  # api version 2.0
        rv = flask_app.delete(url_for('{0}.job_metadata_get_api'.format(api_version)))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data


def test_method_post_not_allowed(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify the job_metadata API does not support HTTP POST and returns HTTP 405 METHOD NOT ALLOWED.
    """
    if api_version == 'v1':
        rv = flask_app.delete(url_for('{0}.job_metadata_api'.format(api_version)))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data
    else:  # api version 2.0
        rv = flask_app.delete(url_for('{0}.job_metadata_get_api'.format(api_version)))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data


def test_method_put_not_allowed(flask_app, db_conn, api_version):
    """Test Depot ID not known yet.

    Verify the job_metadata API does not support HTTP PUT and returns HTTP 405 METHOD NOT ALLOWED.
    """
    if api_version == 'v1':
        rv = flask_app.delete(url_for('{0}.job_metadata_api'.format(api_version)))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data
    else:  # api version 2.0
        rv = flask_app.delete(url_for('{0}.job_metadata_get_api'.format(api_version)))
        assert rv.status_code == 405
        assert b'Method Not Allowed' in rv.data


def test_job_metadata_most_recent_successful_job_start_time(db_conn):
    """Test Depot ID not known yet.

    Verify metadata::test_job_metadata_most_recent_successful_job_start_time function.
    """
    extra_metadata = {'perform_duplicates_check': True,
                      'perform_historic_check': True,
                      'performance_timing': {}}  # noqa E127
    job_metadata_importer(db_conn=db_conn, command='dirbs-import', run_id=1, subcommand='pairing-list',
                          status='success', extra_metadata=extra_metadata)
    metadata.most_recent_job_start_time_by_command(db_conn, 'dirbs-import', subcommand='pairing-list',
                                                   successful_only=True)


def test_job_metadata_v2_pagination(flask_app, db_conn):
    """Test Depot ID not known yet.

    Verify that results returned by metadata api version 2.0 are paginated.
    """
    # insert 20 records
    for i in range(10):
        job_metadata_importer(db_conn=db_conn, command='dirbs-classify', run_id=i, subcommand='',
                              status='success')

        job_metadata_importer(db_conn=db_conn, command='dirbs-prune',
                              run_id=i, subcommand='triplets', status='success')

    # test all records are fetched when no pagination params are given
    rv = flask_app.get(url_for('v2.job_metadata_get_api'))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['_keys']['result_size'] == 20
    assert data['_keys']['current_key'] == '0'
    assert data['_keys']['next_key'] == '10'
    assert len(data['jobs']) == 10

    # test pagination, start from 1st record and 5 records per page
    offset = 0
    limit = 5
    rv = flask_app.get(url_for('v2.job_metadata_get_api', offset=offset, limit=limit))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['_keys']['result_size'] == 20
    assert data['_keys']['current_key'] == '0'
    assert data['_keys']['next_key'] == str(offset + limit)
    assert len(data['jobs']) == 5

    next_offset = offset + limit
    rv = flask_app.get(url_for('v2.job_metadata_get_api', offset=next_offset, limit=limit))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['_keys']['result_size'] == 20
    assert data['_keys']['current_key'] == str(offset + limit)
    assert data['_keys']['next_key'] == '10'

    next_offset = next_offset + limit
    rv = flask_app.get(url_for('v2.job_metadata_get_api', offset=next_offset, limit=limit))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['_keys']['result_size'] == 20
    assert data['_keys']['current_key'] == '10'
    assert data['_keys']['next_key'] == '15'

    # pagination with sorting order ascending based on start time
    offset = 1
    limit = 5
    order = 'ASC'
    rv = flask_app.get(url_for('v2.job_metadata_get_api', offset=offset, limit=limit, order=order))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['_keys']['result_size'] == 20
    assert data['_keys']['current_key'] == '1'
    assert data['_keys']['next_key'] == '6'
    assert len(data['jobs']) == 5
    assert data['jobs'][0]['start_time'] <= data['jobs'][1]['start_time']
    assert data['jobs'][1]['start_time'] <= data['jobs'][2]['start_time']
    assert data['jobs'][2]['start_time'] <= data['jobs'][3]['start_time']
    assert data['jobs'][3]['start_time'] <= data['jobs'][4]['start_time']

    # order Descending
    order = 'DESC'
    rv = flask_app.get(url_for('v2.job_metadata_get_api', offset=offset, limit=limit, order=order))
    assert rv.status_code == 200
    data = json.loads(rv.data.decode('utf-8'))
    assert data['_keys']['result_size'] == 20
    assert data['_keys']['current_key'] == '1'
    assert data['_keys']['next_key'] == '6'
    assert len(data['jobs']) == 5
    assert data['jobs'][0]['start_time'] >= data['jobs'][1]['start_time']
    assert data['jobs'][1]['start_time'] >= data['jobs'][2]['start_time']
    assert data['jobs'][2]['start_time'] >= data['jobs'][3]['start_time']
    assert data['jobs'][3]['start_time'] >= data['jobs'][4]['start_time']
