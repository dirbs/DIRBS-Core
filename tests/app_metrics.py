"""
Metrics unit tests.

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

from flask import url_for
import click
import pytest
from click.testing import CliRunner

from dirbs.cli import common
from dirbs.importer.gsma_data_importer import GSMADataImporter
from _fixtures import *  # noqa: F403, F401
from _helpers import expect_failure, get_importer
from _importer_params import OperatorDataParams, GSMADataParams


def test_api_metrics(mocker, flask_app, api_version):
    """Test Depot ID TBD.

    Verify that StatsD is sent statistics about the performance of TAC and IMEI APIs. The
    metric name should contain the HTTP status code so that the response times can be
    broken down by status code.
    """
    # Can't import dirbs.api at top level as it configure logging
    import dirbs.api

    if api_version == 'v1':
        mocker.patch.object(dirbs.api, 'statsd', auto_spec=True)
        rv = flask_app.get(url_for('{0}.imei_api'.format(api_version), imei='0117220037002633'))
        assert rv.status_code == 200
        dirbs.api.statsd.timing.assert_any_call('dirbs.api.response_time.imei.{0}.GET.200'.format(api_version),
                                                mocker.ANY)
        dirbs.api.statsd.incr.assert_any_call('dirbs.api.successes.imei.{0}.200'.format(api_version))

        dirbs.api.statsd.reset_mock()
        rv = flask_app.get(url_for('{0}.tac_api'.format(api_version), tac='12345678'))
        assert rv.status_code == 200
        dirbs.api.statsd.timing.assert_any_call('dirbs.api.response_time.tac.{0}.GET.200'.format(api_version),
                                                mocker.ANY)
        dirbs.api.statsd.incr.assert_any_call('dirbs.api.successes.tac.{0}.200'.format(api_version))

        for api in ['tac', 'imei']:
            dirbs.api.statsd.reset_mock()
            rv = flask_app.get(url_for('{0}.{1}_api'.format(api_version, api), **{api: 'aaaaaaaaaaaaaaaaaaaaaaa'}))
            assert rv.status_code == 400
            dirbs.api.statsd.timing.assert_any_call('dirbs.api.response_time.{0}.{1}.GET.400'.format(api, api_version),
                                                    mocker.ANY)
            dirbs.api.statsd.incr.assert_any_call('dirbs.api.failures.{0}.{1}.400'.format(api, api_version))

            dirbs.api.statsd.reset_mock()
            rv = flask_app.post(url_for('{0}.{1}_api'.format(api_version, api), **{api: 'a'}))
            assert rv.status_code == 405
            dirbs.api.statsd.timing.assert_any_call('dirbs.api.response_time.{0}.{1}.POST.405'.format(
                api, api_version), mocker.ANY)
            dirbs.api.statsd.incr.assert_any_call('dirbs.api.failures.{0}.{1}.405'.format(api, api_version))
    else:  # TODO: add api version 2 test cases
        pass


def test_exception_metrics(mocker, mocked_statsd, flask_app):
    """Test Depot ID TBD.

    Verify that StatsD is sent stats when a code exception is encountered.
    """
    # Can't import dirbs.api at top level as it configure logging
    import dirbs.api
    flask_app.application.config['PROPAGATE_EXCEPTIONS'] = False
    mocker.patch.object(dirbs.api, 'statsd', auto_spec=True)
    rv = flask_app.get('/test_errors')
    assert rv.status_code == 500
    dirbs.api.statsd.incr.assert_any_call('dirbs.api.failures.unknown.500')
    dirbs.api.statsd.incr.assert_any_call('dirbs.exceptions.api.unknown')

    # Verify that all CLI command also generates a statsd message
    @click.command()
    @click.pass_context
    @common.unhandled_exception_handler
    def test_click_command(ctxt):
        """Test click program."""
        raise ValueError('Testing!')

    runner = CliRunner()
    runner.invoke(test_click_command, obj={'STATSD_CLIENT': mocked_statsd})
    mocked_statsd.incr.assert_any_call('dirbs.exceptions.cli.unknown')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_invalid_imei_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
def test_prevalidation_failure_metrics(mocker, mocked_statsd, logger, operator_data_importer):
    """Test Depot ID TBD.

    Verify that StatsD is sent stats when a prevalidation error happens during import.
    """
    expect_failure(operator_data_importer)
    # Expected call is statsd.gauge(key, 1, delta=True)
    mocked_statsd.gauge.assert_any_call(
        'dirbs.import.operator.test_operator.validation_failures.prevalidation', 1, delta=True)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_imeizerocheck1_20160701_20160731.csv',
                             extract=False)],
                         indirect=True)
@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_small_imeicheck_2016.txt')],
                         indirect=True)
def test_file_level_failure_metrics(mocked_statsd, logger, operator_data_importer, gsma_tac_db_importer):
    """Test Depot ID TBD.

    Verify that StatsD is sent stats when a file-level validation failure happens during import.
    """
    gsma_tac_db_importer.import_data()
    expect_failure(operator_data_importer)
    # Expected call is statsd.gauge(key, 1, delta=True)
    mocked_statsd.gauge.assert_any_call(
        'dirbs.import.operator.test_operator.validation_failures.leading_zero', 1, delta=True)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_blankimsi_20160701_20160731.csv',
                             null_imei_threshold=0.2,
                             null_imsi_threshold=0.2,
                             perform_unclean_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_threshold_failure_metrics(mocked_statsd, logger, operator_data_importer):
    """Test Depot ID TBD.

    Verify that StatsD is sent stats when a threshold validation failure happens during import.
    """
    expect_failure(operator_data_importer)
    # Expected call is statsd.gauge(key, 1, delta=True)
    mocked_statsd.gauge.assert_any_call(
        'dirbs.import.operator.test_operator.validation_failures.null_imsi', 1, delta=True)


@pytest.mark.parametrize('gsma_tac_db_importer',
                         [GSMADataParams(filename='gsma_dump_large_july_2016.txt',
                                         extract=False)],
                         indirect=True)
def test_historic_failure_metrics(mocked_statsd, mocked_config, logger, gsma_tac_db_importer, tmpdir, db_conn,
                                  metadata_db_conn):
    """Test Depot ID TBD.

    Verify that StatsD is sent stats when a threshold validation failure happens during import.
    """
    gsma_tac_db_importer.import_data()

    # Try a small import
    with get_importer(GSMADataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      GSMADataParams(filename='gsma_dump_small_july_2016.txt',
                                     import_size_variation_percent=mocked_config.gsma_threshold_config.
                                     import_size_variation_percent,
                                     import_size_variation_absolute=mocked_config.gsma_threshold_config.
                                     import_size_variation_absolute,
                                     extract=False)) as gsma_small_importer:
        expect_failure(gsma_small_importer, exc_message='Failed import size historic check')

    # Expected call is statsd.gauge(key, 1, delta=True)
    mocked_statsd.gauge.assert_any_call(
        'dirbs.import.gsma_tac.validation_failures.historic_import_size', 1, delta=True)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='operator1_20160701_20160731.csv',
                             perform_region_checks=False,
                             perform_home_network_check=False)],
                         indirect=True)
def test_importer_performance_metrics(mocker, mocked_statsd, logger, operator_data_importer):
    """Test Depot ID TBD.

    Verify that StatsD is sent stats when a prevalidation error happens during import.
    """
    operator_data_importer.import_data()
    # Expected call is statsd.gauge(key, 1, delta=True)
    mocked_statsd.gauge.assert_any_call(
        'dirbs.import.operator.test_operator.runs.1.import_time.components.output_stats', mocker.ANY)
