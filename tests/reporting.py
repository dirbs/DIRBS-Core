"""
Reporting unit tests.

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
import json
import datetime
import csv
import shutil
import time
from html.parser import HTMLParser
from glob import glob
import fnmatch

import pytest
from click.testing import CliRunner

from dirbs import __version__, report_schema_version
from dirbs.cli.report import cli as dirbs_report_cli
from dirbs.cli.classify import cli as dirbs_classify_cli
from dirbs.importer.operator_data_importer import OperatorDataImporter
from _helpers import get_importer, expect_success, find_subdirectory_in_dir, \
    invoke_cli_classify_with_conditions_helper, from_cond_dict_list_to_cond_list, import_data
from _fixtures import *  # noqa: F403, F401
from _importer_params import OperatorDataParams, GSMADataParams, StolenListParams, PairListParams, \
    SubscribersListParams, DeviceAssociationListParams
from dirbs.metadata import job_start_time_by_run_id, query_for_command_runs
from dirbs.utils import most_recently_run_condition_info, format_datetime_for_report


def _import_operator_data(filename, operator, row_count, db_conn, metadata_db_conn, db_config, tmpdir,
                          logger, mocked_statsd, mcc_mnc_pairs, perform_historic_checks=False):
    """Function to import operator data."""
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      OperatorDataParams(
                          filename=filename,
                          operator=operator,
                          perform_null_checks=False,
                          perform_unclean_checks=False,
                          perform_historic_checks=perform_historic_checks,
                          mcc_mnc_pairs=mcc_mnc_pairs,
                          extract=False)) as operator_data_importer:
        import_data(operator_data_importer, 'operator_data', row_count, db_conn, logger)


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False)],
                         indirect=True)
def test_subsequent_reports(postgres, db_conn, operator_data_importer, logger, tmpdir, mocked_config):
    """Test Depot ID 96636/96644.

    Verify operator and country data reports can be subsequent and report files are distinguishedby timestamp.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    runner = CliRunner()
    # Run dirbs-classify so we have condition config in there
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['stolen_violations', output_dir],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    result = runner.invoke(dirbs_report_cli, ['stolen_violations', output_dir],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False)],
                         indirect=True)
def test_report_file_name(postgres, db_conn, operator_data_importer, logger, tmpdir, mocked_config):
    """Test Depot ID 96636/96644.

    Verify operator and country data reports should include operator name.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check',
                                              '11', '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)
    assert result.exit_code == 0
    report_dir_name = find_subdirectory_in_dir('report*', output_dir)
    file_list = os.listdir(report_dir_name)

    # Verify visual reports and JSON files are created for the all the operators
    for ext in ['html', 'json']:
        assert 'Country1_11_2016.{0}'.format(ext) in file_list
        assert 'Country1_operator4_11_2016.{0}'.format(ext) in file_list
        assert 'Country1_operator3_11_2016.{0}'.format(ext) in file_list
        assert 'Country1_operator2_11_2016.{0}'.format(ext) in file_list
        assert 'Country1_operator1_11_2016.{0}'.format(ext) in file_list

    for _ in ['html', 'json']:
        # Verify per-TAC compliance data is there for the country-level report and for operator1
        assert 'Country1_11_2016.csv' in file_list
        assert 'Country1_operator1_11_2016.csv' in file_list
        assert 'Country1_operator2_11_2016.csv' not in file_list
        assert 'Country1_operator3_11_2016.csv' not in file_list
        assert 'Country1_operator4_11_2016.csv' not in file_list
        # Verify conditions counts CSV file is there for the country-level report and for operator1
        assert 'Country1_11_2016_condition_counts.csv' in file_list
        assert 'Country1_operator1_11_2016_condition_counts.csv' in file_list
        assert 'Country1_operator2_11_2016_condition_counts.csv' not in file_list
        assert 'Country1_operator3_11_2016_condition_counts.csv' not in file_list
        assert 'Country1_operator4_11_2016_condition_counts.csv' not in file_list

    # Check that no IMEI overlap files are in list
    overlap_reports = fnmatch.filter(file_list, 'Country1_11_2016_condition_imei_overlap*.csv')
    assert len(overlap_reports) == 0

    # Delete all files so that subsequence dirbs_report_cli works
    for subdir in os.listdir(output_dir):
        shutil.rmtree(os.path.join(output_dir, subdir))

    result = runner.invoke(dirbs_report_cli, ['gsma_not_found', '--disable-retention-check', '--disable-data-check',
                                              '11', '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)
    assert result.exit_code == 0
    report_dir = find_subdirectory_in_dir('report*', output_dir)
    file_list = os.listdir(report_dir)

    # Verify GSMA not found report and duplicates report generated at a country level
    assert 'Country1_11_2016_gsma_not_found.csv' in file_list

    # Delete all files so that subsequence dirbs_report_cli works
    for subdir in os.listdir(output_dir):
        shutil.rmtree(os.path.join(output_dir, subdir))

    result = runner.invoke(dirbs_report_cli, ['top_duplicates', '--disable-retention-check', '--disable-data-check',
                                              '11', '2016', output_dir], catch_exceptions=False,
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    report_dir = find_subdirectory_in_dir('report*', output_dir)
    file_list = os.listdir(report_dir)
    assert 'Country1_11_2016_duplicates.csv' in file_list

    # Delete all files so that subsequence dirbs_report_cli works
    for subdir in os.listdir(output_dir):
        shutil.rmtree(os.path.join(output_dir, subdir))

    result = runner.invoke(dirbs_report_cli, ['condition_imei_overlaps', '--disable-retention-check',
                                              '--disable-data-check',
                                              '11', '2016', output_dir], catch_exceptions=False,
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    report_dir = find_subdirectory_in_dir('report*', output_dir)
    file_list = os.listdir(report_dir)
    # Check that IMEI overlap files are in list
    overlap_reports = fnmatch.filter(file_list, 'Country1_11_2016_condition_imei_overlap*.csv')
    assert len(overlap_reports) > 0


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False)],
                         indirect=True)
def test_report_contains_date_fields(postgres, db_conn, metadata_db_conn, operator_data_importer,
                                     tmpdir, mocked_config, logger, mocked_statsd):
    """Test Depot ID 96637/96748.

    Verify operator and country data reports should include reporting date range and creation date.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    _import_operator_data('testData1-operator-operator4-anonymized_20161101_20161130.csv', 'operator4', 35, db_conn,
                          metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}])
    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)

    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    file_list = os.listdir(reports_dir)

    for report in fnmatch.filter(file_list, '*.json'):
        with open(os.path.join(reports_dir, report), 'r') as json_file:
            parsed_json = json.loads(json_file.read())
            assert parsed_json['start_date'] == '2016-11-01'
            assert parsed_json['end_date'] == '2016-11-30'

    country_report = 'Country1_11_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['creation_date'] == datetime.datetime.now().strftime('%Y-%m-%d')

    operator1_operator_report = 'Country1_operator1_11_2016.json'
    with open(os.path.join(reports_dir, operator1_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['creation_date'] == datetime.datetime.now().strftime('%Y-%m-%d')

    operator4_operator_report = 'Country1_operator4_11_2016.json'
    with open(os.path.join(reports_dir, operator4_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['creation_date'] == datetime.datetime.now().strftime('%Y-%m-%d')


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False)],
                         indirect=True)
def test_report_contains_conditions(postgres, db_conn, operator_data_importer, tmpdir, mocked_config, logger):
    """Test Depot ID 96860/96861.

    Verify that the list of classification conditions used to generate the report are included in the JSON.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    runner = CliRunner()
    # Run dirbs-classify so we have condition config in there
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)
    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    file_list = os.listdir(reports_dir)

    for report in fnmatch.filter(file_list, '*.json'):
        with open(os.path.join(reports_dir, report), 'r') as json_file:
            parsed_json = json.loads(json_file.read())
            if report in ['Country1_11_2016.json', 'Country1_operator1_11_2016.json']:
                assert parsed_json['has_data']
                conditions = parsed_json['classification_conditions']
                # last_successful_run to be tested in test_condition_last_run_time
                assert all(['last_successful_run' in c for c in conditions])
                conditions = [{k: v for k, v in c.items() if k != 'last_successful_run'} for c in conditions]
                assert conditions == [
                    {'blocking': True,
                     'config': {'blocking': True,
                                'amnesty_eligible': False,
                                'dimensions': [{'invert': False,
                                                'module': 'duplicate_threshold',
                                                'parameters': {'period_days': 120,
                                                               'threshold': 10}},
                                               {'invert': False,
                                                'module': 'duplicate_daily_avg',
                                                'parameters': {'min_seen_days': 5,
                                                               'period_days': 30,
                                                               'threshold': 4.0}}],
                                'grace_period_days': 60,
                                'label': 'duplicate_mk1',
                                'max_allowed_matching_ratio': 0.1,
                                'reason': 'Duplicate IMEI detected',
                                'sticky': True},
                     'label': 'duplicate_mk1'},
                    {'blocking': True,
                     'config': {'blocking': True,
                                'amnesty_eligible': False,
                                'dimensions': [{'invert': False,
                                                'module': 'gsma_not_found',
                                                'parameters': {'ignore_rbi_delays': True}}],
                                'grace_period_days': 30,
                                'label': 'gsma_not_found',
                                'max_allowed_matching_ratio': 0.1,
                                'reason': 'TAC not found in GSMA TAC database',
                                'sticky': False},
                     'label': 'gsma_not_found'},
                    {'blocking': True,
                     'config': {'blocking': True,
                                'amnesty_eligible': False,
                                'dimensions': [{'invert': False,
                                                'module': 'stolen_list',
                                                'parameters': {}}],
                                'grace_period_days': 0,
                                'label': 'local_stolen',
                                'max_allowed_matching_ratio': 1.0,
                                'reason': 'IMEI found on local stolen list',
                                'sticky': False},
                     'label': 'local_stolen'},
                    {'blocking': True,
                     'config': {'blocking': True,
                                'amnesty_eligible': False,
                                'dimensions': [{'invert': False,
                                                'module': 'not_on_registration_list',
                                                'parameters': {}}],
                                'grace_period_days': 0,
                                'label': 'not_on_registration_list',
                                'max_allowed_matching_ratio': 1.0,
                                'reason': 'IMEI not found on local registration list',
                                'sticky': False},
                     'label': 'not_on_registration_list'},
                    {'blocking': False,
                     'config': {'blocking': False,
                                'amnesty_eligible': False,
                                'dimensions': [{'invert': False,
                                                'module': 'inconsistent_rat',
                                                'parameters': {}}],
                                'grace_period_days': 30,
                                'label': 'inconsistent_rat',
                                'max_allowed_matching_ratio': 1.0,
                                'reason': 'IMEI RAT inconsistent with device capability',
                                'sticky': False},
                     'label': 'inconsistent_rat'},
                    {'blocking': False,
                     'config': {'blocking': False,
                                'amnesty_eligible': False,
                                'dimensions': [{'invert': False,
                                                'module': 'malformed_imei',
                                                'parameters': {}}],
                                'grace_period_days': 0,
                                'label': 'malformed_imei',
                                'max_allowed_matching_ratio': 0.1,
                                'reason': 'Invalid characters detected in IMEI',
                                'sticky': False},
                     'label': 'malformed_imei'}]
            else:
                # Placeholder reports do not contain conditions
                assert not parsed_json['has_data']
                assert 'classification_conditions' not in parsed_json


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False)],
                         indirect=True)
def test_report_contains_versions(postgres, db_conn, operator_data_importer, tmpdir, mocked_config, logger):
    """Test Depot ID Unknown.

    Verify that the software version and reporting schema version are included in the HTML and JSON reports.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    runner = CliRunner()
    # Run dirbs-classify so we have condition config in there
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)

    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    file_list = os.listdir(reports_dir)

    # Check that JSON files have the required attributes
    for report in fnmatch.filter(file_list, '*.json'):
        with open(os.path.join(reports_dir, report), 'r') as json_file:
            parsed_json = json.loads(json_file.read())
            assert parsed_json['report_schema_version'] == report_schema_version
            assert parsed_json['software_version'] == __version__

    # Now check that the HTML files have the right format
    class ReportParser(HTMLParser):
        def handle_starttag(self, tag, attrs):
            if tag == 'html':
                attrs_dict = dict(attrs)
                assert attrs_dict['data-software-version'] == __version__
                assert attrs_dict['data-report-schema-version'] == str(report_schema_version)

    for report in fnmatch.filter(file_list, '*.html'):
        with open(os.path.join(reports_dir, report), 'r') as html_file:
            parser = ReportParser()
            parser.feed(html_file.read())


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False)],
                         indirect=True)
def test_report_contains_per_day_records(postgres, db_conn, metadata_db_conn, operator_data_importer,
                                         tmpdir, mocked_config, logger, mocked_statsd):
    """Test Depot ID 96638/96639/96640/96641.

    Verify operator and country data reports should include count of distinct triplet records seen per day.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    _import_operator_data('testData1-operator-operator4-anonymized_20161101_20161130.csv', 'operator4', 35, db_conn,
                          metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}])

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)

    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_11_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        recs_per_day_records = {rec['date']: rec['count'] for rec in parsed_json['recs_per_day']}
        assert recs_per_day_records['2016-11-01'] == 2
        assert recs_per_day_records['2016-11-02'] == 2
        assert recs_per_day_records['2016-11-03'] == 1
        assert recs_per_day_records['2016-11-04'] == 3
        assert recs_per_day_records['2016-11-05'] == 1
        assert recs_per_day_records['2016-11-06'] == 1
        assert recs_per_day_records['2016-11-07'] == 3
        assert recs_per_day_records['2016-11-08'] == 1
        assert recs_per_day_records['2016-11-09'] == 1
        assert recs_per_day_records['2016-11-10'] == 2
        assert recs_per_day_records['2016-11-11'] == 2
        assert recs_per_day_records['2016-11-12'] == 2
        assert recs_per_day_records['2016-11-13'] == 1
        assert recs_per_day_records['2016-11-14'] == 0
        assert recs_per_day_records['2016-11-15'] == 1
        assert recs_per_day_records['2016-11-16'] == 0
        assert recs_per_day_records['2016-11-17'] == 2
        assert recs_per_day_records['2016-11-18'] == 3
        assert recs_per_day_records['2016-11-19'] == 1
        assert recs_per_day_records['2016-11-20'] == 1
        assert recs_per_day_records['2016-11-21'] == 1
        assert recs_per_day_records['2016-11-22'] == 1
        assert recs_per_day_records['2016-11-23'] == 0
        assert recs_per_day_records['2016-11-24'] == 2
        assert recs_per_day_records['2016-11-25'] == 0
        assert recs_per_day_records['2016-11-26'] == 0
        assert recs_per_day_records['2016-11-27'] == 0
        assert recs_per_day_records['2016-11-28'] == 0
        assert recs_per_day_records['2016-11-29'] == 0
        assert recs_per_day_records['2016-11-30'] == 1

        imsis_per_day_records = {rec['date']: rec['count'] for rec in parsed_json['imsis_per_day']}
        assert imsis_per_day_records['2016-11-01'] == 2
        assert imsis_per_day_records['2016-11-02'] == 2
        assert imsis_per_day_records['2016-11-03'] == 1
        assert imsis_per_day_records['2016-11-04'] == 3
        assert imsis_per_day_records['2016-11-05'] == 1
        assert imsis_per_day_records['2016-11-06'] == 1
        assert imsis_per_day_records['2016-11-07'] == 3
        assert imsis_per_day_records['2016-11-08'] == 1
        assert imsis_per_day_records['2016-11-09'] == 1
        assert imsis_per_day_records['2016-11-10'] == 2
        assert imsis_per_day_records['2016-11-11'] == 2
        assert imsis_per_day_records['2016-11-12'] == 2
        assert imsis_per_day_records['2016-11-13'] == 1
        assert imsis_per_day_records['2016-11-14'] == 0
        assert imsis_per_day_records['2016-11-15'] == 1
        assert imsis_per_day_records['2016-11-16'] == 0
        assert imsis_per_day_records['2016-11-17'] == 2
        assert imsis_per_day_records['2016-11-18'] == 3
        assert imsis_per_day_records['2016-11-19'] == 1
        assert imsis_per_day_records['2016-11-20'] == 1
        assert imsis_per_day_records['2016-11-21'] == 1
        assert imsis_per_day_records['2016-11-22'] == 1
        assert imsis_per_day_records['2016-11-23'] == 0
        assert imsis_per_day_records['2016-11-24'] == 2
        assert imsis_per_day_records['2016-11-25'] == 0
        assert imsis_per_day_records['2016-11-26'] == 0
        assert imsis_per_day_records['2016-11-27'] == 0
        assert imsis_per_day_records['2016-11-28'] == 0
        assert imsis_per_day_records['2016-11-29'] == 0
        assert imsis_per_day_records['2016-11-30'] == 1
        imeis_per_day = {rec['date']: rec['count'] for rec in parsed_json['imeis_per_day']}
        assert imeis_per_day['2016-11-01'] == 2
        assert imeis_per_day['2016-11-02'] == 2
        assert imeis_per_day['2016-11-03'] == 1
        assert imeis_per_day['2016-11-04'] == 2
        assert imeis_per_day['2016-11-05'] == 1
        assert imeis_per_day['2016-11-06'] == 1
        assert imeis_per_day['2016-11-07'] == 3
        assert imeis_per_day['2016-11-08'] == 1
        assert imeis_per_day['2016-11-09'] == 1
        assert imeis_per_day['2016-11-10'] == 2
        assert imeis_per_day['2016-11-11'] == 2
        assert imeis_per_day['2016-11-12'] == 2
        assert imeis_per_day['2016-11-13'] == 1
        assert imeis_per_day['2016-11-14'] == 0
        assert imeis_per_day['2016-11-15'] == 1
        assert imeis_per_day['2016-11-16'] == 0
        assert imeis_per_day['2016-11-17'] == 1
        assert imeis_per_day['2016-11-18'] == 3
        assert imeis_per_day['2016-11-19'] == 1
        assert imeis_per_day['2016-11-20'] == 1
        assert imeis_per_day['2016-11-21'] == 1
        assert imeis_per_day['2016-11-22'] == 1
        assert imeis_per_day['2016-11-23'] == 0
        assert imeis_per_day['2016-11-24'] == 2
        assert imeis_per_day['2016-11-25'] == 0
        assert imeis_per_day['2016-11-26'] == 0
        assert imeis_per_day['2016-11-27'] == 0
        assert imeis_per_day['2016-11-28'] == 0
        assert imeis_per_day['2016-11-29'] == 0
        assert imeis_per_day['2016-11-30'] == 1
        msisdns_per_day_records = {rec['date']: rec['count'] for rec in parsed_json['msisdns_per_day']}
        assert msisdns_per_day_records['2016-11-01'] == 2
        assert msisdns_per_day_records['2016-11-02'] == 2
        assert msisdns_per_day_records['2016-11-03'] == 1
        assert msisdns_per_day_records['2016-11-04'] == 2
        assert msisdns_per_day_records['2016-11-05'] == 1
        assert msisdns_per_day_records['2016-11-06'] == 1
        assert msisdns_per_day_records['2016-11-07'] == 3
        assert msisdns_per_day_records['2016-11-08'] == 1
        assert msisdns_per_day_records['2016-11-09'] == 1
        assert msisdns_per_day_records['2016-11-10'] == 2
        assert msisdns_per_day_records['2016-11-11'] == 2
        assert msisdns_per_day_records['2016-11-12'] == 2
        assert msisdns_per_day_records['2016-11-13'] == 1
        assert msisdns_per_day_records['2016-11-14'] == 0
        assert msisdns_per_day_records['2016-11-15'] == 1
        assert msisdns_per_day_records['2016-11-16'] == 0
        assert msisdns_per_day_records['2016-11-17'] == 1
        assert msisdns_per_day_records['2016-11-18'] == 3
        assert msisdns_per_day_records['2016-11-19'] == 1
        assert msisdns_per_day_records['2016-11-20'] == 1
        assert msisdns_per_day_records['2016-11-21'] == 1
        assert msisdns_per_day_records['2016-11-22'] == 1
        assert msisdns_per_day_records['2016-11-23'] == 0
        assert msisdns_per_day_records['2016-11-24'] == 2
        assert msisdns_per_day_records['2016-11-25'] == 0
        assert msisdns_per_day_records['2016-11-26'] == 0
        assert msisdns_per_day_records['2016-11-27'] == 0
        assert msisdns_per_day_records['2016-11-28'] == 0
        assert msisdns_per_day_records['2016-11-29'] == 0
        assert msisdns_per_day_records['2016-11-30'] == 1

    operator1_operator_report = 'Country1_operator1_11_2016.json'
    with open(os.path.join(reports_dir, operator1_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        recs_per_day_records = {rec['date']: rec['count'] for rec in parsed_json['recs_per_day']}
        assert recs_per_day_records['2016-11-01'] == 2
        assert recs_per_day_records['2016-11-02'] == 2
        assert recs_per_day_records['2016-11-03'] == 1
        assert recs_per_day_records['2016-11-04'] == 2
        assert recs_per_day_records['2016-11-05'] == 1
        assert recs_per_day_records['2016-11-06'] == 0
        assert recs_per_day_records['2016-11-07'] == 2
        assert recs_per_day_records['2016-11-08'] == 1
        assert recs_per_day_records['2016-11-09'] == 0
        assert recs_per_day_records['2016-11-10'] == 1
        assert recs_per_day_records['2016-11-11'] == 1
        assert recs_per_day_records['2016-11-12'] == 0
        assert recs_per_day_records['2016-11-13'] == 0
        assert recs_per_day_records['2016-11-14'] == 0
        assert recs_per_day_records['2016-11-15'] == 1
        assert recs_per_day_records['2016-11-16'] == 0
        assert recs_per_day_records['2016-11-17'] == 1
        assert recs_per_day_records['2016-11-18'] == 1
        assert recs_per_day_records['2016-11-19'] == 0
        assert recs_per_day_records['2016-11-20'] == 0
        assert recs_per_day_records['2016-11-21'] == 0
        assert recs_per_day_records['2016-11-22'] == 1
        assert recs_per_day_records['2016-11-23'] == 0
        assert recs_per_day_records['2016-11-24'] == 0
        assert recs_per_day_records['2016-11-25'] == 0
        assert recs_per_day_records['2016-11-26'] == 0
        assert recs_per_day_records['2016-11-27'] == 0
        assert recs_per_day_records['2016-11-28'] == 0
        assert recs_per_day_records['2016-11-29'] == 0
        assert recs_per_day_records['2016-11-30'] == 0
        imsis_per_day_records = {rec['date']: rec['count'] for rec in parsed_json['imsis_per_day']}
        assert imsis_per_day_records['2016-11-01'] == 2
        assert imsis_per_day_records['2016-11-02'] == 2
        assert imsis_per_day_records['2016-11-03'] == 1
        assert imsis_per_day_records['2016-11-04'] == 2
        assert imsis_per_day_records['2016-11-05'] == 1
        assert imsis_per_day_records['2016-11-06'] == 0
        assert imsis_per_day_records['2016-11-07'] == 2
        assert imsis_per_day_records['2016-11-08'] == 1
        assert imsis_per_day_records['2016-11-09'] == 0
        assert imsis_per_day_records['2016-11-10'] == 1
        assert imsis_per_day_records['2016-11-11'] == 1
        assert imsis_per_day_records['2016-11-12'] == 0
        assert imsis_per_day_records['2016-11-13'] == 0
        assert imsis_per_day_records['2016-11-14'] == 0
        assert imsis_per_day_records['2016-11-15'] == 1
        assert imsis_per_day_records['2016-11-16'] == 0
        assert imsis_per_day_records['2016-11-17'] == 1
        assert imsis_per_day_records['2016-11-18'] == 1
        assert imsis_per_day_records['2016-11-19'] == 0
        assert imsis_per_day_records['2016-11-20'] == 0
        assert imsis_per_day_records['2016-11-21'] == 0
        assert imsis_per_day_records['2016-11-22'] == 1
        assert imsis_per_day_records['2016-11-23'] == 0
        assert imsis_per_day_records['2016-11-24'] == 0
        assert imsis_per_day_records['2016-11-25'] == 0
        assert imsis_per_day_records['2016-11-26'] == 0
        assert imsis_per_day_records['2016-11-27'] == 0
        assert imsis_per_day_records['2016-11-28'] == 0
        assert imsis_per_day_records['2016-11-29'] == 0
        assert imsis_per_day_records['2016-11-30'] == 0
        imeis_per_day_records = {rec['date']: rec['count'] for rec in parsed_json['imeis_per_day']}
        assert imeis_per_day_records['2016-11-01'] == 2
        assert imeis_per_day_records['2016-11-02'] == 2
        assert imeis_per_day_records['2016-11-03'] == 1
        assert imeis_per_day_records['2016-11-04'] == 2
        assert imeis_per_day_records['2016-11-05'] == 1
        assert imeis_per_day_records['2016-11-06'] == 0
        assert imeis_per_day_records['2016-11-07'] == 2
        assert imeis_per_day_records['2016-11-08'] == 1
        assert imeis_per_day_records['2016-11-09'] == 0
        assert imeis_per_day_records['2016-11-10'] == 1
        assert imeis_per_day_records['2016-11-11'] == 1
        assert imeis_per_day_records['2016-11-12'] == 0
        assert imeis_per_day_records['2016-11-13'] == 0
        assert imeis_per_day_records['2016-11-14'] == 0
        assert imeis_per_day_records['2016-11-15'] == 1
        assert imeis_per_day_records['2016-11-16'] == 0
        assert imeis_per_day_records['2016-11-17'] == 1
        assert imeis_per_day_records['2016-11-18'] == 1
        assert imeis_per_day_records['2016-11-19'] == 0
        assert imeis_per_day_records['2016-11-20'] == 0
        assert imeis_per_day_records['2016-11-21'] == 0
        assert imeis_per_day_records['2016-11-22'] == 1
        assert imeis_per_day_records['2016-11-23'] == 0
        assert imeis_per_day_records['2016-11-24'] == 0
        assert imeis_per_day_records['2016-11-25'] == 0
        assert imeis_per_day_records['2016-11-26'] == 0
        assert imeis_per_day_records['2016-11-27'] == 0
        assert imeis_per_day_records['2016-11-28'] == 0
        assert imeis_per_day_records['2016-11-29'] == 0
        assert imeis_per_day_records['2016-11-30'] == 0
        msisdns_per_day = {rec['date']: rec['count'] for rec in parsed_json['msisdns_per_day']}
        assert msisdns_per_day['2016-11-01'] == 2
        assert msisdns_per_day['2016-11-02'] == 2
        assert msisdns_per_day['2016-11-03'] == 1
        assert msisdns_per_day['2016-11-04'] == 2
        assert msisdns_per_day['2016-11-05'] == 1
        assert msisdns_per_day['2016-11-06'] == 0
        assert msisdns_per_day['2016-11-07'] == 2
        assert msisdns_per_day['2016-11-08'] == 1
        assert msisdns_per_day['2016-11-09'] == 0
        assert msisdns_per_day['2016-11-10'] == 1
        assert msisdns_per_day['2016-11-11'] == 1
        assert msisdns_per_day['2016-11-12'] == 0
        assert msisdns_per_day['2016-11-13'] == 0
        assert msisdns_per_day['2016-11-14'] == 0
        assert msisdns_per_day['2016-11-15'] == 1
        assert msisdns_per_day['2016-11-16'] == 0
        assert msisdns_per_day['2016-11-17'] == 1
        assert msisdns_per_day['2016-11-18'] == 1
        assert msisdns_per_day['2016-11-19'] == 0
        assert msisdns_per_day['2016-11-20'] == 0
        assert msisdns_per_day['2016-11-21'] == 0
        assert msisdns_per_day['2016-11-22'] == 1
        assert msisdns_per_day['2016-11-23'] == 0
        assert msisdns_per_day['2016-11-24'] == 0
        assert msisdns_per_day['2016-11-25'] == 0
        assert msisdns_per_day['2016-11-26'] == 0
        assert msisdns_per_day['2016-11-27'] == 0
        assert msisdns_per_day['2016-11-28'] == 0
        assert msisdns_per_day['2016-11-29'] == 0
        assert msisdns_per_day['2016-11-30'] == 0

    operator4_operator_report = 'Country1_operator4_11_2016.json'
    with open(os.path.join(reports_dir, operator4_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        recs_per_day_records = {rec['date']: rec['count'] for rec in parsed_json['recs_per_day']}
        assert recs_per_day_records['2016-11-01'] == 0
        assert recs_per_day_records['2016-11-02'] == 0
        assert recs_per_day_records['2016-11-03'] == 0
        assert recs_per_day_records['2016-11-04'] == 1
        assert recs_per_day_records['2016-11-05'] == 0
        assert recs_per_day_records['2016-11-06'] == 1
        assert recs_per_day_records['2016-11-07'] == 1
        assert recs_per_day_records['2016-11-08'] == 0
        assert recs_per_day_records['2016-11-09'] == 1
        assert recs_per_day_records['2016-11-10'] == 1
        assert recs_per_day_records['2016-11-11'] == 1
        assert recs_per_day_records['2016-11-12'] == 2
        assert recs_per_day_records['2016-11-13'] == 1
        assert recs_per_day_records['2016-11-14'] == 0
        assert recs_per_day_records['2016-11-15'] == 0
        assert recs_per_day_records['2016-11-16'] == 0
        assert recs_per_day_records['2016-11-17'] == 1
        assert recs_per_day_records['2016-11-18'] == 2
        assert recs_per_day_records['2016-11-19'] == 1
        assert recs_per_day_records['2016-11-20'] == 1
        assert recs_per_day_records['2016-11-21'] == 1
        assert recs_per_day_records['2016-11-22'] == 0
        assert recs_per_day_records['2016-11-23'] == 0
        assert recs_per_day_records['2016-11-24'] == 2
        assert recs_per_day_records['2016-11-25'] == 0
        assert recs_per_day_records['2016-11-26'] == 0
        assert recs_per_day_records['2016-11-27'] == 0
        assert recs_per_day_records['2016-11-28'] == 0
        assert recs_per_day_records['2016-11-29'] == 0
        assert recs_per_day_records['2016-11-30'] == 1
        imsis_per_day_records = {rec['date']: rec['count'] for rec in parsed_json['imsis_per_day']}
        assert imsis_per_day_records['2016-11-01'] == 0
        assert imsis_per_day_records['2016-11-02'] == 0
        assert imsis_per_day_records['2016-11-03'] == 0
        assert imsis_per_day_records['2016-11-04'] == 1
        assert imsis_per_day_records['2016-11-05'] == 0
        assert imsis_per_day_records['2016-11-06'] == 1
        assert imsis_per_day_records['2016-11-07'] == 1
        assert imsis_per_day_records['2016-11-08'] == 0
        assert imsis_per_day_records['2016-11-09'] == 1
        assert imsis_per_day_records['2016-11-10'] == 1
        assert imsis_per_day_records['2016-11-11'] == 1
        assert imsis_per_day_records['2016-11-12'] == 2
        assert imsis_per_day_records['2016-11-13'] == 1
        assert imsis_per_day_records['2016-11-14'] == 0
        assert imsis_per_day_records['2016-11-15'] == 0
        assert imsis_per_day_records['2016-11-16'] == 0
        assert imsis_per_day_records['2016-11-17'] == 1
        assert imsis_per_day_records['2016-11-18'] == 2
        assert imsis_per_day_records['2016-11-19'] == 1
        assert imsis_per_day_records['2016-11-20'] == 1
        assert imsis_per_day_records['2016-11-21'] == 1
        assert imsis_per_day_records['2016-11-22'] == 0
        assert imsis_per_day_records['2016-11-23'] == 0
        assert imsis_per_day_records['2016-11-24'] == 2
        assert imsis_per_day_records['2016-11-25'] == 0
        assert imsis_per_day_records['2016-11-26'] == 0
        assert imsis_per_day_records['2016-11-27'] == 0
        assert imsis_per_day_records['2016-11-28'] == 0
        assert imsis_per_day_records['2016-11-29'] == 0
        assert imsis_per_day_records['2016-11-30'] == 1
        imeis_per_day_records = {rec['date']: rec['count'] for rec in parsed_json['imeis_per_day']}
        assert imeis_per_day_records['2016-11-01'] == 0
        assert imeis_per_day_records['2016-11-01'] == 0
        assert imeis_per_day_records['2016-11-02'] == 0
        assert imeis_per_day_records['2016-11-03'] == 0
        assert imeis_per_day_records['2016-11-04'] == 1
        assert imeis_per_day_records['2016-11-05'] == 0
        assert imeis_per_day_records['2016-11-06'] == 1
        assert imeis_per_day_records['2016-11-07'] == 1
        assert imeis_per_day_records['2016-11-08'] == 0
        assert imeis_per_day_records['2016-11-09'] == 1
        assert imeis_per_day_records['2016-11-10'] == 1
        assert imeis_per_day_records['2016-11-11'] == 1
        assert imeis_per_day_records['2016-11-12'] == 2
        assert imeis_per_day_records['2016-11-13'] == 1
        assert imeis_per_day_records['2016-11-14'] == 0
        assert imeis_per_day_records['2016-11-15'] == 0
        assert imeis_per_day_records['2016-11-16'] == 0
        assert imeis_per_day_records['2016-11-17'] == 1
        assert imeis_per_day_records['2016-11-18'] == 2
        assert imeis_per_day_records['2016-11-19'] == 1
        assert imeis_per_day_records['2016-11-20'] == 1
        assert imeis_per_day_records['2016-11-21'] == 1
        assert imeis_per_day_records['2016-11-22'] == 0
        assert imeis_per_day_records['2016-11-23'] == 0
        assert imeis_per_day_records['2016-11-24'] == 2
        assert imeis_per_day_records['2016-11-25'] == 0
        assert imeis_per_day_records['2016-11-26'] == 0
        assert imeis_per_day_records['2016-11-27'] == 0
        assert imeis_per_day_records['2016-11-28'] == 0
        assert imeis_per_day_records['2016-11-29'] == 0
        assert imeis_per_day_records['2016-11-30'] == 1
        msisdns_per_day = {rec['date']: rec['count'] for rec in parsed_json['msisdns_per_day']}
        assert msisdns_per_day['2016-11-01'] == 0
        assert msisdns_per_day['2016-11-01'] == 0
        assert msisdns_per_day['2016-11-02'] == 0
        assert msisdns_per_day['2016-11-03'] == 0
        assert msisdns_per_day['2016-11-04'] == 1
        assert msisdns_per_day['2016-11-05'] == 0
        assert msisdns_per_day['2016-11-06'] == 1
        assert msisdns_per_day['2016-11-07'] == 1
        assert msisdns_per_day['2016-11-08'] == 0
        assert msisdns_per_day['2016-11-09'] == 1
        assert msisdns_per_day['2016-11-10'] == 1
        assert msisdns_per_day['2016-11-11'] == 1
        assert msisdns_per_day['2016-11-12'] == 2
        assert msisdns_per_day['2016-11-13'] == 1
        assert msisdns_per_day['2016-11-14'] == 0
        assert msisdns_per_day['2016-11-15'] == 0
        assert msisdns_per_day['2016-11-16'] == 0
        assert msisdns_per_day['2016-11-17'] == 1
        assert msisdns_per_day['2016-11-18'] == 2
        assert msisdns_per_day['2016-11-19'] == 1
        assert msisdns_per_day['2016-11-20'] == 1
        assert msisdns_per_day['2016-11-21'] == 1
        assert msisdns_per_day['2016-11-22'] == 0
        assert msisdns_per_day['2016-11-23'] == 0
        assert msisdns_per_day['2016-11-24'] == 2
        assert msisdns_per_day['2016-11-25'] == 0
        assert msisdns_per_day['2016-11-26'] == 0
        assert msisdns_per_day['2016-11-27'] == 0
        assert msisdns_per_day['2016-11-28'] == 0
        assert msisdns_per_day['2016-11-29'] == 0
        assert msisdns_per_day['2016-11-30'] == 1


@pytest.mark.parametrize('operator_data_importer, gsma_tac_db_importer',
                         [(OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False),
                           GSMADataParams(
                               filename='testData1-gsmatac_operator4_operator1_anonymized.txt'))],
                         indirect=True)
def test_report_contains_total_counts(postgres, db_conn, metadata_db_conn, gsma_tac_db_importer,
                                      operator_data_importer, tmpdir, mocked_config, logger, mocked_statsd):
    """Test Depot ID 96642/96736/96737/96738/96739/96740.

    Verify operator and country data reports should include the total counts across the reporting period.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    import_data(gsma_tac_db_importer, 'gsma_data', 13, db_conn, logger)
    _import_operator_data('testData1-operator-operator4-anonymized_20161101_20161130.csv', 'operator4', 35, db_conn,
                          metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}])
    _import_operator_data('testData1-operator-anonymized-invalid_20161101_20161130.csv', 'operator3', 53, db_conn,
                          metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '05'}])

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)

    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_11_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['total_imeis_seen'] == 23
        assert parsed_json['total_gross_adds'] == 23
        assert parsed_json['total_imsis_seen'] == 39
        assert parsed_json['total_msisdns_seen'] == 25
        assert parsed_json['total_triplets_seen'] == 37
        assert parsed_json['top_models_imei_count'] == 19
        assert parsed_json['total_imei_imsis_seen'] == 37
        assert parsed_json['total_imei_msisdns_seen'] == 26
        assert parsed_json['total_imsi_msisdns_seen'] == 39
        assert parsed_json['total_invalid_imei_imsis'] == 4
        assert parsed_json['total_invalid_imei_msisdns'] == 5
        assert parsed_json['total_invalid_triplets'] == 6
        assert parsed_json['total_null_imei_records'] == 3
        assert parsed_json['total_null_imsi_records'] == 2
        assert parsed_json['total_null_msisdn_records'] == 3
        assert parsed_json['total_records_seen'] == 43
        assert parsed_json['compliance_breakdown']['num_compliant_imeis'] == parsed_json['total_imeis_seen']
        assert parsed_json['compliance_breakdown']['num_noncompliant_imeis'] == 0

    operator1_operator_report = 'Country1_operator1_11_2016.json'
    with open(os.path.join(reports_dir, operator1_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['total_imeis_seen'] == 13
        assert parsed_json['total_gross_adds'] == 13
        assert parsed_json['total_imsis_seen'] == 14
        assert parsed_json['total_msisdns_seen'] == 14
        assert parsed_json['total_triplets_seen'] == 14
        assert parsed_json['top_models_imei_count'] == 12
        assert parsed_json['total_imei_imsis_seen'] == 14
        assert parsed_json['total_imei_msisdns_seen'] == 14
        assert parsed_json['total_imsi_msisdns_seen'] == 14
        assert parsed_json['total_invalid_imei_imsis'] == 0
        assert parsed_json['total_invalid_imei_msisdns'] == 0
        assert parsed_json['total_invalid_triplets'] == 0
        assert parsed_json['total_null_imei_records'] == 0
        assert parsed_json['total_null_imsi_records'] == 0
        assert parsed_json['total_null_msisdn_records'] == 0
        assert parsed_json['total_records_seen'] == 14
        assert parsed_json['compliance_breakdown']['num_compliant_imeis'] == parsed_json['total_imeis_seen']
        assert parsed_json['compliance_breakdown']['num_noncompliant_imeis'] == 0

    operator4_operator_report = 'Country1_operator4_11_2016.json'
    with open(os.path.join(reports_dir, operator4_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['total_imeis_seen'] == 12
        assert parsed_json['total_gross_adds'] == 12
        assert parsed_json['total_imsis_seen'] == 13
        assert parsed_json['total_msisdns_seen'] == 14
        assert parsed_json['total_triplets_seen'] == 14
        assert parsed_json['top_models_imei_count'] == 9
        assert parsed_json['total_imei_imsis_seen'] == 13
        assert parsed_json['total_imei_msisdns_seen'] == 14
        assert parsed_json['total_imsi_msisdns_seen'] == 14
        assert parsed_json['total_invalid_imei_imsis'] == 0
        assert parsed_json['total_invalid_imei_msisdns'] == 0
        assert parsed_json['total_invalid_triplets'] == 0
        assert parsed_json['total_null_imei_records'] == 0
        assert parsed_json['total_null_imsi_records'] == 0
        assert parsed_json['total_null_msisdn_records'] == 0
        assert parsed_json['total_records_seen'] == 14
        assert parsed_json['compliance_breakdown']['num_compliant_imeis'] == parsed_json['total_imeis_seen']
        assert parsed_json['compliance_breakdown']['num_noncompliant_imeis'] == 0

    operator3_operator_report = 'Country1_operator3_11_2016.json'
    with open(os.path.join(reports_dir, operator3_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['total_imeis_seen'] == 9
        assert parsed_json['total_gross_adds'] == 9
        assert parsed_json['total_imsis_seen'] == 12
        assert parsed_json['total_msisdns_seen'] == 11
        assert parsed_json['total_triplets_seen'] == 9
        assert parsed_json['top_models_imei_count'] == 7
        assert parsed_json['total_imei_imsis_seen'] == 10
        assert parsed_json['total_imei_msisdns_seen'] == 9
        assert parsed_json['total_imsi_msisdns_seen'] == 11
        assert parsed_json['total_invalid_imei_imsis'] == 4
        assert parsed_json['total_invalid_imei_msisdns'] == 5
        assert parsed_json['total_invalid_triplets'] == 6
        assert parsed_json['total_null_imei_records'] == 3
        assert parsed_json['total_null_imsi_records'] == 2
        assert parsed_json['total_null_msisdn_records'] == 3
        assert parsed_json['total_records_seen'] == 15
        assert parsed_json['compliance_breakdown']['num_compliant_imeis'] == parsed_json['total_imeis_seen']
        assert parsed_json['compliance_breakdown']['num_noncompliant_imeis'] == 0


def test_report_contains_mcc_mnc_pairs(postgres, tmpdir, mocked_config):
    """Test Depot ID unknown.

    Verify operator and country data reports should include the mcc-mnc pairs for each operator.
    """
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)
    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_11_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['mcc_mnc_pairs'] == {'operator2': [{'mcc': '111', 'mnc': '02'}],
                                                'operator4': [{'mcc': '111', 'mnc': '04'}],
                                                'operator1': [{'mcc': '111', 'mnc': '01'}],
                                                'operator3': [{'mcc': '111', 'mnc': '03'}]}

    operator1_operator_report = 'Country1_operator1_11_2016.json'
    with open(os.path.join(reports_dir, operator1_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['mcc_mnc_pairs'] == [{'mnc': '01', 'mcc': '111'}]

    operator4_operator_report = 'Country1_operator4_11_2016.json'
    with open(os.path.join(reports_dir, operator4_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['mcc_mnc_pairs'] == [{'mnc': '04', 'mcc': '111'}]

    operator3_operator_report = 'Country1_operator3_11_2016.json'
    with open(os.path.join(reports_dir, operator3_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['mcc_mnc_pairs'] == [{'mnc': '03', 'mcc': '111'}]

    operator2_operator_report = 'Country1_operator2_11_2016.json'
    with open(os.path.join(reports_dir, operator2_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['mcc_mnc_pairs'] == [{'mnc': '02', 'mcc': '111'}]


@pytest.mark.parametrize('operator_data_importer, gsma_tac_db_importer',
                         [(OperatorDataParams(
                             filename='testData1-v2-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False),
                           GSMADataParams(
                               filename='testData1-gsmatac_operator4_operator1_anonymized.txt'))],
                         indirect=True)
def test_report_contains_top10_model_counts(postgres, db_conn, gsma_tac_db_importer, operator_data_importer,
                                            metadata_db_conn, tmpdir, mocked_config, logger, mocked_statsd):
    """Test Depot ID 96741/96742.

    Verify operator and country data reports include IMEI and gross add counts for each of the top 10 models.
    """
    import_data(operator_data_importer, 'operator_data', 18, db_conn, logger)
    import_data(gsma_tac_db_importer, 'gsma_data', 13, db_conn, logger)
    _import_operator_data('testData1-operator-operator4-anonymized_20161101_20161130.csv', 'operator4', 36, db_conn,
                          metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}])

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)

    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_11_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        top_models_imei_records = {rec['model']: (rec['count'], rec['tech_generations'])
                                   for rec in parsed_json['top_models_imei']}
        assert top_models_imei_records['a64bba0939d4572fe0502afd9dbe5513a686a008'][0] == 4
        assert top_models_imei_records['a64bba0939d4572fe0502afd9dbe5513a686a008'][1] == '2G/3G/4G'
        assert top_models_imei_records['927824c30540c400f59b6c02aeb0a30d5033eb1a'][0] == 2
        assert top_models_imei_records['927824c30540c400f59b6c02aeb0a30d5033eb1a'][1] == 'Non-cellular'
        assert top_models_imei_records['c41643e8b8f611c6b49203aead4f306c4b34eff9'][0] == 2
        assert top_models_imei_records['c41643e8b8f611c6b49203aead4f306c4b34eff9'][1] == '2G'
        assert top_models_imei_records['f3e808a9e81ac355d0e86b08a4e35953f14381ef'][0] == 2
        assert top_models_imei_records['f3e808a9e81ac355d0e86b08a4e35953f14381ef'][1] == '3G'
        assert top_models_imei_records['5bb22f44e3530ecbfa19c670394a82048835fc34'][0] == 2
        assert top_models_imei_records['5bb22f44e3530ecbfa19c670394a82048835fc34'][1] == '2G/4G'
        assert top_models_imei_records['ef12302c27d9b8a5a002918bd643dcd412d2db66'][0] == 2
        assert top_models_imei_records['ef12302c27d9b8a5a002918bd643dcd412d2db66'][1] == 'Non-cellular'
        assert top_models_imei_records['eb8b6a199a22bfa9fc33b13438f27f9136a0e39f'][0] == 2
        assert top_models_imei_records['eb8b6a199a22bfa9fc33b13438f27f9136a0e39f'][1] == '2G'
        assert top_models_imei_records['6d062fd762ba80b7565ef7b1c26dee572e8ae6a2'][0] == 2
        assert top_models_imei_records['6d062fd762ba80b7565ef7b1c26dee572e8ae6a2'][1] == '3G/4G'
        assert top_models_imei_records['d3bdf1170bf4b026e6e29b15a0d66a5ca83f1944'][0] == 1
        assert top_models_imei_records['d3bdf1170bf4b026e6e29b15a0d66a5ca83f1944'][1] == '3G/4G'
        assert top_models_imei_records['cff96c002766bde09400d9030ad2d055e62b7a45'][0] == 1
        assert top_models_imei_records['cff96c002766bde09400d9030ad2d055e62b7a45'][1] == '3G'
        top_models_gross_adds_records = {rec['model']: (rec['count'], rec['tech_generations'])
                                         for rec in parsed_json['top_models_gross_adds']}
        assert top_models_gross_adds_records['a64bba0939d4572fe0502afd9dbe5513a686a008'][0] == 4
        assert top_models_gross_adds_records['a64bba0939d4572fe0502afd9dbe5513a686a008'][1] == '2G/3G/4G'
        assert top_models_gross_adds_records['927824c30540c400f59b6c02aeb0a30d5033eb1a'][0] == 2
        assert top_models_gross_adds_records['927824c30540c400f59b6c02aeb0a30d5033eb1a'][1] == 'Non-cellular'
        assert top_models_gross_adds_records['c41643e8b8f611c6b49203aead4f306c4b34eff9'][0] == 2
        assert top_models_gross_adds_records['c41643e8b8f611c6b49203aead4f306c4b34eff9'][1] == '2G'
        assert top_models_gross_adds_records['f3e808a9e81ac355d0e86b08a4e35953f14381ef'][0] == 2
        assert top_models_gross_adds_records['f3e808a9e81ac355d0e86b08a4e35953f14381ef'][1] == '3G'
        assert top_models_gross_adds_records['5bb22f44e3530ecbfa19c670394a82048835fc34'][0] == 2
        assert top_models_gross_adds_records['5bb22f44e3530ecbfa19c670394a82048835fc34'][1] == '2G/4G'
        assert top_models_gross_adds_records['ef12302c27d9b8a5a002918bd643dcd412d2db66'][0] == 2
        assert top_models_gross_adds_records['ef12302c27d9b8a5a002918bd643dcd412d2db66'][1] == 'Non-cellular'
        assert top_models_gross_adds_records['eb8b6a199a22bfa9fc33b13438f27f9136a0e39f'][0] == 2
        assert top_models_gross_adds_records['eb8b6a199a22bfa9fc33b13438f27f9136a0e39f'][1] == '2G'
        assert top_models_gross_adds_records['6d062fd762ba80b7565ef7b1c26dee572e8ae6a2'][0] == 2
        assert top_models_gross_adds_records['6d062fd762ba80b7565ef7b1c26dee572e8ae6a2'][1] == '3G/4G'
        assert top_models_gross_adds_records['d3bdf1170bf4b026e6e29b15a0d66a5ca83f1944'][0] == 1
        assert top_models_gross_adds_records['d3bdf1170bf4b026e6e29b15a0d66a5ca83f1944'][1] == '3G/4G'
        assert top_models_gross_adds_records['cff96c002766bde09400d9030ad2d055e62b7a45'][0] == 1
        assert top_models_gross_adds_records['cff96c002766bde09400d9030ad2d055e62b7a45'][1] == '3G'

    operator1_operator_report = 'Country1_operator1_11_2016.json'
    with open(os.path.join(reports_dir, operator1_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        top_models_imei_records = {rec['model']: (rec['count'], rec['tech_generations'])
                                   for rec in parsed_json['top_models_imei']}
        assert top_models_imei_records['a64bba0939d4572fe0502afd9dbe5513a686a008'][0] == 4
        assert top_models_imei_records['a64bba0939d4572fe0502afd9dbe5513a686a008'][1] == '2G/3G/4G'
        assert top_models_imei_records['c41643e8b8f611c6b49203aead4f306c4b34eff9'][0] == 2
        assert top_models_imei_records['c41643e8b8f611c6b49203aead4f306c4b34eff9'][1] == '2G'
        assert top_models_imei_records['f3e808a9e81ac355d0e86b08a4e35953f14381ef'][0] == 2
        assert top_models_imei_records['f3e808a9e81ac355d0e86b08a4e35953f14381ef'][1] == '3G'
        assert top_models_imei_records['5bb22f44e3530ecbfa19c670394a82048835fc34'][0] == 2
        assert top_models_imei_records['5bb22f44e3530ecbfa19c670394a82048835fc34'][1] == '2G/4G'
        assert top_models_imei_records['ef12302c27d9b8a5a002918bd643dcd412d2db66'][0] == 2
        assert top_models_imei_records['ef12302c27d9b8a5a002918bd643dcd412d2db66'][1] == 'Non-cellular'
        assert top_models_imei_records['eb8b6a199a22bfa9fc33b13438f27f9136a0e39f'][0] == 1
        assert top_models_imei_records['eb8b6a199a22bfa9fc33b13438f27f9136a0e39f'][1] == '2G'
        top_models_gross_adds_records = {rec['model']: (rec['count'], rec['tech_generations'])
                                         for rec in parsed_json['top_models_gross_adds']}
        assert top_models_gross_adds_records['a64bba0939d4572fe0502afd9dbe5513a686a008'][0] == 4
        assert top_models_gross_adds_records['a64bba0939d4572fe0502afd9dbe5513a686a008'][1] == '2G/3G/4G'
        assert top_models_gross_adds_records['c41643e8b8f611c6b49203aead4f306c4b34eff9'][0] == 2
        assert top_models_gross_adds_records['c41643e8b8f611c6b49203aead4f306c4b34eff9'][1] == '2G'
        assert top_models_gross_adds_records['f3e808a9e81ac355d0e86b08a4e35953f14381ef'][0] == 2
        assert top_models_gross_adds_records['f3e808a9e81ac355d0e86b08a4e35953f14381ef'][1] == '3G'
        assert top_models_gross_adds_records['5bb22f44e3530ecbfa19c670394a82048835fc34'][0] == 2
        assert top_models_gross_adds_records['5bb22f44e3530ecbfa19c670394a82048835fc34'][1] == '2G/4G'
        assert top_models_gross_adds_records['ef12302c27d9b8a5a002918bd643dcd412d2db66'][0] == 2
        assert top_models_gross_adds_records['ef12302c27d9b8a5a002918bd643dcd412d2db66'][1] == 'Non-cellular'
        assert top_models_gross_adds_records['eb8b6a199a22bfa9fc33b13438f27f9136a0e39f'][0] == 1
        assert top_models_gross_adds_records['eb8b6a199a22bfa9fc33b13438f27f9136a0e39f'][1] == '2G'

    operator4_operator_report = 'Country1_operator4_11_2016.json'
    with open(os.path.join(reports_dir, operator4_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        top_models_imei_records = {rec['model']: (rec['count'], rec['tech_generations'])
                                   for rec in parsed_json['top_models_imei']}
        assert top_models_imei_records['6d062fd762ba80b7565ef7b1c26dee572e8ae6a2'][0] == 2
        assert top_models_imei_records['6d062fd762ba80b7565ef7b1c26dee572e8ae6a2'][1] == '3G/4G'
        assert top_models_imei_records['927824c30540c400f59b6c02aeb0a30d5033eb1a'][0] == 2
        assert top_models_imei_records['927824c30540c400f59b6c02aeb0a30d5033eb1a'][1] == 'Non-cellular'
        assert top_models_imei_records['eb8b6a199a22bfa9fc33b13438f27f9136a0e39f'][0] == 2
        assert top_models_imei_records['eb8b6a199a22bfa9fc33b13438f27f9136a0e39f'][1] == '2G'
        assert top_models_imei_records['d3bdf1170bf4b026e6e29b15a0d66a5ca83f1944'][0] == 1
        assert top_models_imei_records['d3bdf1170bf4b026e6e29b15a0d66a5ca83f1944'][1] == '3G/4G'
        assert top_models_imei_records['cff96c002766bde09400d9030ad2d055e62b7a45'][0] == 1
        assert top_models_imei_records['cff96c002766bde09400d9030ad2d055e62b7a45'][1] == '3G'
        assert top_models_imei_records['ef12302c27d9b8a5a002918bd643dcd412d2db66'][0] == 1
        assert top_models_imei_records['ef12302c27d9b8a5a002918bd643dcd412d2db66'][1] == 'Non-cellular'
        top_models_gross_adds_records = {rec['model']: (rec['count'], rec['tech_generations'])
                                         for rec in parsed_json['top_models_gross_adds']}
        assert top_models_gross_adds_records['6d062fd762ba80b7565ef7b1c26dee572e8ae6a2'][0] == 2
        assert top_models_gross_adds_records['6d062fd762ba80b7565ef7b1c26dee572e8ae6a2'][1] == '3G/4G'
        assert top_models_gross_adds_records['927824c30540c400f59b6c02aeb0a30d5033eb1a'][0] == 2
        assert top_models_gross_adds_records['927824c30540c400f59b6c02aeb0a30d5033eb1a'][1] == 'Non-cellular'
        assert top_models_gross_adds_records['eb8b6a199a22bfa9fc33b13438f27f9136a0e39f'][0] == 2
        assert top_models_gross_adds_records['eb8b6a199a22bfa9fc33b13438f27f9136a0e39f'][1] == '2G'
        assert top_models_gross_adds_records['d3bdf1170bf4b026e6e29b15a0d66a5ca83f1944'][0] == 1
        assert top_models_gross_adds_records['d3bdf1170bf4b026e6e29b15a0d66a5ca83f1944'][1] == '3G/4G'
        assert top_models_gross_adds_records['cff96c002766bde09400d9030ad2d055e62b7a45'][0] == 1
        assert top_models_gross_adds_records['cff96c002766bde09400d9030ad2d055e62b7a45'][1] == '3G'
        assert top_models_gross_adds_records['ef12302c27d9b8a5a002918bd643dcd412d2db66'][0] == 1
        assert top_models_gross_adds_records['ef12302c27d9b8a5a002918bd643dcd412d2db66'][1] == 'Non-cellular'


@pytest.mark.parametrize('operator_data_importer, gsma_tac_db_importer',
                         [(OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False),
                           GSMADataParams(
                               filename='testData1-gsmatac_operator4_operator1_anonymized.txt'))],
                         indirect=True)
def test_report_contains_historical_trend_counts(postgres, db_conn, gsma_tac_db_importer, operator_data_importer,
                                                 metadata_db_conn, tmpdir, mocked_config, logger, mocked_statsd):
    """Test Depot ID 96743/96744/96745.

    Verify operator and country reports include historical trends for count of distinct IMSIs, IMEIs and MSISDNs.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    import_data(gsma_tac_db_importer, 'gsma_data', 13, db_conn, logger)
    _import_operator_data('testData1-operator-operator4-anonymized_20161101_20161130.csv', 'operator4', 35, db_conn,
                          metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}])

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)

    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_11_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['historic_imsi_counts'] == [0, 0, 0, 0, 0, 27]
        assert parsed_json['historic_imei_counts'] == [0, 0, 0, 0, 0, 23]
        assert parsed_json['historic_msisdn_counts'] == [0, 0, 0, 0, 0, 25]
        assert parsed_json['historic_triplet_counts'] == [0, 0, 0, 0, 0, 28]

    operator1_operator_report = 'Country1_operator1_11_2016.json'
    with open(os.path.join(reports_dir, operator1_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['historic_imsi_counts'] == [0, 0, 0, 0, 0, 14]
        assert parsed_json['historic_imei_counts'] == [0, 0, 0, 0, 0, 13]
        assert parsed_json['historic_msisdn_counts'] == [0, 0, 0, 0, 0, 14]
        assert parsed_json['historic_triplet_counts'] == [0, 0, 0, 0, 0, 14]

    operator4_operator_report = 'Country1_operator4_11_2016.json'
    with open(os.path.join(reports_dir, operator4_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['historic_imsi_counts'] == [0, 0, 0, 0, 0, 13]
        assert parsed_json['historic_imei_counts'] == [0, 0, 0, 0, 0, 12]
        assert parsed_json['historic_msisdn_counts'] == [0, 0, 0, 0, 0, 14]
        assert parsed_json['historic_triplet_counts'] == [0, 0, 0, 0, 0, 14]


@pytest.mark.parametrize('operator_data_importer, stolen_list_importer, pairing_list_importer, gsma_tac_db_importer',
                         [(OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False),
                           StolenListParams(
                               filename='testData1-sample_stolen_list-anonymized.csv'),
                           PairListParams(
                               filename='testData1-sample_pairinglist-anonymized.csv'),
                           GSMADataParams(
                               filename='testData1-gsmatac_operator4_operator1_anonymized.txt'))],
                         indirect=True)
def test_report_contains_per_tac_compliance_data(postgres, db_conn, gsma_tac_db_importer, operator_data_importer,
                                                 stolen_list_importer, pairing_list_importer, tmpdir, mocked_config,
                                                 metadata_db_conn, logger, mocked_statsd):
    """Test Depot ID 96643.

    Verify operator and country data report should include counts for each combination of conditions per TAC.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    import_data(gsma_tac_db_importer, 'gsma_data', 13, db_conn, logger)
    import_data(stolen_list_importer, 'stolen_list', 21, db_conn, logger)
    import_data(pairing_list_importer, 'pairing_list', 7, db_conn, logger)
    _import_operator_data('testData1-operator-operator4-anonymized_20161101_20161130.csv', 'operator4', 35, db_conn,
                          metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}])

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date=20161201',
                                                '--conditions=gsma_not_found,local_stolen,'
                                                'duplicate_mk1,malformed_imei'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)

    assert result.exit_code == 0

    expected_header_cols = ['TAC', 'duplicate_mk1', 'gsma_not_found', 'local_stolen', 'not_on_registration_list',
                            'inconsistent_rat', 'malformed_imei', 'IMEI count',
                            'IMEI gross adds count', 'IMEI-IMSI count', 'IMEI-MSISDN count',
                            'Subscriber triplet count', 'Compliance Level']
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_11_2016.csv'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        csvreader = csv.reader(file)
        rows = list(csvreader)
        assert len(rows) == 17
        assert rows[0] == expected_header_cols
        assert ['38709433', 'False', 'False', 'False', 'False', 'False', 'False', '1', '1', '1', '1', '1', '2'] in rows
        assert ['21260934', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '2', '2', '2', '2'] in rows
        assert ['21782434', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '2', '2', '2', '2'] in rows
        assert ['56773605', 'False', 'False', 'True', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0'] in rows
        assert ['64220297', 'False', 'True', 'True', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0'] in rows
        assert ['38847733', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '3', '2', '3', '2'] in rows
        assert ['38826033', 'False', 'False', 'False', 'False', 'False', 'False', '1', '1', '2', '2', '2', '2'] in rows
        assert ['38826033', 'False', 'False', 'False', 'False', 'False', 'True', '1', '1', '1', '1', '1', '1'] in rows
        assert ['64220299', 'False', 'True', 'False', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0'] in rows
        assert ['38797833', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '2', '2', '2', '2'] in rows
        assert ['38674133', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '2', '2', '2', '2'] in rows
        assert ['01376803', 'False', 'True', 'False', 'False', 'False', 'False', '1', '1', '1', '2', '2', '0'] in rows
        assert ['64220498', 'False', 'True', 'False', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0'] in rows
        assert ['21123131', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '4', '3', '4', '2'] in rows
        assert ['38772433', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '2', '2', '2', '2'] in rows
        assert ['38245933', 'False', 'False', 'False', 'False', 'False', 'True', '1', '1', '1', '1', '1', '1'] in rows

    operator1_operator_report = 'Country1_operator1_11_2016.csv'
    with open(os.path.join(reports_dir, operator1_operator_report), 'r') as file:
        csvreader = csv.reader(file)
        rows = list(csvreader)
        assert rows[0] == expected_header_cols
        assert len(rows) == 10
        assert ['38797833', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '2', '2', '2', '2'] in rows
        assert ['56773605', 'False', 'False', 'True', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0'] in rows
        assert ['38772433', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '2', '2', '2', '2'] in rows
        assert ['38674133', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '2', '2', '2', '2'] in rows
        assert ['21123131', 'False', 'False', 'False', 'False', 'False', 'False', '1', '1', '1', '1', '1', '2'] in rows
        assert ['38847733', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '2', '2', '2', '2'] in rows
        assert ['38826033', 'False', 'False', 'False', 'False', 'False', 'False', '1', '1', '2', '2', '2', '2'] in rows
        assert ['38826033', 'False', 'False', 'False', 'False', 'False', 'True', '1', '1', '1', '1', '1', '1'] in rows
        assert ['64220498', 'False', 'True', 'False', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0'] in rows

    operator4_operator_report = 'Country1_operator4_11_2016.csv'
    with open(os.path.join(reports_dir, operator4_operator_report), 'r') as file:
        csvreader = csv.reader(file)
        rows = list(csvreader)
        assert rows[0] == expected_header_cols
        assert len(rows) == 10
        assert ['64220297', 'False', 'True', 'True', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0'] in rows
        assert ['38245933', 'False', 'False', 'False', 'False', 'False', 'True', '1', '1', '1', '1', '1', '1'] in rows
        assert ['38709433', 'False', 'False', 'False', 'False', 'False', 'False', '1', '1', '1', '1', '1', '2'] in rows
        assert ['21123131', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '3', '3', '3', '2'] in rows
        assert ['64220299', 'False', 'True', 'False', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0'] in rows
        assert ['21260934', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '2', '2', '2', '2'] in rows
        assert ['38847733', 'False', 'False', 'False', 'False', 'False', 'False', '1', '1', '1', '1', '1', '2'] in rows
        assert ['21782434', 'False', 'False', 'False', 'False', 'False', 'False', '2', '2', '2', '2', '2', '2'] in rows
        assert ['01376803', 'False', 'True', 'False', 'False', 'False', 'False', '1', '1', '1', '2', '2', '0'] in rows


@pytest.mark.parametrize('operator_data_importer, stolen_list_importer, pairing_list_importer, gsma_tac_db_importer',
                         [(OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False),
                           StolenListParams(
                               filename='testData1-sample_stolen_list-anonymized.csv'),
                           PairListParams(
                               filename='testData1-sample_pairinglist-anonymized.csv'),
                           GSMADataParams(
                               filename='testData1-gsmatac_operator4_operator1_anonymized.txt'))],
                         indirect=True)
def test_report_contains_compliance_condition_counts(postgres, db_conn, gsma_tac_db_importer, operator_data_importer,
                                                     stolen_list_importer, pairing_list_importer, tmpdir,
                                                     metadata_db_conn, mocked_config, logger, mocked_statsd):
    """Test Depot ID 96751/96747/96749/96746.

    Verify operator and country reports include count/historic trends of IMEIs/IMSIs
    added to the blacklist for this reporting period.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    import_data(gsma_tac_db_importer, 'gsma_data', 13, db_conn, logger)
    _import_operator_data('testData1-operator-operator4-anonymized_20161101_20161130.csv', 'operator4', 35, db_conn,
                          metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}])
    import_data(stolen_list_importer, 'stolen_list', 21, db_conn, logger)
    import_data(pairing_list_importer, 'pairing_list', 7, db_conn, logger)

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date=20161201',
                                                '--conditions=gsma_not_found,local_stolen,'
                                                'duplicate_mk1,malformed_imei'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)
    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    default_historic_compliance_breakdown = {
        'num_compliant_imeis': 0,
        'num_noncompliant_imeis': 0,
        'num_noncompliant_imeis_blocking': 0,
        'num_noncompliant_imeis_info_only': 0,
        'num_compliant_triplets': 0,
        'num_noncompliant_triplets': 0,
        'num_noncompliant_triplets_blocking': 0,
        'num_noncompliant_triplets_info_only': 0,
        'num_compliant_imei_imsis': 0,
        'num_noncompliant_imei_imsis': 0,
        'num_noncompliant_imei_imsis_blocking': 0,
        'num_noncompliant_imei_imsis_info_only': 0,
        'num_compliant_imei_msisdns': 0,
        'num_noncompliant_imei_msisdns': 0,
        'num_noncompliant_imei_msisdns_blocking': 0,
        'num_noncompliant_imei_msisdns_info_only': 0
    }

    country_report = 'Country1_11_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        duplicate_condition_breakdown = {'num_imeis': 0,
                                         'num_triplets': 0,
                                         'num_imei_gross_adds': 0,
                                         'num_imei_imsis': 0,
                                         'num_imei_msisdns': 0}
        assert duplicate_condition_breakdown in parsed_json['historic_conditions_breakdown']['duplicate_mk1']
        stolen_condition_breakdown = {'num_imeis': 2,
                                      'num_triplets': 2,
                                      'num_imei_gross_adds': 2,
                                      'num_imei_imsis': 2,
                                      'num_imei_msisdns': 2}
        assert stolen_condition_breakdown in parsed_json['historic_conditions_breakdown']['local_stolen']
        malformed_imei_condition_breakdown = {'num_imeis': 2,
                                              'num_triplets': 2,
                                              'num_imei_gross_adds': 2,
                                              'num_imei_imsis': 2,
                                              'num_imei_msisdns': 2}
        assert malformed_imei_condition_breakdown in parsed_json['historic_conditions_breakdown']['malformed_imei']

        # Verify historic counts
        for i in range(0, 5):
            assert default_historic_compliance_breakdown == parsed_json['historic_compliance_breakdown'][i]
        # Verify current counts
        compliance_breakdown = {'num_compliant_imeis': 16,
                                'num_compliant_imei_imsis': 20,
                                'num_compliant_imei_msisdns': 18,
                                'num_compliant_triplets': 20,
                                'num_noncompliant_imeis': 7,
                                'num_noncompliant_imeis_blocking': 5,
                                'num_noncompliant_imeis_info_only': 2,
                                'num_noncompliant_imei_imsis': 9,
                                'num_noncompliant_imei_imsis_blocking': 5,
                                'num_noncompliant_imei_imsis_info_only': 2,
                                'num_noncompliant_imei_msisdns': 6,
                                'num_noncompliant_imei_msisdns_blocking': 6,
                                'num_noncompliant_imei_msisdns_info_only': 2,
                                'num_noncompliant_triplets': 8,
                                'num_noncompliant_triplets_blocking': 6,
                                'num_noncompliant_triplets_info_only': 2}
        assert parsed_json['compliance_breakdown'] == compliance_breakdown
        assert compliance_breakdown == parsed_json['historic_compliance_breakdown'][5]
        assert compliance_breakdown == parsed_json['historic_compliance_breakdown'][-1]

        duplicate_condition_breakdown = {'num_imeis': 0,
                                         'num_triplets': 0,
                                         'num_imei_gross_adds': 0,
                                         'num_imei_imsis': 0,
                                         'num_imei_msisdns': 0}
        assert parsed_json['conditions_breakdown']['duplicate_mk1'] == duplicate_condition_breakdown
        stolen_condition_breakdown = {'num_imeis': 2,
                                      'num_triplets': 2,
                                      'num_imei_gross_adds': 2,
                                      'num_imei_imsis': 2,
                                      'num_imei_msisdns': 2}
        assert parsed_json['conditions_breakdown']['local_stolen'] == stolen_condition_breakdown
        malformed_imei_condition_breakdown = {'num_imeis': 2,
                                              'num_triplets': 2,
                                              'num_imei_gross_adds': 2,
                                              'num_imei_imsis': 2,
                                              'num_imei_msisdns': 2}
        assert parsed_json['conditions_breakdown']['malformed_imei'] == malformed_imei_condition_breakdown

    operator1_operator_report = 'Country1_operator1_11_2016.json'
    with open(os.path.join(reports_dir, operator1_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        duplicate_condition_breakdown = {'num_imeis': 0,
                                         'num_triplets': 0,
                                         'num_imei_gross_adds': 0,
                                         'num_imei_imsis': 0,
                                         'num_imei_msisdns': 0}
        assert duplicate_condition_breakdown in parsed_json['historic_conditions_breakdown']['duplicate_mk1']
        stolen_condition_breakdown = {'num_imeis': 1,
                                      'num_triplets': 1,
                                      'num_imei_gross_adds': 1,
                                      'num_imei_imsis': 1,
                                      'num_imei_msisdns': 1}
        assert stolen_condition_breakdown in parsed_json['historic_conditions_breakdown']['local_stolen']
        malformed_imei_condition_breakdown = {'num_imeis': 1,
                                              'num_triplets': 1,
                                              'num_imei_gross_adds': 1,
                                              'num_imei_imsis': 1,
                                              'num_imei_msisdns': 1}
        assert malformed_imei_condition_breakdown in parsed_json['historic_conditions_breakdown']['malformed_imei']

        # Verify historic counts
        for i in range(0, 5):
            assert default_historic_compliance_breakdown == parsed_json['historic_compliance_breakdown'][i]
        # Verify current counts
        compliance_breakdown = {'num_compliant_imeis': 10,
                                'num_compliant_imei_imsis': 11,
                                'num_compliant_imei_msisdns': 11,
                                'num_compliant_triplets': 11,
                                'num_noncompliant_imeis': 3,
                                'num_noncompliant_imeis_blocking': 2,
                                'num_noncompliant_imeis_info_only': 1,
                                'num_noncompliant_imei_imsis': 4,
                                'num_noncompliant_imei_imsis_blocking': 2,
                                'num_noncompliant_imei_imsis_info_only': 1,
                                'num_noncompliant_imei_msisdns': 2,
                                'num_noncompliant_imei_msisdns_blocking': 2,
                                'num_noncompliant_imei_msisdns_info_only': 1,
                                'num_noncompliant_triplets': 3,
                                'num_noncompliant_triplets_blocking': 2,
                                'num_noncompliant_triplets_info_only': 1}
        assert parsed_json['compliance_breakdown'] == compliance_breakdown
        assert compliance_breakdown == parsed_json['historic_compliance_breakdown'][5]
        assert compliance_breakdown == parsed_json['historic_compliance_breakdown'][-1]

        duplicate_condition_breakdown = {'num_imeis': 0,
                                         'num_triplets': 0,
                                         'num_imei_gross_adds': 0,
                                         'num_imei_imsis': 0,
                                         'num_imei_msisdns': 0}
        assert parsed_json['conditions_breakdown']['duplicate_mk1'] == duplicate_condition_breakdown
        stolen_condition_breakdown = {'num_imeis': 1,
                                      'num_triplets': 1,
                                      'num_imei_gross_adds': 1,
                                      'num_imei_imsis': 1,
                                      'num_imei_msisdns': 1}
        assert parsed_json['conditions_breakdown']['local_stolen'] == stolen_condition_breakdown
        malformed_imei_condition_breakdown = {'num_imeis': 1,
                                              'num_triplets': 1,
                                              'num_imei_gross_adds': 1,
                                              'num_imei_imsis': 1,
                                              'num_imei_msisdns': 1}
        assert parsed_json['conditions_breakdown']['malformed_imei'] == malformed_imei_condition_breakdown

    operator4_operator_report = 'Country1_operator4_11_2016.json'
    with open(os.path.join(reports_dir, operator4_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        duplicate_condition_breakdown = {'num_imeis': 0,
                                         'num_triplets': 0,
                                         'num_imei_gross_adds': 0,
                                         'num_imei_imsis': 0,
                                         'num_imei_msisdns': 0}
        assert duplicate_condition_breakdown in parsed_json['historic_conditions_breakdown']['duplicate_mk1']
        stolen_condition_breakdown = {'num_imeis': 1,
                                      'num_triplets': 1,
                                      'num_imei_gross_adds': 1,
                                      'num_imei_imsis': 1,
                                      'num_imei_msisdns': 1}
        assert stolen_condition_breakdown in parsed_json['historic_conditions_breakdown']['local_stolen']
        malformed_imei_condition_breakdown = {'num_imeis': 1,
                                              'num_triplets': 1,
                                              'num_imei_gross_adds': 1,
                                              'num_imei_imsis': 1,
                                              'num_imei_msisdns': 1}
        assert malformed_imei_condition_breakdown in parsed_json['historic_conditions_breakdown']['malformed_imei']

        # Verify historic counts
        for i in range(0, 5):
            assert default_historic_compliance_breakdown == parsed_json['historic_compliance_breakdown'][i]
        # Verify current counts
        compliance_breakdown = {'num_compliant_imeis': 8,
                                'num_compliant_imei_imsis': 9,
                                'num_compliant_imei_msisdns': 9,
                                'num_compliant_triplets': 9,
                                'num_noncompliant_imeis': 4,
                                'num_noncompliant_imeis_blocking': 3,
                                'num_noncompliant_imeis_info_only': 1,
                                'num_noncompliant_imei_imsis': 5,
                                'num_noncompliant_imei_imsis_blocking': 3,
                                'num_noncompliant_imei_imsis_info_only': 1,
                                'num_noncompliant_imei_msisdns': 4,
                                'num_noncompliant_imei_msisdns_blocking': 4,
                                'num_noncompliant_imei_msisdns_info_only': 1,
                                'num_noncompliant_triplets': 5,
                                'num_noncompliant_triplets_blocking': 4,
                                'num_noncompliant_triplets_info_only': 1}
        assert parsed_json['compliance_breakdown'] == compliance_breakdown
        assert compliance_breakdown == parsed_json['historic_compliance_breakdown'][5]
        assert compliance_breakdown == parsed_json['historic_compliance_breakdown'][-1]

        duplicate_condition_breakdown = {'num_imeis': 0,
                                         'num_triplets': 0,
                                         'num_imei_gross_adds': 0,
                                         'num_imei_imsis': 0,
                                         'num_imei_msisdns': 0}
        assert parsed_json['conditions_breakdown']['duplicate_mk1'] == duplicate_condition_breakdown
        stolen_condition_breakdown = {'num_imeis': 1,
                                      'num_triplets': 1,
                                      'num_imei_gross_adds': 1,
                                      'num_imei_imsis': 1,
                                      'num_imei_msisdns': 1}
        assert parsed_json['conditions_breakdown']['local_stolen'] == stolen_condition_breakdown
        malformed_imei_condition_breakdown = {'num_imeis': 1,
                                              'num_triplets': 1,
                                              'num_imei_gross_adds': 1,
                                              'num_imei_imsis': 1,
                                              'num_imei_msisdns': 1}
        assert parsed_json['conditions_breakdown']['malformed_imei'] == malformed_imei_condition_breakdown


@pytest.mark.parametrize('operator_data_importer, gsma_tac_db_importer',
                         [(OperatorDataParams(
                             filename='operator1_with_rat_info_20160701_20160731.csv',
                             operator='operator1',
                             perform_null_checks=False,
                             perform_leading_zero_check=False,
                             perform_rat_import=True,
                             extract=False),
                           GSMADataParams(
                               filename='gsma_dump_rat_computation_check.txt'))],
                         indirect=True)
def test_top_10_models_null_model_manufacturer(postgres, db_conn, gsma_tac_db_importer, operator_data_importer,
                                               logger, tmpdir, mocked_config):
    """Test Depot ID Unknown.

    Regression test case for DIRBS-789. Verify that dirbs-report works correctly when the Top 10 models list
    contains a NULL model or manufacturer in the GSMA TAC DB.
    """
    import_data(operator_data_importer, 'operator_data', 9, db_conn, logger)
    import_data(gsma_tac_db_importer, 'gsma_data', 9, db_conn, logger)

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance - this step would normally fail if
    # the test regresses
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)

    assert result.exit_code == 0


@pytest.mark.parametrize('operator_data_importer',
                         [(OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False))],
                         indirect=True)
def test_imei_imsi_overloading(postgres, db_conn, operator_data_importer, tmpdir, logger, mocked_config):
    """Test Depot ID Unknown.

    Checks that the JSON report contains IMEI-IMSI overloading information
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)

    assert result.exit_code == 0

    expected = [{'num_imeis': 12, 'seen_with_imsis': 1}, {'num_imeis': 1, 'seen_with_imsis': 2}]
    report_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_11_2016.json'
    with open(os.path.join(report_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['imei_imsi_overloading'] == expected

    operator1_operator_report = 'Country1_operator1_11_2016.json'
    with open(os.path.join(report_dir, operator1_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['imei_imsi_overloading'] == expected

    operator2_operator_report = 'Country1_operator2_11_2016.json'
    with open(os.path.join(report_dir, operator2_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert 'imei_imsi_overloading' not in parsed_json


@pytest.mark.parametrize('operator_data_importer',
                         [(OperatorDataParams(
                             filename='testData1-operator-operator1-imsi-overloaded_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False))],
                         indirect=True)
def test_imsi_imei_overloading(postgres, db_conn, operator_data_importer, tmpdir, logger, mocked_config):
    """Test Depot ID Unknown.

    Checks that the JSON report contains IMSI-IMEI overloading information
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)

    assert result.exit_code == 0
    report_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_11_2016.json'
    expected = [{'num_imsis': 1, 'seen_with_imeis': 1},
                {'num_imsis': 1, 'seen_with_imeis': 4},
                {'num_imsis': 1, 'seen_with_imeis': 8}]
    with open(os.path.join(report_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['imsi_imei_overloading'] == expected

    operator1_operator_report = 'Country1_operator1_11_2016.json'
    with open(os.path.join(report_dir, operator1_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['imsi_imei_overloading'] == expected

    operator2_operator_report = 'Country1_operator2_11_2016.json'
    with open(os.path.join(report_dir, operator2_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert 'imsi_imei_overloading' not in parsed_json


@pytest.mark.parametrize('operator_data_importer',
                         [(OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_average_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False))],
                         indirect=True)
def test_daily_imei_imsi_overloading(postgres, db_conn, operator_data_importer, tmpdir, logger, mocked_config):
    """Test Depot ID Unknown.

    Checks that the JSON report contains average IMEI-IMSI overloading information
    """
    import_data(operator_data_importer, 'operator_data', 37, db_conn, logger)

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)
    assert result.exit_code == 0

    expected = [{'bin_end': 1.1,
                 'num_imeis': 2,
                 'bin_start': 1.0},
                {'bin_end': 1.5,
                 'num_imeis': 1,
                 'bin_start': 1.4},
                {'bin_end': 1.7,
                 'num_imeis': 1,
                 'bin_start': 1.6}]
    report_dir = os.path.join(output_dir, os.listdir(output_dir)[0])
    country_report = 'Country1_11_2016.json'
    with open(os.path.join(report_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['daily_imei_imsi_overloading'] == expected

    operator1_operator_report = 'Country1_operator1_11_2016.json'
    with open(os.path.join(report_dir, operator1_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['daily_imei_imsi_overloading'] == expected

    operator2_operator_report = 'Country1_operator2_11_2016.json'
    with open(os.path.join(report_dir, operator2_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert 'daily_imei_imsi_overloading' not in parsed_json


@pytest.mark.parametrize('operator_data_importer',
                         [(OperatorDataParams(
                             filename='testData1-operator-operator1-imsi-overloaded_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False))],
                         indirect=True)
def test_imei_overlap_reports(postgres, db_conn, operator_data_importer, tmpdir, logger, mocked_config,
                              metadata_db_conn, mocked_statsd):
    """Test Depot ID Unknown.

    Checks that the IMEI overlap reports are generated correctly
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)

    # Classify to generate gsma_not_found records
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # If we generate overlap reports after only generating data for one operator, there should be no overlap

    result = runner.invoke(dirbs_report_cli, ['condition_imei_overlaps', '--disable-retention-check',
                                              '--disable-data-check', '11', '2016', output_dir],
                           obj={'APP_CONFIG': mocked_config}, catch_exceptions=False)

    assert result.exit_code == 0
    report_dir = find_subdirectory_in_dir('report*', output_dir)
    file_list = os.listdir(report_dir)
    overlap_reports = fnmatch.filter(file_list, 'Country1_11_2016_condition_imei_overlap*.csv')
    for r in overlap_reports:
        with open(os.path.join(report_dir, r), 'r') as input_file:
            reader = csv.reader(input_file)
            rows = list(reader)
            # Just the header row
            assert len(rows) == 1

    # Delete all files so that subsequence dirbs_report_cli works
    for subdir in os.listdir(output_dir):
        shutil.rmtree(os.path.join(output_dir, subdir))

    # If we import the same file as a different operator, everything should overlap in the GSMA not found overlap
    # list
    _import_operator_data('testData1-operator-operator1-imsi-overloaded_20161101_20161130.csv', 'operator4', 34,
                          db_conn, metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}])

    result = runner.invoke(dirbs_report_cli, ['condition_imei_overlaps', '--disable-retention-check',
                                              '--disable-data-check', '11', '2016', output_dir],
                           catch_exceptions=False, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    report_dir = find_subdirectory_in_dir('report*', output_dir)
    with open(os.path.join(report_dir,
                           'Country1_11_2016_condition_imei_overlap_gsma_not_found.csv'), 'r') as input_file:
        reader = csv.reader(input_file)
        rows = list(reader)
        # All rows should overlap and should have operator1 and operator4 listed as the overlap
        assert len(rows) == 14
        for row in rows[1:]:
            assert row[1] == 'operator1|operator4'


@pytest.mark.parametrize('operator_data_importer, stolen_list_importer, pairing_list_importer, gsma_tac_db_importer',
                         [(OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False),
                           StolenListParams(
                               filename='testData1-sample_stolen_list-anonymized.csv'),
                           PairListParams(
                               filename='testData1-sample_pairinglist-anonymized.csv'),
                           GSMADataParams(
                               filename='testData1-gsmatac_operator4_operator1_anonymized.txt'))],
                         indirect=True)
def test_report_contains_condition_count_table(postgres, db_conn, gsma_tac_db_importer, operator_data_importer,
                                               stolen_list_importer, pairing_list_importer, tmpdir,
                                               metadata_db_conn, mocked_config, logger, mocked_statsd):
    """Test Depot ID Unknown.

    Verify operator and country reports include a summed up condition count table detailing counts for every
    combination of classification conditions found
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    import_data(gsma_tac_db_importer, 'gsma_data', 13, db_conn, logger)
    _import_operator_data('testData1-operator-operator4-anonymized_20161101_20161130.csv', 'operator4', 35, db_conn,
                          metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}])
    import_data(stolen_list_importer, 'stolen_list', 21, db_conn, logger)
    import_data(pairing_list_importer, 'pairing_list', 7, db_conn, logger)

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date=20161201',
                                                '--conditions=gsma_not_found,local_stolen,'
                                                'duplicate_mk1,malformed_imei'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '11',
                                              '2016', output_dir], catch_exceptions=False,
                           obj={'APP_CONFIG': mocked_config})

    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_11_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert 'condition_combination_table' in parsed_json
        tbl = parsed_json['condition_combination_table']
        expected_rows = \
            [{'combination': {'duplicate_mk1': False,
                              'gsma_not_found': False,
                              'inconsistent_rat': False,
                              'local_stolen': False,
                              'malformed_imei': False,
                              'not_on_registration_list': False},
              'compliance_level': 2,
              'num_imei_gross_adds': 16,
              'num_imei_imsis': 20,
              'num_imei_msisdns': 18,
              'num_imeis': 16,
              'num_subscriber_triplets': 20},
             {'combination': {'duplicate_mk1': False,
                              'gsma_not_found': False,
                              'inconsistent_rat': False,
                              'local_stolen': False,
                              'malformed_imei': True,
                              'not_on_registration_list': False},
              'compliance_level': 1,
              'num_imei_gross_adds': 2,
              'num_imei_imsis': 2,
              'num_imei_msisdns': 2,
              'num_imeis': 2,
              'num_subscriber_triplets': 2},
             {'combination': {'duplicate_mk1': False,
                              'gsma_not_found': False,
                              'inconsistent_rat': False,
                              'local_stolen': True,
                              'malformed_imei': False,
                              'not_on_registration_list': False},
              'compliance_level': 0,
              'num_imei_gross_adds': 1,
              'num_imei_imsis': 1,
              'num_imei_msisdns': 1,
              'num_imeis': 1,
              'num_subscriber_triplets': 1},
             {'combination': {'duplicate_mk1': False,
                              'gsma_not_found': True,
                              'inconsistent_rat': False,
                              'local_stolen': False,
                              'malformed_imei': False,
                              'not_on_registration_list': False},
              'compliance_level': 0,
              'num_imei_gross_adds': 3,
              'num_imei_imsis': 3,
              'num_imei_msisdns': 4,
              'num_imeis': 3,
              'num_subscriber_triplets': 4},
             {'combination': {'duplicate_mk1': False,
                              'gsma_not_found': True,
                              'inconsistent_rat': False,
                              'local_stolen': True,
                              'malformed_imei': False,
                              'not_on_registration_list': False},
              'compliance_level': 0,
              'num_imei_gross_adds': 1,
              'num_imei_imsis': 1,
              'num_imei_msisdns': 1,
              'num_imeis': 1,
              'num_subscriber_triplets': 1}]

        for expected_row in expected_rows:
            assert expected_row in tbl

    country_csv = 'Country1_11_2016_condition_counts.csv'
    expected_csv_headers = ['duplicate_mk1', 'gsma_not_found', 'local_stolen', 'not_on_registration_list',
                            'inconsistent_rat', 'malformed_imei', 'IMEI count', 'IMEI gross adds count',
                            'IMEI-IMSI count', 'IMEI-MSISDN count', 'Subscriber triplet count', 'Compliance Level']

    with open(os.path.join(reports_dir, country_csv), 'r') as file:
        csvr = csv.reader(file)
        lines = list(csvr)
        expected_rows = \
            [['False', 'False', 'False', 'False', 'False', 'False', '16', '16', '20', '18', '20', '2'],
             ['False', 'False', 'False', 'False', 'False', 'True', '2', '2', '2', '2', '2', '1'],
             ['False', 'False', 'True', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0'],
             ['False', 'True', 'False', 'False', 'False', 'False', '3', '3', '3', '4', '4', '0'],
             ['False', 'True', 'True', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0']]
        assert lines[0] == expected_csv_headers
        for line in lines[1:]:
            assert line in expected_rows

    operator1_operator_report = 'Country1_operator1_11_2016.json'
    with open(os.path.join(reports_dir, operator1_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert 'condition_combination_table' in parsed_json
        tbl = parsed_json['condition_combination_table']
        expected_rows = \
            [{'combination': {'duplicate_mk1': False,
                              'gsma_not_found': False,
                              'inconsistent_rat': False,
                              'local_stolen': False,
                              'malformed_imei': False,
                              'not_on_registration_list': False},
              'compliance_level': 2,
              'num_imei_gross_adds': 10,
              'num_imei_imsis': 11,
              'num_imei_msisdns': 11,
              'num_imeis': 10,
              'num_subscriber_triplets': 11},
             {'combination': {'duplicate_mk1': False,
                              'gsma_not_found': False,
                              'inconsistent_rat': False,
                              'local_stolen': False,
                              'malformed_imei': True,
                              'not_on_registration_list': False},
              'compliance_level': 1,
              'num_imei_gross_adds': 1,
              'num_imei_imsis': 1,
              'num_imei_msisdns': 1,
              'num_imeis': 1,
              'num_subscriber_triplets': 1},
             {'combination': {'duplicate_mk1': False,
                              'gsma_not_found': False,
                              'inconsistent_rat': False,
                              'local_stolen': True,
                              'malformed_imei': False,
                              'not_on_registration_list': False},
              'compliance_level': 0,
              'num_imei_gross_adds': 1,
              'num_imei_imsis': 1,
              'num_imei_msisdns': 1,
              'num_imeis': 1,
              'num_subscriber_triplets': 1},
             {'combination': {'duplicate_mk1': False,
                              'gsma_not_found': True,
                              'inconsistent_rat': False,
                              'local_stolen': False,
                              'malformed_imei': False,
                              'not_on_registration_list': False},
              'compliance_level': 0,
              'num_imei_gross_adds': 1,
              'num_imei_imsis': 1,
              'num_imei_msisdns': 1,
              'num_imeis': 1,
              'num_subscriber_triplets': 1}]

        for expected_row in expected_rows:
            assert expected_row in tbl

    operator1_csv = 'Country1_operator1_11_2016_condition_counts.csv'
    with open(os.path.join(reports_dir, operator1_csv), 'r') as file:
        csvr = csv.reader(file)
        lines = list(csvr)
        expected_rows = \
            [['False', 'False', 'False', 'False', 'False', 'False', '10', '10', '11', '11', '11', '2'],
             ['False', 'False', 'False', 'False', 'False', 'True', '1', '1', '1', '1', '1', '1'],
             ['False', 'False', 'True', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0'],
             ['False', 'True', 'False', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0']]
        assert lines[0] == expected_csv_headers
        for line in lines[1:]:
            assert line in expected_rows

    operator4_operator_report = 'Country1_operator4_11_2016.json'
    with open(os.path.join(reports_dir, operator4_operator_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert 'condition_combination_table' in parsed_json
        tbl = parsed_json['condition_combination_table']
        expected_rows = \
            [{'combination': {'duplicate_mk1': False,
                              'gsma_not_found': False,
                              'inconsistent_rat': False,
                              'local_stolen': False,
                              'malformed_imei': False,
                              'not_on_registration_list': False},
              'compliance_level': 2,
              'num_imei_gross_adds': 8,
              'num_imei_imsis': 9,
              'num_imei_msisdns': 9,
              'num_imeis': 8,
              'num_subscriber_triplets': 9},
             {'combination': {'duplicate_mk1': False,
                              'gsma_not_found': False,
                              'inconsistent_rat': False,
                              'local_stolen': False,
                              'malformed_imei': True,
                              'not_on_registration_list': False},
              'compliance_level': 1,
              'num_imei_gross_adds': 1,
              'num_imei_imsis': 1,
              'num_imei_msisdns': 1,
              'num_imeis': 1,
              'num_subscriber_triplets': 1},
             {'combination': {'duplicate_mk1': False,
                              'gsma_not_found': True,
                              'inconsistent_rat': False,
                              'local_stolen': False,
                              'malformed_imei': False,
                              'not_on_registration_list': False},
              'compliance_level': 0,
              'num_imei_gross_adds': 2,
              'num_imei_imsis': 2,
              'num_imei_msisdns': 3,
              'num_imeis': 2,
              'num_subscriber_triplets': 3},
             {'combination': {'duplicate_mk1': False,
                              'gsma_not_found': True,
                              'inconsistent_rat': False,
                              'local_stolen': True,
                              'malformed_imei': False,
                              'not_on_registration_list': False},
              'compliance_level': 0,
              'num_imei_gross_adds': 1,
              'num_imei_imsis': 1,
              'num_imei_msisdns': 1,
              'num_imeis': 1,
              'num_subscriber_triplets': 1}]
        for expected_row in expected_rows:
            assert expected_row in tbl

    operator4_csv = 'Country1_operator4_11_2016_condition_counts.csv'
    with open(os.path.join(reports_dir, operator4_csv), 'r') as file:
        csvr = csv.reader(file)
        lines = list(csvr)
        expected_rows = \
            [['False', 'False', 'False', 'False', 'False', 'False', '8', '8', '9', '9', '9', '2'],
             ['False', 'False', 'False', 'False', 'False', 'True', '1', '1', '1', '1', '1', '1'],
             ['False', 'True', 'False', 'False', 'False', 'False', '2', '2', '2', '3', '3', '0'],
             ['False', 'True', 'True', 'False', 'False', 'False', '1', '1', '1', '1', '1', '0']]
        assert lines[0] == expected_csv_headers
        for line in lines[1:]:
            assert line in expected_rows


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='OPerator1',
                             perform_unclean_checks=False,
                             extract=False)],
                         indirect=True)
def test_report_operator_id_uppercase(postgres, db_conn, metadata_db_conn, operator_data_importer,
                                      tmpdir, mocked_config, logger, mocked_statsd):
    """Test Depot ID not know yet.

    Verify operator and country data reports should include count of distinct triplet records seen per day.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    _import_operator_data('testData1-operator-operator4-anonymized_20161101_20161130.csv', 'operator4', 35, db_conn,
                          metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}])

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check',
                                              '11', '2016', output_dir], catch_exceptions=False,
                           obj={'APP_CONFIG': mocked_config})

    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_11_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        recs_per_day_records = {rec['date']: rec['count'] for rec in parsed_json['recs_per_day']}
        assert recs_per_day_records['2016-11-01'] == 2
        assert recs_per_day_records['2016-11-02'] == 2
        assert recs_per_day_records['2016-11-03'] == 1
        assert recs_per_day_records['2016-11-04'] == 3
        assert recs_per_day_records['2016-11-05'] == 1
        assert recs_per_day_records['2016-11-06'] == 1
        assert recs_per_day_records['2016-11-07'] == 3
        assert recs_per_day_records['2016-11-08'] == 1
        assert recs_per_day_records['2016-11-09'] == 1
        assert recs_per_day_records['2016-11-10'] == 2
        assert recs_per_day_records['2016-11-11'] == 2
        assert recs_per_day_records['2016-11-12'] == 2


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161105,111111013136460,111018001111111,223338000000\n'
                                     '20161105,211111060451101,111018001111111,223338000000\n'
                                     '20161105,211111060451100,111015111111111,223355000000\n'
                                     '20161105,311111060451100,111015111111111,223355000000\n'
                                     '20161105,411111060451100,111015111111111,223355000000\n'
                                     '20161105,511111013659809,111015111111111,223614000000',
                             operator='operator1',
                             cc=['22'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}],
                             perform_leading_zero_check=False,
                             extract=False)],
                         indirect=True)
@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(content='IMEI,reporting_date,status\n'
                                                   '111111013136460,20161104,\n'
                                                   '211111060451111,20161103,\n'
                                                   '211111060451100,20161103,')],
                         indirect=True)
def test_blacklist_stolen_violation_report(postgres, db_conn, metadata_db_conn, operator_data_importer,
                                           stolen_list_importer, tmpdir, mocked_config, logger, mocked_statsd,
                                           monkeypatch):
    """Test Depot ID not know yet.

    Verify that is possible to generate CSV blacklist stolen violations reports.
    like black-list violations but specialised
    Reports all IMEIs seen where IMEI is in stolen_list and
    monthly_network_triplets_per_mno.last_seen > stolen_list.reporting_date.
    Verify also that, in case of different IMEIs (211111060451111, 211111060451100)
    with same imei_norm, stolen violation report don't get repeated results  by merging them in one row.
    """
    # blacklist_violations_grace_period_days = 1
    # 111111013136460, 211111060451100 are in stolen_list
    # 211111060451100 has last_seen > reporting_date + blacklist_violations_grace_period_days and needs to be in report
    expect_success(operator_data_importer, 6, db_conn, logger)
    stolen_list_importer.import_data()

    monkeypatch.setattr(mocked_config.report_config, 'blacklist_violations_grace_period_days', 1)
    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['stolen_violations', output_dir],
                           obj={'APP_CONFIG': mocked_config})

    assert result.exit_code == 0
    expected_dir_list = ['stolen_violations_operator3.csv', 'stolen_violations_operator4.csv',
                         'stolen_violations_operator1.csv', 'stolen_violations_operator2.csv']

    fn = find_subdirectory_in_dir('report__stolen_violations*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    dir_list = os.listdir(dir_path)
    assert all([x in dir_list for x in expected_dir_list])
    with open(os.path.join(dir_path, 'stolen_violations_operator1.csv'), 'r') as input_file:
        reader = csv.reader(input_file)
        rows = list(reader)

    assert rows == [['imei_norm', 'last_seen', 'reporting_date'], ['21111106045110', '20161105', '20161103']]


@pytest.mark.parametrize('operator_data_importer, classification_data',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161105,111111013136460,111018001111111,223338000000\n'
                                     '20161105,211111013136460,111015111111111,223355000000\n'
                                     '20161105,311111013136460,111015111111111,223355000000\n'
                                     '20161105,411111013136460,111015111111111,223355000000\n'
                                     '20161105,511111060451100,111015111111111,223614000000',
                             operator='operator1',
                             cc=['22'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}],
                             perform_leading_zero_check=False,
                             extract=False),
                           'classification_state/imei_api_class_stolen_violation_report.csv')],
                         indirect=True)
@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(content='IMEI,reporting_date,status\n'
                                                   '111111013136460,20161102,\n'
                                                   '211111013136460,20161102,\n'
                                                   '311111013136460,20161102,\n'
                                                   '411111013136460,20161103,\n'
                                                   '511111060451100,20161103,')],
                         indirect=True)
def test_blacklist_stolen_filter_by_conditions(postgres, db_conn, metadata_db_conn, operator_data_importer,
                                               stolen_list_importer, tmpdir, mocked_config, logger, mocked_statsd,
                                               monkeypatch, classification_data):
    """Test Depot ID not know yet.

    Verify that is possible to generate CSV blacklist stolen violations reports filtered by conditions.
    --filter-by-condition <cond_name>, takes the results and JOINs against the classification_state table
    WHERE end_date IS NULL and cond_name is equal to the parameter passed.
    Verify also that we don't get repeated rows in case of duplicate imei_norm in classification_state table
    i.e.
    11111101313646,gsma_not_found,'2016-01-01',,'2016-11-20'
    11111101313646,local_stolen,'2016-01-02',,'2016-11-21'
    """
    # filter by conditions: gsma_not_found, local_stolen
    # only 2 rows have invalid conditions in classification_state:
    # 411111013136460, duplicate_mk1, '2016-01-01',, '2016-11-20'
    # 511111060451100, duplicate_mk1, '2016-01-01',, '2016-11-20'
    expect_success(operator_data_importer, 5, db_conn, logger)
    stolen_list_importer.import_data()

    monkeypatch.setattr(mocked_config.report_config, 'blacklist_violations_grace_period_days', 1)
    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)

    result = runner.invoke(dirbs_report_cli, ['stolen_violations',
                                              '--filter-by-conditions', 'gsma_not_found, local_stolen', output_dir],
                           obj={'APP_CONFIG': mocked_config})

    assert result.exit_code == 0

    fn = find_subdirectory_in_dir('report__stolen_violations*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    with open(os.path.join(dir_path, 'stolen_violations_operator1.csv'), 'r') as input_file:
        reader = csv.reader(input_file)
        rows = list(reader)

    assert len(rows) == 4
    exp_list = [['11111101313646', '20161105', '20161102'],
                ['31111101313646', '20161105', '20161102'],
                ['21111101313646', '20161105', '20161102']]
    assert all([x in rows for x in exp_list])


@pytest.mark.parametrize('operator_data_importer, classification_data',
                         [(OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20161105,111111013136460,111018001111111,223338000000\n'
                                     '20161106,211111013136460,111015111111111,223355000000\n'
                                     '20161107,311111013136460,111015111111111,223355000000\n'
                                     '20161107,411111013136460,111015111111111,223355000000\n'
                                     '20161107,511111060451100,111015111111111,223614000000',
                             operator='operator1',
                             cc=['22'],
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}],
                             perform_leading_zero_check=False,
                             extract=False),
                           'classification_state/imei_api_class_stolen_violation_report.csv')],
                         indirect=True)
@pytest.mark.parametrize('stolen_list_importer',
                         [StolenListParams(content='IMEI,reporting_date,status\n'
                                                   '111111013136460,20161102,\n'
                                                   '211111013136460,20161102,\n'
                                                   '311111013136460,20161102,\n'
                                                   '411111013136460,20161103,\n'
                                                   '511111060451100,20161103,')],
                         indirect=True)
def test_blacklist_stolen_newer_than_and_cond_options(postgres, db_conn, metadata_db_conn, operator_data_importer,
                                                      stolen_list_importer, tmpdir, mocked_config, logger,
                                                      monkeypatch, mocked_statsd, classification_data):
    """Test Depot ID not know yet.

    Verify that is possible to generate CSV blacklist stolen violations reports specifying options.
    Options available are:
    --newer-than option, which only includes violations newer than the date passed (filter network triplets)
    --filter-by-condition <cond_name>, takes the results and JOINs against the classification_state table
    WHERE end_date IS NULL and cond_name is equal to the parameter passed
    """
    # OPTION 1:--filter-by-condition:
    # filter by conditions: gsma_not_found, local_stolen
    # only 2 rows have invalid conditions in classification_state:
    # 411111013136460, duplicate_mk1, '2016-01-01',, '2016-11-20'
    # 511111060451100, duplicate_mk1, '2016-01-01',, '2016-11-20'

    # OPTION 2: --newer-than
    # newer than 20161105 date
    # only IMEI 111111013136460 is excluded because date 20161105 is older than 20161105
    expect_success(operator_data_importer, 5, db_conn, logger)
    stolen_list_importer.import_data()

    monkeypatch.setattr(mocked_config.report_config, 'blacklist_violations_grace_period_days', 1)
    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['stolen_violations', '--newer-than', '20161105',
                                              '--filter-by-conditions', 'gsma_not_found, local_stolen', output_dir],
                           obj={'APP_CONFIG': mocked_config})

    assert result.exit_code == 0

    fn = find_subdirectory_in_dir('report__stolen_violations*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    with open(os.path.join(dir_path, 'stolen_violations_operator1.csv'), 'r') as input_file:
        reader = csv.reader(input_file)
        rows = list(reader)

    assert len(rows) == 3
    exp_list = [['31111101313646', '20161107', '20161102'],
                ['21111101313646', '20161106', '20161102']]
    assert all([x in rows for x in exp_list])


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160702,10132222698280,11101400135251,22300825684694\n'
                                     '20160203,31111106045110,111025111111111,223355000000\n'
                                     '20160203,3511AAB1111110,111025111111111,223355000000\n'
                                     '20160203,3BAA0000000000,111025111111111,223614000000\n'
                                     '20160203,41111101365980,310035111111111,743614000000\n'
                                     '20160203,81111101313646,111018001111111,223338000000\n'
                                     '20160203,4111110139,310035111111112,743614000002\n'
                                     '20160203,81111101,111018001111112,223338000002\n'
                                     '20160203,311111060,111025111111112,223355000002\n'
                                     '20160704,40277777370026,,22300049781840\n'
                                     '20160704,,11101803062043,22300049781840\n'
                                     '20160704,30266666370026,11101803062043,',
                             operator='operator1',
                             cc=['22'],
                             perform_unclean_checks=False,
                             perform_null_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             perform_leading_zero_check=False,
                             extract=False)],
                         indirect=True)
def test_report_for_month_null_imei(postgres, db_conn, metadata_db_conn, operator_data_importer,
                                    tmpdir, logger, mocked_config):
    """Test Depot ID not know yet.

    Verify that no exception is thrown when running reports for a month that has
    nulls on imei or imsi or msisdn.
    """
    expect_success(operator_data_importer, 12, db_conn, logger)
    output_dir = str(tmpdir)
    runner = CliRunner()
    result = runner.invoke(dirbs_report_cli,
                           ['standard', '--disable-retention-check', '--disable-data-check', '--force-refresh',
                            '07', '2016', output_dir], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_7_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())

        recs_per_day_records = {rec['date']: rec['count'] for rec in parsed_json['recs_per_day']}
        assert recs_per_day_records['2016-07-02'] == 1
        assert recs_per_day_records['2016-07-04'] == 0

        imeis_per_day = {rec['date']: rec['count'] for rec in parsed_json['imeis_per_day']}
        assert imeis_per_day['2016-07-02'] == 1
        assert imeis_per_day['2016-07-04'] == 2


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False)],
                         indirect=True)
def test_make_report_directory(postgres, db_conn, operator_data_importer, logger, tmpdir, mocked_config):
    """Verifying that directory is created with proper name."""
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    run_id = operator_data_importer.import_id
    db_conn.commit()
    runner = CliRunner()

    # Run dirbs-classify so we have class_run_id not None
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check'], obj={'APP_CONFIG': mocked_config})
    run_id += 1
    assert result.exit_code == 0

    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['stolen_violations', output_dir],
                           obj={'APP_CONFIG': mocked_config})
    run_id += 1
    assert result.exit_code == 0

    # stolen_violation_directory
    # i.e. report__stolen_violations__20171102_051731__run_id_4__class_id_3
    fn = os.path.basename(find_subdirectory_in_dir('report__stolen_violations*', output_dir))
    run_id_start_time = job_start_time_by_run_id(db_conn, run_id).strftime('%Y%m%d_%H%M%S')
    class_run_id = _class_run_id(db_conn, mocked_config)
    assert fn.startswith('report__stolen_violations__{0}'.format(run_id_start_time))
    assert fn.endswith('run_id_{0}__class_id_{1}'.format(run_id, class_run_id))

    # standard
    # i.e. report__standard__20171102_052206__run_id_5__class_id_3__data_id_1__month_7__year_2016
    result = runner.invoke(dirbs_report_cli,
                           ['standard', '--disable-retention-check', '--disable-data-check', '--force-refresh',
                            '07', '2016', output_dir], obj={'APP_CONFIG': mocked_config})
    run_id += 1
    assert result.exit_code == 0

    fn = os.path.basename(find_subdirectory_in_dir('report__standard*', output_dir))
    run_id_start_time = job_start_time_by_run_id(db_conn, run_id).strftime('%Y%m%d_%H%M%S')
    class_run_id = _class_run_id(db_conn, mocked_config)
    assert fn.startswith('report__standard__{0}__run_id_{1}__class_id_{2}__data_id_1__month_7__year_2016'
                         .format(run_id_start_time, run_id, class_run_id))
    # condition_imei_overlaps
    # i.e. report__condition_imei_overlaps__20171102_052800__run_id_6__class_id_3__month_11__year_2016
    result = runner.invoke(dirbs_report_cli, ['condition_imei_overlaps', '--disable-retention-check',
                                              '--disable-data-check',
                                              '11', '2016', output_dir], catch_exceptions=False,
                           obj={'APP_CONFIG': mocked_config})
    run_id += 1
    assert result.exit_code == 0
    fn = os.path.basename(find_subdirectory_in_dir('report__condition_imei_overlaps*', output_dir))
    run_id_start_time = job_start_time_by_run_id(db_conn, run_id).strftime('%Y%m%d_%H%M%S')
    class_run_id = _class_run_id(db_conn, mocked_config)
    assert fn.startswith('report__condition_imei_overlaps__{0}__run_id_{1}'.format(run_id_start_time, run_id))
    assert fn.endswith('class_id_{0}__month_11__year_2016'.format(class_run_id))


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20160702,AA,11101400135251,22300825684694',
                             operator='operator1',
                             perform_unclean_checks=False,
                             perform_null_checks=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             perform_leading_zero_check=False,
                             extract=False)],
                         indirect=True)
def test_condition_last_run_time(db_conn, operator_data_importer, mocked_config, logger, tmpdir, monkeypatch):
    """Test Depot ID Unknown.

    Verify that the JSON standard monthly report contains the last successful run time for each condition.
    """
    with db_conn as conn:
        import_data(operator_data_importer, 'operator_data', 1, conn, logger)

    cond_dict_list = [{
        'label': 'c1',
        'reason': 'stolen',
        'dimensions': [{'module': 'stolen_list'}]
    }, {
        'label': 'c2',
        'reason': 'malformed',
        'dimensions': [{'module': 'malformed_imei'}]
    }]

    cond_list = from_cond_dict_list_to_cond_list(cond_dict_list)
    monkeypatch.setattr(mocked_config, 'conditions', cond_list)

    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '07',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)
    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_7_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        conditions_last_successful_run = {c['label']: c['last_successful_run']
                                          for c in parsed_json['classification_conditions']}
        assert conditions_last_successful_run['c1'] is None
        assert conditions_last_successful_run['c2'] is None

    # Run dirbs-classify once with no-safety-check
    first_run_results = invoke_cli_classify_with_conditions_helper(cond_dict_list, mocked_config, monkeypatch,
                                                                   db_conn=db_conn,
                                                                   classify_options=['--no-safety-check'])
    assert first_run_results == ['AA']

    # Sleep inserted to make sure the time for each classification run is different by a second (result
    # is stored in report in seconds)
    time.sleep(1)

    # Run dirbs-classify once *without* no-safety-check (should fail as 100% of IMEIs fail c2 condition)
    invoke_cli_classify_with_conditions_helper(cond_dict_list, mocked_config, monkeypatch,
                                               db_conn=db_conn, expect_success=False)

    # We now run dirbs-report to check that the JSON report has the last run time for each condition
    #
    # - C1's last run time should be for the second dirbs-classify run as it always succeeds (0 matched IMEIs
    #   since no stolen list imported)
    # - C2's last run time should be for the first dirbs-classify run as safety check failed on the second
    #   run
    one_output_dir = os.path.join(output_dir, 'one')
    os.makedirs(one_output_dir, exist_ok=True)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '07',
                                              '2016', one_output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)
    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', one_output_dir)

    # Query start time for two classification jobs
    job_metadata_list = query_for_command_runs(conn, 'dirbs-classify')
    assert len(job_metadata_list) == 2
    job_start_time_dict = {job.run_id: job.start_time for job in job_metadata_list}
    first_run_start_time = job_start_time_dict[min(job_start_time_dict.keys())]
    second_run_start_time = job_start_time_dict[max(job_start_time_dict.keys())]

    results = most_recently_run_condition_info(db_conn, ['c1', 'c2'])
    assert results['c1']['last_successful_run'] == second_run_start_time
    assert results['c2']['last_successful_run'] == first_run_start_time
    assert results['c1']['last_successful_run'] != results['c2']['last_successful_run']

    country_report = 'Country1_7_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        conditions_last_successful_run = {c['label']: c['last_successful_run']
                                          for c in parsed_json['classification_conditions']}
        assert format_datetime_for_report(first_run_start_time) == conditions_last_successful_run['c2']
        assert format_datetime_for_report(second_run_start_time) == conditions_last_successful_run['c1']


def _class_run_id(conn, config):
    cond_run_info = most_recently_run_condition_info(conn, [c.label for c in config.conditions],
                                                     successful_only=True)
    if not cond_run_info:
        return None
    else:
        return max([v['run_id'] for k, v in cond_run_info.items()])


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False)],
                         indirect=True)
def test_total_gross_adds_count(postgres, db_conn, metadata_db_conn, operator_data_importer, tmpdir, mocked_config,
                                logger, mocked_statsd):
    """Test Depot ID not known yet.

    Verify total gross adds count.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    _import_operator_data('testData1-operator-operator1-anonymized_20161201_20161230.csv', 'operator1', 21, db_conn,
                          metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}], perform_historic_checks=False)

    # This commit is required as the CliRunner below will use a different DB connection
    # and the imported data needs to be committed so that it is visible to that other connection
    db_conn.commit()

    # Run dirbs-report using db args from the temp postgres instance
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '12',
                                              '2016', output_dir], obj={'APP_CONFIG': mocked_config},
                           catch_exceptions=False)

    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_12_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert parsed_json['total_imeis_seen'] == 4
        # gross adds
        # first file 17 rows
        # second file is made of 4 rows:
        # 2 row are the same as the first file
        # 2 are different
        assert parsed_json['total_gross_adds'] == 2


@pytest.mark.parametrize('operator_data_importer, stolen_list_importer',
                         [(OperatorDataParams(
                             filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                             operator='operator1',
                             perform_unclean_checks=False,
                             extract=False),
                           StolenListParams(
                               filename='sample_stolen_import_list_anonym.csv'))],
                         indirect=True)
def test_per_tac_gross_adds(postgres, db_conn, operator_data_importer,
                            stolen_list_importer, tmpdir, metadata_db_conn, mocked_config, logger, mocked_statsd):
    """Test Depot ID Unknown.

    Verify per TAC num_imei_gross_adds and num_imeis.
    We import 2 operator files:
    the first one has 17 rows
    the second one has 4 rows, 2 rows contain the same IMEIs as the first operator file.
    we import one stolen list file containing all the IMEIs of the second operator file

    we expect 'total_imeis_seen' == 4 and 'total_gross_adds' == 2 but are both 4

    However, total_imeis_seen and total_gross_adds are correct as the other test above
    called test_total_gross_adds_count verifies that total_imeis_seen is passing.
    """
    import_data(operator_data_importer, 'operator_data', 17, db_conn, logger)
    import_data(stolen_list_importer, 'stolen_list', 4, db_conn, logger)

    _import_operator_data('testData1-operator-operator1-anonymized_20161201_20161230.csv', 'operator1', 21, db_conn,
                          metadata_db_conn, mocked_config.db_config, tmpdir, logger, mocked_statsd,
                          mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}], perform_historic_checks=False)
    db_conn.commit()
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check', '--curr-date=20161201',
                                                '--conditions=local_stolen'],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check', '12',
                                              '2016', output_dir], catch_exceptions=False,
                           obj={'APP_CONFIG': mocked_config})

    assert result.exit_code == 0
    reports_dir = find_subdirectory_in_dir('report*', output_dir)
    country_report = 'Country1_12_2016.json'
    with open(os.path.join(reports_dir, country_report), 'r') as file:
        parsed_json = json.loads(file.read())
        assert 'condition_combination_table' in parsed_json
        tbl = parsed_json['condition_combination_table']
        expected_rows = \
            [{'num_imei_gross_adds': 2,
              'num_imei_imsis': 4,
              'num_imei_msisdns': 4,
              'combination': {'inconsistent_rat': False,
                              'not_on_registration_list': False,
                              'malformed_imei': False,
                              'gsma_not_found': False,
                              'local_stolen': True,
                              'duplicate_mk1': False},
              'num_subscriber_triplets': 4,
              'compliance_level': 0,
              'num_imeis': 4}]

        assert expected_rows == tbl


@pytest.mark.parametrize('pairing_list_importer',
                         [PairListParams(content='imei,imsi,msisdn\n'
                                                 '12345678901230,11107678901234,555555555555555\n'
                                                 '12345678901231,11108678901234,555555555555556\n'
                                                 '12345678901232,11109678901234,555555555555557\n'
                                                 '12345678901233,11101678901234,555555555555558\n'
                                                 '12345678901234,11101678901234,555555555555559\n'
                                                 '12345678901235,11102678901234,555555555555550\n'
                                                 '12345678901236,11102678901234,555555555555545\n')],
                         indirect=True)
@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20190221,12345678901228,11105678901234,1\n'
                                     '20190121,12345678901229,11106678901234,1\n'
                                     '20190121,12345678901230,11107678901234,1\n'
                                     '20180812,12345678901230,11107678901234,1\n'
                                     '20180812,12345678901231,11108678901234,1\n'
                                     '20180812,12345678901232,11109678901234,1',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_leading_zero_check=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
def test_non_active_pairs_generation(per_test_postgres, mocked_config, logger, monkeypatch,
                                     tmpdir, db_conn, pairing_list_importer, operator_data_importer):
    """Verify that the non-active pair list generation functionality works correctly."""
    operator_data_importer.import_data()
    pairing_list_importer.import_data()

    # Run dirbs-report using cli
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli, ['non_active_pairs', '10', output_dir], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    fn = find_subdirectory_in_dir('report__non_active_pairs*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    files = glob('{0}/*.csv'.format(dir_path))
    with open(files[0], 'r') as pairs_file:
        reader = csv.reader(pairs_file)
        rows = list(reader)

    assert len(rows) == 5
    expected_list = [['imei_norm', 'imsi', 'last_seen'],
                     ['12345678901230', '11107678901234', '2018-08-12'],
                     ['12345678901232', '11109678901234', '2018-08-12'],
                     ['12345678901231', '11108678901234', '2018-08-12'],
                     ['12345678901230', '11107678901234', '2019-01-21']]
    assert all([x in rows for x in expected_list])


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(content='uid,imsi\n'
                                                        'uid-01-sub,11118978901234\n'
                                                        'uid-02-sub,11119978901234\n'
                                                        'uid-03-sub,11128978901234\n')],
                         indirect=True)
@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20190221,12345678901228,11105678901234,1\n'
                                     '20190121,12345678901229,11106678901234,1\n'
                                     '20190121,12345678901230,11107678901234,1\n'
                                     '20180812,12345678901230,11107678901234,1\n'
                                     '20180812,12345678901231,11108678901234,1\n'
                                     '20180812,12345678901232,11109678901234,1',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_leading_zero_check=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
def test_unregistered_subscribers_generation(per_test_postgres, mocked_config, logger, monkeypatch,
                                             tmpdir, db_conn, subscribers_list_importer, operator_data_importer):
    """Verify that the unregistered_subscribers report works correctly."""
    # import data
    operator_data_importer.import_data()
    subscribers_list_importer.import_data()

    # run dirbs cli
    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli,
                           ['unregistered_subscribers', output_dir],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    fn = find_subdirectory_in_dir('report__unregistered_subscribers*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    with open('{0}/unregistered_subscribers_operator1.csv'.format(dir_path), 'r') as fn:
        reader = csv.reader(fn)
        rows = list(reader)

    assert len(rows) == 7
    expected_list = [['imsi', 'first_seen', 'last_seen'],
                     ['11107678901234', '20180812', '20180812'],
                     ['11109678901234', '20180812', '20180812'],
                     ['11108678901234', '20180812', '20180812'],
                     ['11107678901234', '20190121', '20190121'],
                     ['11106678901234', '20190121', '20190121'],
                     ['11105678901234', '20190221', '20190221']]
    assert all([x in rows for x in expected_list])


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(content='uid,imsi\n'
                                                        'uid-01-sub,11105678901234\n'
                                                        'uid-02-sub,11108678901234\n'
                                                        'uid-03-sub,11109678901234\n')],
                         indirect=True)
@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20190221,12345678901228,11105678901234,1\n'
                                     '20190121,12345678901229,11106678901234,1\n'
                                     '20190121,12345678901230,11107678901234,1\n'
                                     '20180812,12345678901230,11107678901234,1\n'
                                     '20180812,12345678901231,11108678901234,1\n'
                                     '20180812,12345678901232,11109678901234,1',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_leading_zero_check=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
def test_unregistered_subscribers_with_newer_than_option(per_test_postgres, mocked_config, logger, monkeypatch,
                                                         tmpdir, db_conn, subscribers_list_importer,
                                                         operator_data_importer):
    """Verify unregsitered_subscribers_list generation with newer-than parameter."""
    operator_data_importer.import_data()
    subscribers_list_importer.import_data()

    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli,
                           ['unregistered_subscribers', '--newer-than', '20190120', output_dir],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    fn = find_subdirectory_in_dir('report__unregistered_subscribers*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    with open('{0}/unregistered_subscribers_operator1.csv'.format(dir_path), 'r') as fn:
        reader = csv.reader(fn)
        rows = list(reader)

    assert len(rows) == 3
    expected_list = [['imsi', 'first_seen', 'last_seen'],
                     ['11107678901234', '20190121', '20190121'],
                     ['11106678901234', '20190121', '20190121']]
    assert all([x in rows for x in expected_list])
    assert ['11107678901234', '20180812', '20180812'] not in rows


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20190221,12345678901228,11105678901234,1\n'
                                     '20190121,12345678901229,11106678901234,1\n'
                                     '20190121,12345678901230,11107678901234,1\n'
                                     '20180812,12345678901230,11107678901234,1\n'
                                     '20180812,12345678901231,11108678901234,1\n'
                                     '20180812,12345678901232,11109678901234,1',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_leading_zero_check=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
def test_classified_triplets_single_condition(per_test_postgres, mocked_config, logger, monkeypatch,
                                              tmpdir, db_conn, operator_data_importer):
    """Verify classified_triplets report list generation with single condition as argument."""
    operator_data_importer.import_data()

    cli_runner = CliRunner()
    output_dir = str(tmpdir)
    cond_list = [{
        'label': 'gsma_not_found',
        'grace_period_days': 30,
        'blocking': True,
        'sticky': False,
        'reason': 'Not found in gsma',
        'dimensions': [{'module': 'gsma_not_found'}]
    }]

    classified_imeis_list = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                       curr_date='20190201', db_conn=db_conn,
                                                                       classify_options=['--no-safety-check'])
    assert classified_imeis_list == ['12345678901229', '12345678901230', '12345678901231', '12345678901232']

    # invoke report runner
    result = cli_runner.invoke(dirbs_report_cli,
                               ['classified_triplets', 'gsma_not_found', output_dir],
                               obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    fn = find_subdirectory_in_dir('report__classified_triplets*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    with open('{0}/classified_triplets_gsma_not_found.csv'.format(dir_path), 'r') as fn:
        reader = csv.reader(fn)
        rows = list(reader)

    assert len(rows) == 6
    expected_list = [['imei', 'imsi', 'msisdn', 'operator'],
                     ['12345678901229', '11106678901234', '1', 'operator1'],
                     ['12345678901230', '11107678901234', '1', 'operator1'],
                     ['12345678901230', '11107678901234', '1', 'operator1'],
                     ['12345678901231', '11108678901234', '1', 'operator1'],
                     ['12345678901232', '11109678901234', '1', 'operator1']]
    assert all([x in rows for x in expected_list])


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20190221,12345678901228,11105678901234,1\n'
                                     '20190121,12345678901229,11106678901234,1\n'
                                     '20190121,12345678901230,11107678901234,1\n'
                                     '20180812,12345678901230,11107678901234,1\n'
                                     '20180812,12345678901231,11108678901234,1\n'
                                     '20180812,12345678901232,11109678901234,1',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_leading_zero_check=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
def test_classified_triplets_multiple_conditions(per_test_postgres, mocked_config, logger, monkeypatch,
                                                 tmpdir, db_conn, operator_data_importer):
    """Verify classified_triplets report list generation with multiple conditions as argument."""
    operator_data_importer.import_data()

    cli_runner = CliRunner()
    output_dir = str(tmpdir)
    cond_list = [{
        'label': 'gsma_not_found',
        'grace_period_days': 30,
        'blocking': True,
        'sticky': False,
        'reason': 'Not found in gsma',
        'dimensions': [{'module': 'gsma_not_found'}
                       ]
    },
        {'label': 'not_on_registration_list',
         'grace_period_days': 30,
         'blocking': True,
         'sticky': False,
         'reason': 'Not found in reg list',
         'dimensions': [{'module': 'not_on_registration_list'}]
         }
    ]

    classified_imeis_list = invoke_cli_classify_with_conditions_helper(cond_list, mocked_config, monkeypatch,
                                                                       curr_date='20190201', db_conn=db_conn,
                                                                       classify_options=['--no-safety-check'])
    assert classified_imeis_list == ['12345678901228', '12345678901229', '12345678901229',
                                     '12345678901230', '12345678901230', '12345678901231',
                                     '12345678901231', '12345678901232', '12345678901232']

    # invoke report runner
    result = cli_runner.invoke(dirbs_report_cli,
                               ['classified_triplets', 'gsma_not_found,not_on_registration_list', output_dir],
                               obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    fn = find_subdirectory_in_dir('report__classified_triplets*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    path, dirs, files = next(os.walk(dir_path))

    assert len(files) == 2  # verify that two seperate files are generated
    assert 'classified_triplets_not_on_registration_list.csv' in files
    assert 'classified_triplets_gsma_not_found.csv' in files

    for fn in files:
        with open('{0}/{1}'.format(dir_path, fn), 'r') as fn:
            reader = csv.reader(fn)
            rows = list(reader)

        expected_list = [['imei', 'imsi', 'msisdn', 'operator'],
                         ['12345678901229', '11106678901234', '1', 'operator1'],
                         ['12345678901230', '11107678901234', '1', 'operator1'],
                         ['12345678901230', '11107678901234', '1', 'operator1'],
                         ['12345678901231', '11108678901234', '1', 'operator1'],
                         ['12345678901232', '11109678901234', '1', 'operator1']]
        assert all([x in rows for x in expected_list])


@pytest.mark.parametrize('subscribers_list_importer',
                         [SubscribersListParams(content='uid,imsi\n'
                                                        'uid-01-sub,11105678901234\n'
                                                        'uid-02-sub,11106678901235')],
                         indirect=True)
@pytest.mark.parametrize('device_association_list_importer',
                         [DeviceAssociationListParams(content='uid,imei\n'
                                                              'uid-01-sub,12345678901228\n'
                                                              'uid-02-sub,12345678901229')],
                         indirect=True)
@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             content='date,imei,imsi,msisdn\n'
                                     '20190221,12345678901228,11105678901234,1\n'
                                     '20190121,12345678901229,11106678901235,1\n'
                                     '20190222,12345678901230,11107678901236,1\n'
                                     '20190212,12345678901231,11107678901237,1\n'
                                     '20190212,12345678901232,11108678901238,1\n'
                                     '20190212,12345678901233,11109678901239,1',
                             extract=False,
                             perform_unclean_checks=False,
                             perform_leading_zero_check=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             operator='operator1'
                         )],
                         indirect=True)
def test_association_list_violations(per_test_postgres, mocked_config, logger, monkeypatch,
                                     tmpdir, db_conn, subscribers_list_importer,
                                     operator_data_importer, device_association_list_importer):
    """Verify that the association list violation reporting works correctly."""
    operator_data_importer.import_data()
    subscribers_list_importer.import_data()
    device_association_list_importer.import_data()

    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli,
                           ['association_list_violations', '02', '2019', output_dir],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    fn = find_subdirectory_in_dir('report__association_list_violations*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    with open('{0}/association_violations_operator1.csv'.format(dir_path), 'r') as fn:
        reader = csv.reader(fn)
        rows = list(reader)

    assert len(rows) == 5
    expected_list = [['imei', 'imsi', 'msisdn', 'first_seen', 'last_seen'],
                     ['12345678901232', '11108678901238', '1', '2019-02-12', '2019-02-12'],
                     ['12345678901230', '11107678901236', '1', '2019-02-22', '2019-02-22'],
                     ['12345678901233', '11109678901239', '1', '2019-02-12', '2019-02-12'],
                     ['12345678901231', '11107678901237', '1', '2019-02-12', '2019-02-12']]
    assert all([x in rows for x in expected_list])


@pytest.mark.parametrize('operator_data_importer',
                         [OperatorDataParams(
                             filename='transient_msisdn_operator1_20201201_20201231.csv',
                             operator='operator1',
                             extract=False,
                             perform_leading_zero_check=False,
                             mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                             perform_unclean_checks=False,
                             perform_file_daterange_check=False,
                             perform_region_checks=False,
                             perform_home_network_check=False,
                             perform_historic_checks=False
                         )],
                         indirect=True)
def test_transient_msisdns(per_test_postgres, mocked_config, logger, monkeypatch,
                           tmpdir, db_conn, operator_data_importer):
    """Verify the transient msisdns reporting functionality."""
    operator_data_importer.import_data()

    runner = CliRunner()
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_report_cli,
                           ['transient_msisdns', '30', '3', '--current-date', '20201231', output_dir],
                           obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    fn = find_subdirectory_in_dir('report__transient_msisdns*', output_dir)
    dir_path = os.path.join(output_dir, fn)
    with open('{0}/transient_msisdns_operator1.csv'.format(dir_path), 'r') as fn:
        reader = csv.reader(fn)
        rows = list(reader)

    assert len(rows) == 2
    expected_res = [['msisdn'],
                    ['2210011111111']]
    assert all([x in rows for x in expected_res])
