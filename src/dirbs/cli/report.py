"""
DIRBS CLI for report generation (Operator, Country). Installed by setuptools as a dirbs-report console script.

Copyright (c) 2018-2019 Qualcomm Technologies, Inc.

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
  If you use this software in a product, an acknowledgment is required by displaying the trademark/log as per the
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
import datetime
import hashlib
import logging
import pkgutil
import os
import sys
import csv
import contextlib

from dateutil import relativedelta
from psycopg2 import sql
import click

from dirbs import report_schema_version
from dirbs.reports import CountryReport, OperatorReport, generate_monthly_report_stats
import dirbs.cli.common as common
from dirbs.config import OperatorConfig
import dirbs.metadata as metadata
import dirbs.utils as utils
import dirbs.reports.exceptions as exceptions
from dirbs.dimensions.gsma_not_found import GSMANotFound
import dirbs.partition_utils as partition_utils


def _gen_metadata_for_reports(filenames, output_dir):
    """
    Function to generate a metadata dictionary for a list file pointer and a passed number of records.

    :param filenames: list of file names
    :param output_dir: output directory path
    :return: dict
    """
    rv = []
    for fn in filenames:
        abs_fn = os.path.join(output_dir, fn)
        file_size = os.stat(abs_fn).st_size
        md5_hash = hashlib.md5()
        md5_hash.update(open(abs_fn, 'rb').read())
        md5sum = md5_hash.hexdigest()
        rv.append({
            'filename': os.path.abspath(abs_fn),
            'md5sum': md5sum,
            'file_size_bytes': file_size
        })
    return rv


def _parse_month_year_report_options_args(f):
    """
    Decorator used to parse all the monthly, year command line options and update the config.

    :param f: obj
    :return: obj
    """
    f = _parse_force_refresh(f)
    f = _parse_disable_retention_check(f)
    f = _parse_disable_data_check(f)
    f = _parse_debug_query_performance(f)
    f = _parse_output_dir(f)
    f = _parse_year(f)
    f = _parse_month(f)
    return f


def _parse_month(f):
    """
    Function to parse month option on the command line.

    :param f: obj
    :return: click arg obj
    """
    return click.argument('month',
                          type=int,
                          callback=_validate_month)(f)


def _parse_year(f):
    """
    Function to parse year option on the command line.

    :param f: obj
    :return: click arg obj
    """
    return click.argument('year',
                          type=int,
                          callback=_validate_year)(f)


def _parse_output_dir(f):
    """
    Function to parse output dir option on the command line.

    :param f: obj
    :return: click arg obj
    """
    return click.argument('output_dir',
                          type=click.Path(exists=True, file_okay=False, writable=True))(f)


def _parse_force_refresh(f):
    """
    Function to parse force refresh option on the command line.

    :param f: obj
    :return: click option obj
    """
    return click.option('--force-refresh/--no-refresh',
                        default=True,
                        is_flag=True,
                        help='Whether data in report should be refreshed from latest data or '
                             'from previously-calculated data (default: --no-refresh).')(f)


def _parse_disable_retention_check(f):
    """
    Function to parse disable retention check option on the command line.

    :param f: obj
    :return: click option obj
    """
    return click.option('--disable-retention-check',
                        default=False,
                        is_flag=True,
                        help='Disable check that stops reports being run for months outside the retention period.')(f)


def _parse_disable_data_check(f):
    """
    Function to parse disable data check option on the command line.

    :param f: obj
    :return: click option obj
    """
    return click.option('--disable-data-check',
                        default=False,
                        is_flag=True,
                        help='Disable check to validate existence of data for all configured operators in this '
                             'reporting month.')(f)


def _parse_debug_query_performance(f):
    """
    Function to parse debug query performance option on the command line.

    :param f: obj
    :return: click option obj
    """
    return click.option('--debug-query-performance',
                        default=False,
                        is_flag=True,
                        help='Enable this to print out more stats about duration of queries during stats '
                             'generation.')(f)


def _validate_month(ctx, param, val):
    """
    Helper function to validate a month coming from the CLI.

    :param ctx: current cli context
    :param param: param
    :param val: month value
    :return: validated month value
    """
    if val < 1 or val > 12:
        raise click.BadParameter('Month must be between 1 and 12')
    return val


def _validate_year(ctx, param, val):
    """
    Helper function to validate a year coming from the CLI.

    :param ctx: current cli context
    :param param: param
    :param val: year value
    :return: validated year value
    """
    if val < 2000 or val > 2100:
        raise click.BadParameter('Year must be between 2000 and 2100')
    return val


def _write_report(report, month, year, output_dir, filename_prefix, css_filename, js_filename,
                  per_tac_compliance_data):
    """
    Helper function to write an individual report to disk.

    :param report: report type
    :param month: reporting month
    :param year: reporting year
    :param output_dir: output dir path
    :param filename_prefix: file name prefix
    :param css_filename: stylesheet file name
    :param js_filename: js file name
    :param per_tac_compliance_data: per tac compliance flag
    :return: metadata obj
    """
    # Generate the raw data
    logger = logging.getLogger('dirbs.report')
    data = report.gen_report_data()

    data_filename = '{0}_{1:d}_{2:d}.json'.format(filename_prefix, month, year)
    html_filename = '{0}_{1:d}_{2:d}.html'.format(filename_prefix, month, year)
    per_tac_csv_filename = '{0}_{1:d}_{2:d}.csv'.format(filename_prefix, month, year)
    condition_counts_filename = '{0}_{1:d}_{2:d}_condition_counts.csv'.format(filename_prefix, month, year)

    # Store a list of generate filenames so we can generate metadata
    generated_filenames = [html_filename, data_filename]

    # Write the raw JSON data to disk
    json_data = json.dumps(data, indent=4, sort_keys=True, cls=utils.JSONEncoder).encode('utf-8')
    with open(os.path.join(output_dir, data_filename), 'wb') as of:
        of.write(json_data)

    # Write the CSV per-TAC compliance data to disk
    if data['has_data']:
        condition_labels = [c['label'] for c in data['classification_conditions']]
        condition_table_headers = condition_labels + \
            ['IMEI count', 'IMEI gross adds count', 'IMEI-IMSI count', 'IMEI-MSISDN count', 'Subscriber triplet count',
             'Compliance Level']
        value_keys = ['num_imeis', 'num_imei_gross_adds', 'num_imei_imsis', 'num_imei_msisdns',
                      'num_subscriber_triplets', 'compliance_level']
        if per_tac_compliance_data is not None:
            with open(os.path.join(output_dir, per_tac_csv_filename), 'w', encoding='utf8') as of:
                writer = csv.writer(of)
                writer.writerow(['TAC'] + condition_table_headers)
                for tac, combinations in per_tac_compliance_data.items():
                    for combination, compliance_stats in combinations.items():
                        combination_list = list(combination)
                        writer.writerow([tac] + combination_list +
                                        [compliance_stats[key] for key in value_keys])
            generated_filenames.append(per_tac_csv_filename)
        else:
            logger.warning('No per-TAC compliance data will be output to CSV file, as compliance data was not '
                           'calculated or is empty')

    # Write the CSV condition combination data to disk
    condition_combination_table = data.get('condition_combination_table')
    if condition_combination_table is not None:
        with open(os.path.join(output_dir, condition_counts_filename), 'w', encoding='utf8') as of:
            writer = csv.writer(of)
            writer.writerow(condition_table_headers)
            for combination in data['condition_combination_table']:
                combination_list = [combination['combination'][label] for label in condition_labels]
                writer.writerow(combination_list + [combination[key] for key in value_keys])
        generated_filenames.append(condition_counts_filename)
    else:
        logger.warning('No condition counts table data will be output to CSV file, as table data is empty')

    # Generate the HTML report
    html = report.gen_html_report(data, css_filename, js_filename)

    # Write the HTML file to disk
    with open(os.path.join(output_dir, html_filename), 'wb') as of:
        of.write(html)

    return _gen_metadata_for_reports(generated_filenames, output_dir)


def _validate_data_partitions(config, conn, month, year, logger, disable_data_check):
    """
    Validate that data is present for all configured operators and only configured operators.

    :param config: dirbs config obj
    :param conn: database conection
    :param month: data month
    :param year: data year
    :param logger: dirbs logger obj
    :param disable_data_check: data check flag
    """
    operators = config.region_config.operators
    assert len(operators) > 0

    operator_partitions = utils.child_table_names(conn, 'monthly_network_triplets_per_mno')
    observed_operator_ids = {x for x in utils.table_invariants_list(conn, operator_partitions, ['operator_id'])}
    required_operator_ids = {(o.id,) for o in operators}
    missing_operator_ids = required_operator_ids - observed_operator_ids
    if len(missing_operator_ids) > 0:
        msg = 'Missing monthly_network_triplets_per_mno partitions for operators: {0}' \
              .format(', '.join([x[0] for x in missing_operator_ids]))
        if disable_data_check:
            logger.warning(msg)
        else:
            logger.error(msg)
            raise exceptions.MissingOperatorDataException(msg)

    extra_operator_ids = observed_operator_ids - required_operator_ids
    if len(extra_operator_ids) > 0:
        msg = 'Extra monthly_network_triplets_per_mno partitions detected for unconfigured operators: {0}' \
              .format(', '.join([x[0] for x in extra_operator_ids]))
        if disable_data_check:
            logger.warning(msg)
        else:
            logger.error(msg)
            raise exceptions.ExtraOperatorDataException(msg)

    operator_monthly_partitions = set()
    for op_partition in operator_partitions:
        operator_monthly_partitions.update(utils.child_table_names(conn, op_partition))
    observed_invariants = {x for x in utils.table_invariants_list(conn,
                                                                  operator_monthly_partitions,
                                                                  ['operator_id', 'triplet_year', 'triplet_month'])}
    observed_invariants = {x for x in observed_invariants if x.triplet_year == year and x.triplet_month == month}
    required_invariants = {(o.id, year, month) for o in operators}
    missing_invariants = required_invariants - observed_invariants
    if len(missing_invariants) > 0:
        msg = 'Missing monthly_network_triplets_per_mno partitions for the requested reporting ' \
              'month for the following configured operators: {0}' \
              .format(', '.join([x[0] for x in missing_invariants]))
        if disable_data_check:
            logger.warning(msg)
        else:
            logger.error(msg)
            raise exceptions.MissingOperatorDataException(msg)

    extra_invariants = observed_invariants - required_invariants
    if len(extra_invariants) > 0:
        msg = 'Extra monthly_network_triplets_per_mno partitions detected for the requested ' \
              'reporting month for the following unconfigured operators: {0}' \
              .format(', '.join([x[0] for x in extra_invariants]))
        if disable_data_check:
            logger.warning(msg)
        else:
            logger.error(msg)
            raise exceptions.ExtraOperatorDataException(msg)

    country_imei_shard_name = partition_utils.monthly_network_triplets_country_partition(month=month, year=year)
    with conn.cursor() as cursor:
        cursor.execute(utils.table_exists_sql(), [country_imei_shard_name])
        partition_exists = cursor.fetchone()[0]
        if not partition_exists:
            msg = 'Missing monthly_network_triplets_country partition for year and month'
            if disable_data_check:
                logger.warning(msg)
            else:
                logger.error(msg)
                raise exceptions.ExtraOperatorDataException(msg)


def _write_country_gsma_not_found_report(conn, config, month, year, country_name, output_dir):
    """
    Helper function to write out the country-wide GSMA not found report.

    :param conn: database connection
    :param config: dirbs config obj
    :param month: data month
    :param year: data year
    :param country_name: name of the country
    :param output_dir: output directory path
    :return: metadata obj
    """
    gsma_not_found_csv_filename = '{0}_{1:d}_{2:d}_gsma_not_found.csv'.format(country_name, month, year)
    with open(os.path.join(output_dir, gsma_not_found_csv_filename), 'w', encoding='utf8') as of:
        writer = csv.writer(of)
        writer.writerow(['IMEI'])
        dim = GSMANotFound()
        sql = dim.sql(conn, config, 1, 100)
        with conn.cursor(name='gsma_not_found_report') as cursor:
            cursor.execute(sql)
            for res in cursor:
                writer.writerow([res.imei_norm])

    return _gen_metadata_for_reports([gsma_not_found_csv_filename], output_dir)


def _write_country_duplicates_report(conn, config, month, year, country_name, output_dir, imsi_min_limit=5):
    """
    Helper function to write out the country-wide duplicates report.

    :param conn: database connection
    :param config: dirbs config obj
    :param month: data month
    :param year: data year
    :param country_name: country name
    :param output_dir: output directory path
    :param imsi_min_limit: imsi min limit
    :return: metadata obj
    """
    duplicates_csv_filename = '{0}_{1:d}_{2:d}_duplicates.csv'.format(country_name, month, year)
    with open(os.path.join(output_dir, duplicates_csv_filename), 'w', encoding='utf8') as of:
        writer = csv.writer(of)
        writer.writerow(['IMEI', 'IMSI count'])
        # We can't use our normal duplicate dimension here as it doesn't give the limits, so unfortunately,
        # we have to use a modified query that is also slightly optimized as it can query using triplet_year
        # and triplet_month
        with conn.cursor(name='duplicates_report') as cursor:
            cursor.execute("""SELECT imei_norm,
                                     COUNT(*) AS imsi_count
                                FROM (SELECT DISTINCT imei_norm, imsi
                                        FROM monthly_network_triplets_country_no_null_imeis
                                       WHERE triplet_month = %s
                                         AND triplet_year = %s
                                         AND is_valid_imsi(imsi)) all_network_imei_imsis
                            GROUP BY imei_norm HAVING COUNT(*) >= %s
                            ORDER BY imsi_count DESC""",
                           [month, year, imsi_min_limit])
            for res in cursor:
                writer.writerow([res.imei_norm, res.imsi_count])

    return _gen_metadata_for_reports([duplicates_csv_filename], output_dir)


def _write_condition_imei_overlaps(conn, config, month, year, country_name, output_dir, cond_names):
    """
    Helper function to write out IMEIs that are seen on multiple operators that have been classified.

    :param conn: database connection
    :param config: dirbs config obj
    :param month: data month
    :param year: data year
    :param country_name: country name
    :param output_dir: output directory path
    :param cond_names: list of condition names
    :return: metadata obj
    """
    with contextlib.ExitStack() as stack:
        # Push files into exit stack so that they will all be closed.
        filename_cond_map = {'{0}_{1:d}_{2:d}_condition_imei_overlap_{3}.csv'.format(country_name, month, year, c): c
                             for c in cond_names}
        condname_file_map = {c: stack.enter_context(open(os.path.join(output_dir, fn), 'w', encoding='utf8'))
                             for fn, c in filename_cond_map.items()}
        # Create a map from condition name to csv writer
        condname_csvwriter_map = {c: csv.writer(condname_file_map[c]) for c in cond_names}
        # Write the header to each csvwriter
        for _, writer in condname_csvwriter_map.items():
            writer.writerow(['IMEI', 'Operators'])
        # Runa query to find all the classified IMEIs seen on multiple operators
        with conn.cursor(name='imeis_overlap') as cursor:
            cursor.execute("""SELECT imei_norm, cond_name, string_agg(DISTINCT operator_id, '|') AS operators
                                FROM classification_state
                                JOIN monthly_network_triplets_per_mno_no_null_imeis
                               USING (imei_norm)
                               WHERE triplet_month = %s
                                 AND triplet_year = %s
                                 AND end_date IS NULL
                            GROUP BY imei_norm, cond_name
                                     HAVING COUNT(DISTINCT operator_id) > 1""",
                           [month, year])
            for res in cursor:
                condname_csvwriter_map[res.cond_name].writerow([res.imei_norm, res.operators])

    return _gen_metadata_for_reports(list(filename_cond_map.keys()), output_dir)


def _make_report_directory(ctx, base_dir, run_id, conn, config, class_run_id=None, **extra_options):
    """
    Make directory based on timestamp, data_id and class_run_id.

    :param ctx: current cli context
    :param base_dir: base directory path
    :param run_id: job run id
    :param conn: database connection
    :param config: dirbs config obj
    :param class_run_id: class run id
    :param extra_options: extra command line options
    :return: path of report directory
    """
    assert run_id
    fn_components = ['report']

    # subcommand
    subcommand = ctx.command.name
    fn_components.append(subcommand)

    # timestamp
    run_id_start_time = metadata.job_start_time_by_run_id(conn, run_id)
    assert run_id_start_time
    fn_components.append(run_id_start_time.strftime('%Y%m%d_%H%M%S'))

    # run_id
    fn_components.append('run_id_{0:d}'.format(run_id))

    # class_run_id - to be computed if not provided and could be None in case of no classification jobs.
    if not class_run_id:
        cond_run_info = utils.most_recently_run_condition_info(conn, [c.label for c in config.conditions],
                                                               successful_only=True)
        if not cond_run_info:
            class_run_id = None
        else:
            class_run_id = max([v['run_id'] for k, v in cond_run_info.items()])

    if class_run_id:
        fn_components.append('class_id_{0:d}'.format(class_run_id))

    # data_id, month, year
    for k, v in sorted(extra_options.items()):
        assert v
        fn_components.append('{0}_{1}'.format(k, v))

    dir_name = '__'.join(fn_components)
    report_dir = os.path.join(base_dir, dir_name)
    os.makedirs(report_dir)
    return report_dir


# validation checks
def _reports_validation_checks(disable_retention_check, year, month, logger, config, conn, disable_data_check):
    """
    Helper method to perform validation checks on reports.

    :param disable_retention_check: retention check flag
    :param year: data year
    :param month: data month
    :param logger: dirbs logger obj
    :param config: dirbs config obj
    :param conn: database connection
    :param disable_data_check: data check flag
    """
    _retention_window_check(disable_retention_check, year, month, config, logger)
    _operators_configured_check(config, logger)
    _extra_missing_operator_check(config, conn, month, year, logger, disable_data_check)


def _retention_window_check(disable_retention_check, year, month, config, logger):
    """
    Helper method to perform retention check.

    :param disable_retention_check: retention check flag
    :param year: data year
    :param month: data month
    :param config: dirbs config obj
    :param logger: dirbs logger obj
    """
    # DIRBS-371: Make sure that we fail if part of the month is outside the retention
    # window
    if not disable_retention_check:
        report_start_date = datetime.date(year, month, 1)
        curr_date = datetime.date.today()
        retention_months = config.retention_config.months_retention
        retention_window_start = datetime.date(curr_date.year, curr_date.month, 1) - \
            relativedelta.relativedelta(months=retention_months)
        if report_start_date < retention_window_start:
            logger.error('Attempting to generate a report for a period outside the retention window...')
            sys.exit(1)


def _operators_configured_check(config, logger):
    """
    Helper method to perform configured operators check.

    :param config: dirbs config obj
    :param logger: dirbs logger obj
    """
    # Fail if there are no configured operators
    operators = config.region_config.operators
    if len(operators) == 0:
        logger.error('No operators configured in region config. No report can be generated...')
        sys.exit(1)


def _extra_missing_operator_check(config, conn, month, year, logger, disable_data_check):
    """
    Process extra missing operator check.

    :param config: dirbs config obj
    :param conn: database connection
    :param month: data month
    :param year: data year
    :param logger: dirbs logger obj
    :param disable_data_check: data check flag
    """
    # Validate that data is present for all configured operators and only configured operators
    try:
        _validate_data_partitions(config, conn, month, year, logger, disable_data_check)
    except (exceptions.ExtraOperatorDataException, exceptions.MissingOperatorDataException):
        logger.error('Extra or missing operator data detected above will skew report counts, so report '
                     'will not be generated. To ignore this warning, use the --disable-data-check option')
        sys.exit(1)


@click.group(no_args_is_help=False)
@common.setup_initial_logging
@click.version_option()
@common.parse_verbosity_option
@common.parse_db_options
@common.parse_statsd_options
@click.pass_context
@common.configure_logging
def cli(ctx):
    """DIRBS script to output reports (operator and country) for a given MONTH and YEAR."""
    pass


@cli.command()  # noqa: C901
@common.parse_multiprocessing_options
@_parse_month_year_report_options_args
@click.pass_context
@common.unhandled_exception_handler
@common.cli_wrapper(command='dirbs-report', subcommand='standard', required_role='dirbs_core_report')
def standard(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
             force_refresh, disable_retention_check, disable_data_check, debug_query_performance,
             month, year, output_dir):
    """Generate standard monthly operator and country-level reports."""
    # Store metadata
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       refreshed_data=force_refresh,
                                       month=month,
                                       year=year,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))

    _reports_validation_checks(disable_retention_check, year, month, logger, config, conn,
                               disable_data_check)

    # Next, generate all the report data so that report generation can happen very quickly
    data_id, class_run_id, per_tac_compliance_data = generate_monthly_report_stats(config, conn, month, year,
                                                                                   statsd, metrics_run_root,
                                                                                   run_id,
                                                                                   force_refresh,
                                                                                   debug_query_performance)

    # Store metadata about the report data ID and classification run ID
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, data_id=data_id,
                                       classification_run_id=class_run_id)

    report_dir = _make_report_directory(ctx, output_dir, run_id, conn, config, class_run_id=class_run_id,
                                        year=year, month=month, data_id=data_id)

    # First, copy all the report JS/CSS files into the output directory in
    # cachebusted form and get the cachebusted filenames
    asset_map = {}
    report_assets = [
        'js/report.js',
        'css/report.css'
    ]

    for fn in report_assets:
        logger.info('Copying required asset "%s" to report folder', fn)
        asset = pkgutil.get_data('dirbs', fn)
        name, ext = fn.split('/')[-1].split('.')
        filename = '{0}_{1}.{2}'.format(name, utils.cachebusted_filename_from_contents(asset), ext)
        asset_map[fn] = filename
        with open(os.path.join(report_dir, filename), 'wb') as of:
            of.write(asset)

    js_filename = asset_map['js/report.js']
    css_filename = asset_map['css/report.css']

    # Next, generate the country level report
    report_metadata = []
    with utils.CodeProfiler() as cp:
        logger.info('Generating country report...')
        country_name = config.region_config.name
        country_per_tac_compliance_data = None
        if per_tac_compliance_data is not None:
            country_per_tac_compliance_data = per_tac_compliance_data[OperatorConfig.COUNTRY_OPERATOR_NAME]
        report = CountryReport(conn, data_id, config, month, year, country_name,
                               has_compliance_data=country_per_tac_compliance_data is not None)
        report_metadata.extend(_write_report(report, month, year, report_dir, country_name,
                                             css_filename, js_filename, country_per_tac_compliance_data))

    statsd.gauge('{0}runtime.per_report.country'.format(metrics_run_root), cp.duration)
    operators = config.region_config.operators
    # Finally, generate the operator reports
    for op in operators:
        with utils.CodeProfiler() as cp:
            logger.info('Generating operator report for operator ID %s...', op.id)
            operator_per_tac_compliance_data = None
            if per_tac_compliance_data is not None:
                operator_per_tac_compliance_data = per_tac_compliance_data.get(op.id)
            report = OperatorReport(conn, data_id, config, month, year, op,
                                    has_compliance_data=operator_per_tac_compliance_data is not None)
            report_prefix = '{0}_{1}'.format(country_name, op.id)
            report_metadata.extend(_write_report(report, month, year, report_dir, report_prefix,
                                                 css_filename, js_filename, operator_per_tac_compliance_data))
        statsd.gauge('{0}runtime.per_report.operators.{1}'.format(metrics_run_root, op.id),
                     cp.duration)

    # Store per-report job metadata
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_outputs=report_metadata)


@cli.command(name='gsma_not_found')  # noqa: C901
@common.parse_multiprocessing_options
@_parse_month_year_report_options_args
@click.pass_context
@common.unhandled_exception_handler
@common.cli_wrapper(command='dirbs-report', subcommand='gsma_not_found', required_role='dirbs_core_report')
def gsma_not_found(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
                   force_refresh, disable_retention_check, disable_data_check, debug_query_performance,
                   month, year, output_dir):
    """Generate report of all GSMA not found IMEIs."""
    _reports_validation_checks(disable_retention_check, year, month, logger, config, conn,
                               disable_data_check)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       refreshed_data=force_refresh,
                                       month=month,
                                       year=year,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))
    report_dir = _make_report_directory(ctx, output_dir, run_id, conn, config, year=year, month=month)

    report_metadata = []

    with utils.CodeProfiler() as cp:
        logger.info('Generating country GSMA not found report...')
        country_name = config.region_config.name
        report_metadata.extend(_write_country_gsma_not_found_report(conn, config, month,
                                                                    year, country_name, report_dir))
    statsd.gauge('{0}runtime.per_report.gsma_not_found'.format(metrics_run_root), cp.duration)

    # Store metadata about the report data ID and classification run ID
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_outputs=report_metadata)


@cli.command(name='top_duplicates')  # noqa: C901
@common.parse_multiprocessing_options
@_parse_month_year_report_options_args
@click.pass_context
@common.unhandled_exception_handler
@common.cli_wrapper(command='dirbs-report', subcommand='top_duplicates', required_role='dirbs_core_report')
def top_duplicates(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
                   force_refresh, disable_retention_check, disable_data_check, debug_query_performance,
                   month, year, output_dir):
    """Generate report listing IMEIs seen with more than 5 IMSIs in a given month and year."""
    _reports_validation_checks(disable_retention_check, year, month, logger, config, conn,
                               disable_data_check)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       refreshed_data=force_refresh,
                                       month=month,
                                       year=year,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))
    report_metadata = []
    report_dir = _make_report_directory(ctx, output_dir, run_id, conn, config, year=year, month=month)
    with utils.CodeProfiler() as cp:
        imsi_min_limit = 5
        country_name = config.region_config.name
        logger.info('Generating country duplicate IMEI report (IMEIs seen with more than {0:d} IMSIs this '
                    'reporting month)...'.format(imsi_min_limit))
        report_metadata.extend(_write_country_duplicates_report(conn, config, month, year, country_name,
                                                                report_dir, imsi_min_limit=imsi_min_limit))
    statsd.gauge('{0}runtime.per_report.top_duplicates'.format(metrics_run_root), cp.duration)

    # Store metadata about the report data ID and classification run ID
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_outputs=report_metadata)


@cli.command(name='condition_imei_overlaps')  # noqa: C901
@common.parse_multiprocessing_options
@_parse_month_year_report_options_args
@click.pass_context
@common.unhandled_exception_handler
@common.cli_wrapper(command='dirbs-report', subcommand='condition_imei_overlaps', required_role='dirbs_core_report')
def condition_imei_overlaps(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root,
                            metrics_run_root, force_refresh, disable_retention_check, disable_data_check,
                            debug_query_performance, month, year, output_dir):
    """Generate per-condition reports showing matched IMEIs seen on more than one MNO network."""
    _reports_validation_checks(disable_retention_check, year, month, logger, config, conn,
                               disable_data_check)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       refreshed_data=force_refresh,
                                       month=month,
                                       year=year,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))
    report_dir = _make_report_directory(ctx, output_dir, run_id, conn, config, year=year, month=month)
    report_metadata = []

    with utils.CodeProfiler() as cp:
        country_name = config.region_config.name
        logger.info('Generating country per-condition IMEI overlap reports (classified IMEIs seen on more than '
                    'one MNO\'s network this month...')
        cond_names = [c.label for c in config.conditions]
        report_metadata.extend(_write_condition_imei_overlaps(conn, config, month, year, country_name,
                                                              report_dir, cond_names))
    statsd.gauge('{0}runtime.per_report.condition_imei_overlaps'.format(metrics_run_root), cp.duration)

    # Store metadata about the report data ID and classification run ID
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_outputs=report_metadata)


@cli.command(name='stolen_violations')  # noqa: C901
@common.parse_multiprocessing_options
@click.pass_context
@common.unhandled_exception_handler
@_parse_output_dir
@common.cli_wrapper(command='dirbs-report', subcommand='stolen_violations', required_role='dirbs_core_report')
@click.option('--newer-than',
              default=None,
              callback=common.validate_date,
              help='Include violations only when observed date on network is newer than this date (YYYYMMDD).')
@click.option('--filter-by-conditions',
              help='Specify a comma-separated list of condition names if you wish to filter by those conditions.',
              callback=common.validate_conditions,
              default=None)
def stolen_violations(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root,
                      metrics_run_root, output_dir, newer_than, filter_by_conditions):
    """Generate per-MNO list of IMEIs seen on the network after they were reported stolen."""
    _operators_configured_check(config, logger)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))

    report_dir = _make_report_directory(ctx, output_dir, run_id, conn, config)

    with utils.CodeProfiler() as cp:
        logger.info('Generating per-MNO stolen list violations reports...')
        with contextlib.ExitStack() as stack:
            # Push files into exit stack so that they will all be closed.
            operator_ids = [o.id for o in config.region_config.operators]
            filename_op_map = {'stolen_violations_{0}.csv'.format(o): o for o in operator_ids}
            opname_file_map = {o: stack.enter_context(open(os.path.join(report_dir, fn), 'w', encoding='utf8'))
                               for fn, o in filename_op_map.items()}
            # Create a map from operator name to csv writer
            opname_csvwriter_map = {o: csv.writer(opname_file_map[o]) for o in operator_ids}
            # Write the header to each csvwriter
            for _, writer in opname_csvwriter_map.items():
                writer.writerow(['imei_norm', 'last_seen', 'reporting_date'])

            # Run a query to find all the classified IMEIs seen on multiple operators
            blacklist_violations_grace_period_days = config.report_config.blacklist_violations_grace_period_days
            with conn.cursor() as cursor:
                query = sql.SQL("""SELECT imei_norm, last_seen, reporting_date, operator_id
                                     FROM (SELECT imei_norm, MIN(reporting_date) AS reporting_date
                                             FROM stolen_list
                                         GROUP BY imei_norm) AS stolen_imeis
                                     JOIN LATERAL (
                                           SELECT imei_norm, operator_id, MAX(last_seen) AS last_seen
                                             FROM monthly_network_triplets_per_mno_no_null_imeis nt
                                            WHERE imei_norm = stolen_imeis.imei_norm
                                              AND virt_imei_shard = calc_virt_imei_shard(stolen_imeis.imei_norm)
                                         GROUP BY imei_norm, operator_id) network_imeis
                                    USING (imei_norm)
                                    WHERE network_imeis.last_seen > stolen_imeis.reporting_date + %s
                                          {0}
                                          {1}""")

                if filter_by_conditions:
                    cond_filter_query = """AND EXISTS(SELECT 1
                                                        FROM classification_state
                                                       WHERE imei_norm = stolen_imeis.imei_norm
                                                         AND virt_imei_shard =
                                                                calc_virt_imei_shard(stolen_imeis.imei_norm)
                                                         AND cond_name IN %s
                                                         AND end_date IS NULL)"""
                    sql_bytes = cursor.mogrify(cond_filter_query, [tuple([c.label for c in filter_by_conditions])])
                    conditions_filter_sql = sql.SQL(str(sql_bytes, conn.encoding))
                else:
                    conditions_filter_sql = sql.SQL('')

                if newer_than:
                    newer_than_query = 'AND last_seen > %s'
                    sql_bytes = cursor.mogrify(newer_than_query, [newer_than])
                    date_filter_sql = sql.SQL(str(sql_bytes, conn.encoding))
                else:
                    date_filter_sql = sql.SQL('')

                cursor.execute(query.format(conditions_filter_sql, date_filter_sql),
                               [blacklist_violations_grace_period_days])
                for res in cursor:
                    opname_csvwriter_map[res.operator_id].writerow([res.imei_norm, res.last_seen.strftime('%Y%m%d'),
                                                                    res.reporting_date.strftime('%Y%m%d')])

        report_metadata = _gen_metadata_for_reports(list(filename_op_map.keys()), report_dir)

    statsd.gauge('{0}runtime.per_report.blacklist_violations_stolen'.format(metrics_run_root), cp.duration)

    # Store metadata about the report data ID and classification run ID
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_outputs=report_metadata)
