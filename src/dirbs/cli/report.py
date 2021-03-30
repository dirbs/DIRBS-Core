"""
DIRBS CLI for report generation (Operator, Country). Installed by setuptools as a dirbs-report console script.

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

import datetime
import pkgutil
import os

import click

import dirbs.utils as utils
import dirbs.cli.common as common
import dirbs.metadata as metadata
from dirbs import report_schema_version
from dirbs.config.region import OperatorConfig
from dirbs.reports import CountryReport, OperatorReport, generate_monthly_report_stats
from dirbs.reports.csv_reports import reports_validation_checks, make_report_directory, write_report, \
    write_country_gsma_not_found_report, write_country_duplicates_report, write_condition_imei_overlaps, \
    operators_configured_check, write_stolen_violations, write_non_active_pairs, write_un_registered_subscribers, \
    write_classified_triplets, write_blacklist_violations, write_association_list_violations, write_transient_msisdns


def _parse_month_year_report_options_args(f: callable) -> callable:
    """
    Decorator used to parse all the monthly, year command line options and update the config.

    Arguments:
        f: callable function to be decorated
    Returns:
        f: decorated callable function
    """
    f = _parse_force_refresh(f)
    f = _parse_disable_retention_check(f)
    f = _parse_disable_data_check(f)
    f = _parse_debug_query_performance(f)
    f = _parse_output_dir(f)
    f = _parse_year(f)
    f = _parse_month(f)
    return f


def _parse_month(f: callable) -> callable:
    """
    Function to parse month option on the command line.

    Arguments:
        f: callable function
    Returns:
        click argument
    """
    return click.argument('month',
                          type=int,
                          callback=_validate_month)(f)


def _parse_year(f: callable) -> callable:
    """
    Function to parse year option on the command line.

    Arguments:
        f: callable function
    Returns:
        click argument
    """
    return click.argument('year',
                          type=int,
                          callback=_validate_year)(f)


def _parse_output_dir(f: callable) -> callable:
    """
    Function to parse output dir option on the command line.

    Arguments:
        f: callable function
    Returns:
        click argument
    """
    return click.argument('output_dir',
                          type=click.Path(exists=True, file_okay=False, writable=True))(f)


def _parse_force_refresh(f: callable) -> callable:
    """
    Function to parse force refresh option on the command line.

    Arguments:
        f: callable function
    Returns:
        click argument
    """
    return click.option('--force-refresh/--no-refresh',
                        default=True,
                        is_flag=True,
                        help='Whether data in report should be refreshed from latest data or '
                             'from previously-calculated data (default: --no-refresh).')(f)


def _parse_disable_retention_check(f: callable) -> callable:
    """
    Function to parse disable retention check option on the command line.

    Arguments:
        f: callable function
    Returns:
        click argument
    """
    return click.option('--disable-retention-check',
                        default=False,
                        is_flag=True,
                        help='Disable check that stops reports being run for months outside the retention period.')(f)


def _parse_disable_data_check(f: callable) -> callable:
    """
    Function to parse disable data check option on the command line.

    Arguments:
        f: callable function
    Returns:
        click argument
    """
    return click.option('--disable-data-check',
                        default=False,
                        is_flag=True,
                        help='Disable check to validate existence of data for all configured operators in this '
                             'reporting month.')(f)


def _parse_debug_query_performance(f: callable) -> callable:
    """
    Function to parse debug query performance option on the command line.

    Arguments:
        f: callable function
    Returns:
        click argument
    """
    return click.option('--debug-query-performance',
                        default=False,
                        is_flag=True,
                        help='Enable this to print out more stats about duration of queries during stats '
                             'generation.')(f)


def _validate_month(ctx, param, val: int) -> int:
    """
    Helper function to validate a month coming from the CLI.

    Arguments:
        ctx: click cmd context
        param: required default parameter
        val: value to validate (month between 1-12)
    Returns:
        val: validated month value
    Raises:
        click.BadParameter: When month value does not lie in between 1-12
    """
    if val < 1 or val > 12:
        raise click.BadParameter('Month must be between 1 and 12')
    return val


def _validate_year(ctx, param, val: int) -> int:
    """
    Helper function to validate a year coming from the CLI.

    Arguments:
        ctx: click cmd context
        param: required default parameter
        val: value to validate (year between 2000-2100)
    Returns:
        val: validated month value
    Raises:
        click.BadParameter: When year value does not lie in between 2000-2100
    """
    if val < 2000 or val > 2100:
        raise click.BadParameter('Year must be between 2000 and 2100')
    return val


def _parse_positive_int(ctx: callable, param, value: str) -> int:
    """Helper function to parse a positive integer and return.

    Arguments:
        ctx: click context
        param: required param
        value: value to parse
    Returns:
        parsed_value: parsed integer value
    Raises:
        BadParameter: if value is less than or equal to 0 or not given or negative
    """
    try:
        if value is not None:
            parsed_value = int(value)
            if parsed_value <= 0:
                raise click.BadParameter('--period value must be greater than 0')
            return parsed_value
        raise click.BadParameter('--period is required')
    except ValueError:
        raise click.BadParameter('--period value must be positive integer')


@click.group(no_args_is_help=False)
@common.setup_initial_logging
@click.version_option()
@common.parse_verbosity_option
@common.parse_db_options
@common.parse_statsd_options
@click.pass_context
@common.configure_logging
def cli(ctx: callable) -> None:
    """DIRBS script to output reports (operator and country) for a given MONTH and YEAR.

    Arguments:
        ctx: click context (required)
    Returns:
        None
    """
    pass


@cli.command()  # noqa: C901
@common.parse_multiprocessing_options
@_parse_month_year_report_options_args
@click.pass_context
@common.unhandled_exception_handler
@common.cli_wrapper(command='dirbs-report', subcommand='standard', required_role='dirbs_core_report')
def standard(ctx: callable, config: callable, statsd: callable, logger: callable, run_id: int, conn: callable,
             metadata_conn: callable, command: str, metrics_root: callable, metrics_run_root: callable,
             force_refresh: bool, disable_retention_check: bool, disable_data_check: bool,
             debug_query_performance: bool, month: int, year: int, output_dir: str) -> None:
    """Generate standard monthly operator and country-level reports.

    Arguments:
        ctx: click context object
        config: DIRBS config object
        statsd: DIRBS statsd connection object
        logger: DIRBS custom logger object
        run_id: run id of the current job
        conn: DIRBS PostgreSQL connection object
        metadata_conn: DIRBS PostgreSQL metadata connection object
        command: name of the command
        metrics_root: root object for the statsd metrics
        metrics_run_root: root object for the statsd run metrics
        force_refresh: bool to force writing/generating reports from scratch
        disable_retention_check: bool to disable data retention check
        disable_data_check: bool to disable data check
        debug_query_performance: bool to debug query performance
        month: reporting month
        year: reporting year
        output_dir: output directory path
    Returns:
        None
    """
    # Store metadata
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       refreshed_data=force_refresh,
                                       month=month,
                                       year=year,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))

    reports_validation_checks(disable_retention_check, year, month, logger, config, conn,
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

    report_dir = make_report_directory(ctx, output_dir, run_id, conn, config, class_run_id=class_run_id,
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
        report_metadata.extend(write_report(report, month, year, report_dir, country_name,
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
            report_metadata.extend(write_report(report, month, year, report_dir, report_prefix,
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
def gsma_not_found(ctx: callable, config: callable, statsd: callable, logger: callable, run_id: int, conn: callable,
                   metadata_conn: callable, command: str, metrics_root: callable, metrics_run_root: callable,
                   force_refresh: bool, disable_retention_check: bool, disable_data_check: bool,
                   debug_query_performance: bool, month: int, year: int, output_dir: str) -> None:
    """Generate report of all GSMA not found IMEIs.

    Arguments:
        ctx: click context object
        config: DIRBS config object
        statsd: DIRBS statsd connection object
        logger: DIRBS custom logger object
        run_id: run id of the current job
        conn: DIRBS PostgreSQL connection object
        metadata_conn: DIRBS PostgreSQL metadata connection object
        command: name of the command
        metrics_root: root object for the statsd metrics
        metrics_run_root: root object for the statsd run metrics
        force_refresh: bool to force writing/generating reports from scratch
        disable_retention_check: bool to disable data retention check
        disable_data_check: bool to disable data check
        debug_query_performance: bool to debug query performance
        month: reporting month
        year: reporting year
        output_dir: output directory path
    Returns:
        None
    """
    reports_validation_checks(disable_retention_check, year, month, logger, config, conn,
                              disable_data_check)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       refreshed_data=force_refresh,
                                       month=month,
                                       year=year,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))
    report_dir = make_report_directory(ctx, output_dir, run_id, conn, config, year=year, month=month)

    report_metadata = []

    with utils.CodeProfiler() as cp:
        logger.info('Generating country GSMA not found report...')
        country_name = config.region_config.name
        report_metadata.extend(write_country_gsma_not_found_report(conn, config, month,
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
def top_duplicates(ctx: callable, config: callable, statsd: callable, logger: callable, run_id: int, conn: callable,
                   metadata_conn: callable, command: str, metrics_root: callable, metrics_run_root: callable,
                   force_refresh: bool, disable_retention_check: bool, disable_data_check: bool,
                   debug_query_performance: bool, month: int, year: int, output_dir: str) -> None:
    """Generate report listing IMEIs seen with more than 5 IMSIs in a given month and year.

    Arguments:
        ctx: click context object
        config: DIRBS config object
        statsd: DIRBS statsd connection object
        logger: DIRBS custom logger object
        run_id: run id of the current job
        conn: DIRBS PostgreSQL connection object
        metadata_conn: DIRBS PostgreSQL metadata connection object
        command: name of the command
        metrics_root: root object for the statsd metrics
        metrics_run_root: root object for the statsd run metrics
        force_refresh: bool to force writing/generating reports from scratch
        disable_retention_check: bool to disable data retention check
        disable_data_check: bool to disable data check
        debug_query_performance: bool to debug query performance
        month: reporting month
        year: reporting year
        output_dir: output directory path
    Returns:
        None
    """
    reports_validation_checks(disable_retention_check, year, month, logger, config, conn,
                              disable_data_check)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       refreshed_data=force_refresh,
                                       month=month,
                                       year=year,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))
    report_metadata = []
    report_dir = make_report_directory(ctx, output_dir, run_id, conn, config, year=year, month=month)
    with utils.CodeProfiler() as cp:
        imsi_min_limit = 5
        country_name = config.region_config.name
        logger.info('Generating country duplicate IMEI report (IMEIs seen with more than {0:d} IMSIs this '
                    'reporting month)...'.format(imsi_min_limit))
        report_metadata.extend(write_country_duplicates_report(conn, config, month, year, country_name,
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
def condition_imei_overlaps(ctx: callable, config: callable, statsd: callable, logger: callable, run_id: int,
                            conn: callable, metadata_conn: callable, command: str, metrics_root: callable,
                            metrics_run_root: callable, force_refresh: bool, disable_retention_check: bool,
                            disable_data_check: bool, debug_query_performance: bool, month: int, year: int,
                            output_dir: str):
    """Generate per-condition reports showing matched IMEIs seen on more than one MNO network.

    Arguments:
        ctx: click context object
        config: DIRBS config object
        statsd: DIRBS statsd connection object
        logger: DIRBS custom logger object
        run_id: run id of the current job
        conn: DIRBS PostgreSQL connection object
        metadata_conn: DIRBS PostgreSQL metadata connection object
        command: name of the command
        metrics_root: root object for the statsd metrics
        metrics_run_root: root object for the statsd run metrics
        force_refresh: bool to force writing/generating reports from scratch
        disable_retention_check: bool to disable data retention check
        disable_data_check: bool to disable data check
        debug_query_performance: bool to debug query performance
        month: reporting month
        year: reporting year
        output_dir: output directory path
    Returns:
        None
    """
    reports_validation_checks(disable_retention_check, year, month, logger, config, conn, disable_data_check)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       refreshed_data=force_refresh,
                                       month=month,
                                       year=year,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))
    report_dir = make_report_directory(ctx, output_dir, run_id, conn, config, year=year, month=month)
    report_metadata = []

    with utils.CodeProfiler() as cp:
        country_name = config.region_config.name
        logger.info('Generating country per-condition IMEI overlap reports (classified IMEIs seen on more than '
                    "one MNO\'s network this month...")
        cond_names = [c.label for c in config.conditions]
        report_metadata.extend(write_condition_imei_overlaps(conn, config, month, year, country_name,
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
def stolen_violations(ctx: callable, config: callable, statsd: callable, logger: callable, run_id: int, conn: callable,
                      metadata_conn: callable, command: str, metrics_root: callable, metrics_run_root: callable,
                      output_dir: str, newer_than: str, filter_by_conditions: list) -> None:
    """Generate per-MNO list of IMEIs seen on the network after they were reported stolen.

    Arguments:
        ctx: click context object
        config: DIRBS config object
        statsd: DIRBS statsd connection object
        logger: DIRBS custom logger object
        run_id: run id of the current job
        conn: DIRBS PostgreSQL connection object
        metadata_conn: DIRBS PostgreSQL metadata connection object
        command: name of the command
        metrics_root: root object for the statsd metrics
        metrics_run_root: root object for the statsd run metrics
        output_dir: output directory path
        newer_than: violation newer then this date
        filter_by_conditions: list of condition to filter by
    Returns:
        None
    """
    operators_configured_check(config, logger)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))

    report_dir = make_report_directory(ctx, output_dir, run_id, conn, config)

    with utils.CodeProfiler() as cp:
        report_metadata = write_stolen_violations(config, logger, report_dir, conn, filter_by_conditions, newer_than)

    statsd.gauge('{0}runtime.per_report.blacklist_violations_stolen'.format(metrics_run_root), cp.duration)

    # Store metadata about the report data ID and classification run ID
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_outputs=report_metadata)


@cli.command(name='non_active_pairs')  # noqa: C901
@click.pass_context
@common.unhandled_exception_handler
@click.argument('period', callback=_parse_positive_int)
@_parse_output_dir
@common.cli_wrapper(command='dirbs-report', subcommand='non_active_pairs', required_role='dirbs_core_report')
def non_active_pairs(ctx: callable, config: callable, statsd: callable, logger: callable, run_id: int,
                     conn: callable, metadata_conn: callable, command: str, metrics_root: callable,
                     metrics_run_root: callable, output_dir: str, period: int) -> None:
    """Generate list of Non-Active pairs over specified period.

    Arguments:
        ctx: click context object
        config: DIRBS config object
        statsd: DIRBS statsd connection object
        logger: DIRBS custom logger object
        run_id: run id of the current job
        conn: DIRBS PostgreSQL connection object
        metadata_conn: DIRBS PostgreSQL metadata connection object
        command: name of the command
        metrics_root: root object for the statsd metrics
        metrics_run_root: root object for the statsd run metrics
        output_dir: output directory path
        period: period in days for a pair being count as not active (not active for these many days)
    Returns:
        None
    """
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))

    current_date = datetime.date.today()
    last_seen_date = datetime.date(current_date.year,
                                   current_date.month,
                                   current_date.day) - datetime.timedelta(period)
    logger.info('List of None-Active Pairs with last_seen less than {0} will be generated'.format(last_seen_date))
    report_dir = make_report_directory(ctx, output_dir, run_id, conn, config)

    with utils.CodeProfiler() as cp:
        report_metadata = write_non_active_pairs(conn, logger, report_dir, last_seen_date)

    statsd.gauge('{0}runtime.per_report.non_active_pairs'.format(metrics_run_root), cp.duration)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_outputs=report_metadata)


@cli.command(name='unregistered_subscribers')
@common.parse_multiprocessing_options
@click.pass_context
@common.unhandled_exception_handler
@_parse_output_dir
@common.cli_wrapper(command='dirbs-report', subcommand='unregistered_subscribers', required_role='dirbs_core_report')
@click.option('--newer-than',
              default=None,
              callback=common.validate_date,
              help='Include imsis only when observed date on network is newer than this date (YYYYMMDD).')
def unregistered_subscribers(ctx: callable, config: callable, statsd: callable, logger: callable, run_id: int,
                             conn: callable, metadata_conn: callable, command: str, metrics_root: callable,
                             metrics_run_root: callable, output_dir: str, newer_than: str):
    """Generate per-MNO list of IMSIs that are not registered in subscribers list.

    Arguments:
        ctx: click context object
        config: DIRBS config object
        statsd: DIRBS statsd connection object
        logger: DIRBS custom logger object
        run_id: run id of the current job
        conn: DIRBS PostgreSQL connection object
        metadata_conn: DIRBS PostgreSQL metadata connection object
        command: name of the command
        metrics_root: root object for the statsd metrics
        metrics_run_root: root object for the statsd run metrics
        output_dir: output directory path
        newer_than: violation newer then this date
    Returns:
        None
    """
    operators_configured_check(config, logger)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))
    report_dir = make_report_directory(ctx, output_dir, run_id, conn, config)

    with utils.CodeProfiler() as cp:
        report_metadata = write_un_registered_subscribers(logger, config, report_dir, conn, newer_than)

    statsd.gauge('{0}runtime.per_report.unregistered_subscribers'.format(metrics_run_root), cp.duration)

    # store metadata
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_outputs=report_metadata)


@cli.command(name='classified_triplets')
@click.pass_context
@common.unhandled_exception_handler
@click.argument('conditions', callback=common.validate_conditions)
@_parse_output_dir
@common.cli_wrapper(command='dirbs-report', subcommand='classified_triplets', required_role='dirbs_core_report')
def classified_triplets(ctx: callable, config: callable, statsd: callable, logger: callable, run_id: int,
                        conn: callable, metadata_conn: callable, command: str, metrics_root: callable,
                        metrics_run_root: callable, output_dir: str, conditions: list) -> None:
    """Generate per-condition classified triplets list.

    Arguments:
        ctx: click context object
        config: DIRBS config object
        statsd: DIRBS statsd connection object
        logger: DIRBS custom logger object
        run_id: run id of the current job
        conn: DIRBS PostgreSQL connection object
        metadata_conn: DIRBS PostgreSQL metadata connection object
        command: name of the command
        metrics_root: root object for the statsd metrics
        metrics_run_root: root object for the statsd run metrics
        output_dir: output directory path
        conditions: list of conditions for classified triplets
    Returns:
        None
    """
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))
    report_dir = make_report_directory(ctx, output_dir, run_id, conn, config)

    with utils.CodeProfiler() as cp:
        report_metadata = write_classified_triplets(logger, conditions, report_dir, conn)

    statsd.gauge('{0}runtime.per_report.classified_triplets'.format(metrics_run_root), cp.duration)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_outputs=report_metadata)


@cli.command(name='blacklist_violations')
@click.pass_context
@common.unhandled_exception_handler
@_parse_month
@_parse_year
@_parse_output_dir
@common.cli_wrapper(command='dirbs-report', subcommand='blacklist_violations', required_role='dirbs_core_report')
def blacklist_violations(ctx: callable, config: callable, statsd: callable, logger: callable, run_id: int,
                         conn: callable, metadata_conn: callable, command: str, metrics_root: callable,
                         metrics_run_root: callable, output_dir: str, month: int, year: int) -> None:
    """Generate per-operator blacklist violations.

    Arguments:
        ctx: click context object
        config: DIRBS config object
        statsd: DIRBS statsd connection object
        logger: DIRBS custom logger object
        run_id: run id of the current job
        conn: DIRBS PostgreSQL connection object
        metadata_conn: DIRBS PostgreSQL metadata connection object
        command: name of the command
        metrics_root: root object for the statsd metrics
        metrics_run_root: root object for the statsd run metrics
        output_dir: output directory path
        month: reporting month
        year: reporting year
    Returns:
        None
    """
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))
    report_dir = make_report_directory(ctx, output_dir, run_id, conn, config)

    with utils.CodeProfiler() as cp:
        report_metadata = write_blacklist_violations(logger, config, report_dir, conn, month, year)
    statsd.gauge('{0}runtime.per_report.blacklist_violation'.format(metrics_run_root), cp.duration)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_outputs=report_metadata)


@cli.command(name='association_list_violations')
@click.pass_context
@common.unhandled_exception_handler
@_parse_month
@_parse_year
@_parse_output_dir
@common.cli_wrapper(command='dirbs-report', subcommand='association_list_violations',
                    required_role='dirbs_core_report')
def association_list_violations(ctx: callable, config: callable, statsd: callable, logger: callable, run_id: int,
                                conn: callable, metadata_conn: callable, command: str, metrics_root: callable,
                                metrics_run_root: callable, output_dir: str, month: int, year: int):
    """Generate per-operator association list violations (UID-IMEI-IMSI).

    Arguments:
        ctx: click context object
        config: DIRBS config object
        statsd: DIRBS statsd connection object
        logger: DIRBS custom logger object
        run_id: run id of the current job
        conn: DIRBS PostgreSQL connection object
        metadata_conn: DIRBS PostgreSQL metadata connection object
        command: name of the command
        metrics_root: root object for the statsd metrics
        metrics_run_root: root object for the statsd run metrics
        output_dir: output directory path
        month: reporting month
        year: reporting year
    Returns:
        None
    """
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))
    report_dir = make_report_directory(ctx, output_dir, run_id, conn, config)

    with utils.CodeProfiler() as cp:
        report_metadata = write_association_list_violations(logger, config, report_dir, conn, month, year)
    statsd.gauge('{0}runtime.per_report.association_list_violations'.format(metrics_run_root), cp.duration)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_outputs=report_metadata)


@cli.command(name='transient_msisdns')
@click.pass_context
@common.unhandled_exception_handler
@click.argument('period', callback=_parse_positive_int)
@click.argument('num_of_imeis', callback=_parse_positive_int)
@_parse_output_dir
@common.cli_wrapper(command='dirbs-report', subcommand='transient_msisdns', required_role='dirbs_core_report')
@click.option('--current-date',
              default=None,
              callback=common.validate_date,
              help='Setting current date for the analysis in the form (YYYYMMDD).')
def transient_msisdns(ctx: callable, config: callable, statsd: callable, logger: callable, run_id: int,
                      conn: callable, metadata_conn: callable, command: str, metrics_root: callable,
                      metrics_run_root: callable, output_dir: str, period: int, num_of_imeis: int,
                      current_date: str) -> None:
    """Generate list of MSISDNS used with possible transient IMEIs.

    Required Arguments:
        period: Analysis period in days (positive integer)
        num_of_imeis: Number of IMEIs a MSISDN must be seen with for analysis
    """
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       report_schema_version=report_schema_version,
                                       output_dir=os.path.abspath(str(output_dir)))
    report_dir = make_report_directory(ctx, output_dir, run_id, conn, config)

    with utils.CodeProfiler() as cp:
        report_metadata = write_transient_msisdns(logger, period, report_dir, conn,
                                                  config, num_of_imeis, current_date=current_date)

    statsd.gauge('{0}runtime.per_report.transient_msisdns'.format(metrics_run_root), cp.duration)
    metadata.add_optional_job_metadata(metadata_conn, command, run_id, report_outputs=report_metadata)
