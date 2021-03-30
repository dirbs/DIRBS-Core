"""
DIRBS CSV/CLI reports generation scripts.

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
import sys
import csv
import json
import logging
import hashlib
import datetime
import contextlib

import numpy as np
from psycopg2 import sql
from dateutil import relativedelta

import dirbs.utils as utils
import dirbs.metadata as metadata
import dirbs.reports.exceptions as exceptions
import dirbs.partition_utils as partition_utils
from dirbs.dimensions.gsma_not_found import GSMANotFound


def _gen_metadata_for_reports(filenames: list, output_dir: str) -> list:
    """
    Function to generate a metadata dictionary for a list file pointer and a passed number of records.

    Arguments:
        filenames: list of file names to generate metadata
        output_dir: path of output directory
    Returns:
        rv: list of metadata dictionaries

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


def write_report(report: callable, month: int, year: int, output_dir: str, filename_prefix: str,
                 css_filename: str, js_filename: str, per_tac_compliance_data: dict) -> callable:
    """
    Helper function to write an individual report to disk.

    Arguments:
        report: Report type to write
        month: month of the generated report
        year: year of the generated report
        output_dir: path of the output dir
        filename_prefix: Prefix of report file name
        css_filename: name of the css file
        js_filename: name of the JS file
        per_tac_compliance_data: compliance data of tacs
    Returns:
        Reports metadata

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
                        writer.writerow([tac] + combination_list + [compliance_stats[key] for key in value_keys])
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


def _validate_data_partitions(config: callable, conn: callable, month: int, year: int, logger: callable,
                              disable_data_check: bool) -> None:
    """
    Validate that data is present for all configured operators and only configured operators.

    Arguments:
        config: DIRBS config object
        conn: DIRBS postgresql connection object
        month: data partition month
        year: data partition year
        logger: DIRBS logger object
        disable_data_check: boolean to disable data check
    Returns:
        None
    Raises:
        MissingOperatorDataException: if monthly_network_triplets_per_mno partition is missing for any operator
        ExtraOperatorDataException: if monthly_network_triplets_per_mno partition is detected for unconfigured mno
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


def write_country_gsma_not_found_report(conn: callable, config: callable, month: int,
                                        year: int, country_name: str, output_dir: str) -> callable:
    """
    Helper function to write out the country-wide GSMA not found report.

    Arguments:
        conn: DIRBS PostgreSQL connection object
        config: DIRBS config object
        month: reporting month
        year: reporting year
        country_name: name of the country
        output_dir: output directory path
    Returns:
        Report metadata
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


def write_country_duplicates_report(conn: callable, config: callable, month: int, year: int,
                                    country_name: str, output_dir: str, imsi_min_limit: int = 5) -> callable:
    """
    Helper function to write out the country-wide duplicates report.

    Arguments:
        conn: DIRBS PostgreSQL connection object
        config: DIRBS config object
        month: month of the report
        year: year of the report
        country_name: name of the country
        output_dir: output directory path
        imsi_min_limit: minimum IMSI limit (default 5)
    Returns:
        Reports metadata
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


def write_condition_imei_overlaps(conn: callable, config: callable, month: int, year: int,
                                  country_name: str, output_dir: str, cond_names: list) -> callable:
    """
    Helper function to write out IMEIs that are seen on multiple operators that have been classified.

    Arguments:
        conn: DIRBS PostgreSQL connection object
        config: DIRBS config object
        month: month of the report
        year: year of the report
        country_name: name of the country
        output_dir: output directory path
        cond_names: list of condition names
    Returns:
        Reports metadata
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


def make_report_directory(ctx: callable, base_dir: str, run_id: int, conn: callable,
                          config: callable, class_run_id: int = None, **extra_options) -> str:
    """
    Make directory based on timestamp, data_id and class_run_id.

    Arguments:
        ctx: click cli context
        base_dir: path to the base directory
        run_id: run_id of the current job
        conn: DIRBS PostgreSQL connection object
        config: DIRBS config object
        class_run_id: run if of the classification job
        extra_options: extra options dir
    Returns:
        report_dir: Report directory path
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
def reports_validation_checks(disable_retention_check: bool, year: int, month: int, logger: callable,
                              config: callable, conn: callable, disable_data_check: callable) -> None:
    """
    Helper method to perform validation checks on reports.

    Arguments:
        disable_retention_check: bool to disable retention check
        year: year of the report
        month: month of the report
        logger: DIRBS logger object
        config: DIRBS config object
        conn: DIRBS PostgreSQL connection object
        disable_data_check: bool to disable data check
    Returns:
        None
    """
    _retention_window_check(disable_retention_check, year, month, config, logger)
    operators_configured_check(config, logger)
    _extra_missing_operator_check(config, conn, month, year, logger, disable_data_check)


def _retention_window_check(disable_retention_check: bool, year: int, month: int,
                            config: callable, logger: callable) -> None:
    """
    Helper method to perform retention check.

    Arguments:
        disable_retention_check: bool to disable retention check
        year: year of the report
        month: month of the report
        config: DIRBS config object
        logger: DIRBS logger object
    Returns:
        None
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


def operators_configured_check(config: callable, logger: callable) -> None:
    """
    Helper method to perform configured operators check.

    Arguments:
        config: DIRBS config object
        logger: DIRBS logger object
    Returns:
        None
    """
    # Fail if there are no configured operators
    operators = config.region_config.operators
    if len(operators) == 0:
        logger.error('No operators configured in region config. No report can be generated...')
        sys.exit(1)


def _extra_missing_operator_check(config: callable, conn: callable, month: int,
                                  year: int, logger: callable, disable_data_check: bool) -> None:
    """
    Process extra missing operator check.

    Arguments:
        config: DIRBS config object
        conn: DIRBS PostgreSQL connection object
        month: month of the report
        year: year of the report
        logger: DIRBS logger object
        disable_data_check: bool to dosable data check
    Returns:
        None
    """
    # Validate that data is present for all configured operators and only configured operators
    try:
        _validate_data_partitions(config, conn, month, year, logger, disable_data_check)
    except (exceptions.ExtraOperatorDataException, exceptions.MissingOperatorDataException):
        logger.error('Extra or missing operator data detected above will skew report counts, so report '
                     'will not be generated. To ignore this warning, use the --disable-data-check option')
        sys.exit(1)


def write_stolen_violations(config: callable, logger: callable, report_dir: str, conn: callable,
                            filter_by_conditions: list, newer_than: str) -> callable:
    """Helper method to write per operator stolen list violation reports.

    Arguments:
        config: DIRBS config object
        logger: DIRBS logger object
        report_dir: path to report directory
        conn: DIRBS PostgreSQL connection object
        filter_by_conditions: list of condition to filter
        newer_than: violation newer then this date
    Return:
        Report metadata
    """
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
                                                     AND end_date IS NULL)"""  # noqa: Q449
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

    return _gen_metadata_for_reports(list(filename_op_map.keys()), report_dir)


def write_non_active_pairs(conn: callable, logger: callable, report_dir: str, last_seen_date: str) -> callable:
    """Helper method to write non active paris over the network for a specific period.

    Arguments:
        conn: DIRBS PostgreSQL connection object
        logger: DIRBS logger object
        report_dir: reporting directory
        last_seen_date: date on which pair is last_seen
    Returns:
        report metadata
    """
    logger.info('Generating Non-Active Pairs report...')
    with open(os.path.join(report_dir, 'non_active_pairs_{0}.csv'.format(last_seen_date)),
              'w', encoding='utf-8') as pairs_file, conn.cursor() as cursor:
        csv_writer = csv.DictWriter(pairs_file,
                                    fieldnames=['imei_norm', 'imsi', 'last_seen'],
                                    extrasaction='ignore')
        csv_writer.writeheader()
        cursor.execute(sql.SQL("""SELECT pl.imei_norm, pl.imsi, mnt.last_seen
                                    FROM pairing_list AS pl
                              INNER JOIN monthly_network_triplets_country AS mnt
                                         ON pl.imei_norm = mnt.imei_norm
                                     AND pl.imsi = mnt.imsi
                                   WHERE mnt.last_seen < %s"""), [last_seen_date])
        num_written_records = 0
        for row_data in cursor:
            csv_writer.writerow(row_data._asdict())
            num_written_records += 1

        cursor.execute('SELECT COUNT(*) FROM pairing_list')
        total_records = cursor.fetchone()[0]
        logger.info('total_records: {0}, written_records: {1}'.format(total_records, num_written_records))

    return _gen_metadata_for_reports(['non_active_pairs_{0}.csv'.format(last_seen_date)], report_dir)


def write_un_registered_subscribers(logger: callable, config: callable, report_dir: str, conn: callable,
                                    newer_than: str) -> callable:
    """Helper method to write per operator unregistered subscribers reports.

    Arguments:
        logger: DIRBS logger object
        config: DIRBS config object
        report_dir: reporting dir path
        conn: DIRBS PostgreSQL connection object
        newer_than: newer than this date
    Returns:
        metadata
    """
    logger.info('Generating per-MNO unregistered subscribers list...')
    with contextlib.ExitStack() as stack:
        # push files to the exit stack so that they will all be closed properly.
        operator_ids = [o.id for o in config.region_config.operators]
        filename_op_map = {'unregistered_subscribers_{0}.csv'.format(o): o for o in operator_ids}
        opname_file_map = {o: stack.enter_context(open(os.path.join(report_dir, fn), 'w', encoding='utf-8'))
                           for fn, o in filename_op_map.items()}

        # create a map from operator name to csv writer
        opname_csvwriter_map = {o: csv.writer(opname_file_map[o]) for o in operator_ids}

        # write the header to each file
        for _, writer in opname_csvwriter_map.items():
            writer.writerow(['imsi', 'first_seen', 'last_seen'])

        # query to find all the unregistered imsis across the operators
        with conn.cursor() as cursor:
            query = sql.SQL("""SELECT imsi, first_seen, last_seen, operator_id
                                 FROM monthly_network_triplets_per_mno_no_null_imeis AS mno
                                WHERE NOT EXISTS (SELECT 1
                                                    FROM subscribers_registration_list
                                                   WHERE imsi = mno.imsi) {0}""")

            if newer_than:
                newer_than_query = 'AND last_seen > %s'
                sql_bytes = cursor.mogrify(newer_than_query, [newer_than])
                date_filter_sql = sql.SQL(str(sql_bytes, conn.encoding))
            else:
                date_filter_sql = sql.SQL('')

            cursor.execute(query.format(date_filter_sql))
            for res in cursor:
                opname_csvwriter_map[res.operator_id].writerow([res.imsi,
                                                                res.first_seen.strftime('%Y%m%d'),
                                                                res.last_seen.strftime('%Y%m%d')])

    logger.info('per-MNO unregistered subscribers list generated successfully')
    return _gen_metadata_for_reports(list(filename_op_map.keys()), report_dir)


def write_classified_triplets(logger: callable, conditions: list, report_dir: str, conn: callable):
    """Helper method to write classified triplets reports.

    Arguments:
        logger: DIRBS logger object
        conditions: conditions list to write report about
        report_dir: reporting directory
        conn: DIRBS PostgreSQL connection
    Returns:
        Report Metadata
    """
    logger.info('Generating per-condition classified triplets list...')
    with contextlib.ExitStack() as stack:
        # push files to the exit stack, to close them properly
        condition_labels = [c.label for c in conditions]
        filename_cond_map = {'classified_triplets_{0}.csv'.format(c): c for c in condition_labels}
        cond_label_file_map = {c: stack.enter_context(open(os.path.join(report_dir, fn), 'w', encoding='utf-8'))
                               for fn, c in filename_cond_map.items()}

        # create mapping between condition label and csv writer
        cond_label_csvwriter_map = {c: csv.writer(cond_label_file_map[c]) for c in condition_labels}

        # write headers to the files
        for _, writer in cond_label_csvwriter_map.items():
            writer.writerow(['imei', 'imsi', 'msisdn', 'operator'])

        # run query to find all classified triplets for the given conditions
        with conn.cursor() as cursor:
            query = """SELECT cs.imei_norm AS imei, cs.cond_name, mno.imsi,
                              mno.msisdn, mno.operator_id AS operator
                         FROM classification_state AS cs
                   INNER JOIN monthly_network_triplets_per_mno_no_null_imeis AS mno
                              ON mno.imei_norm = cs.imei_norm
                        WHERE cs.cond_name IN %s
                          AND cs.virt_imei_shard = calc_virt_imei_shard(cs.imei_norm)
                          AND cs.end_date IS NULL
                          AND mno.virt_imei_shard = calc_virt_imei_shard(mno.imei_norm)"""  # noqa: Q440
            sql_bytes = cursor.mogrify(query, [tuple([c for c in condition_labels])])
            query = sql.SQL(str(sql_bytes, conn.encoding))
            cursor.execute(query)

            for res in cursor:
                cond_label_csvwriter_map[res.cond_name].writerow([res.imei, res.imsi, res.msisdn, res.operator])
        logger.info('Per-condition classified triplets list generated successfully.')

    return _gen_metadata_for_reports(list(filename_cond_map.keys()), report_dir)


def write_blacklist_violations(logger: callable, config: callable, report_dir: str,
                               conn: callable, month: int, year: int) -> callable:
    """Helper method to write per operator blacklist violation report.

    Arguments:
        logger: DIRBS logger object
        config: DIRBS config object
        report_dir: reporting directory
        conn: DIRBS PostgreSQL connection object
        month: reporting month
        year: reporting year
    Returns:
        Report Metadata
    """
    logger.info('Generating per-MNO blacklist violations...')
    with contextlib.ExitStack() as stack:
        # push files to the stack to handle
        operator_ids = [o.id for o in config.region_config.operators]
        filename_op_map = {'blacklist_violations_{0}.csv'.format(o): o for o in operator_ids}
        opname_file_map = {o: stack.enter_context(open(os.path.join(report_dir, fn), 'w', encoding='utf-8'))
                           for fn, o in filename_op_map.items()}
        opname_csvwriter_map = {o: csv.writer(opname_file_map[o]) for o in operator_ids}

        # write the headers
        for _, writer in opname_csvwriter_map.items():
            writer.writerow(['imei', 'last_seen'])

        # query to find blacklist violations
        with conn.cursor() as cursor:
            cursor.execute("""SELECT imei_norm AS imei, last_seen, operator_id
                                FROM classification_state
                                JOIN monthly_network_triplets_per_mno_no_null_imeis
                               USING (imei_norm)
                               WHERE triplet_month = %s
                                 AND triplet_year = %s
                                 AND end_date IS NULL
                                 AND block_date IS NOT NULL
                                 AND last_seen > block_date""",
                           [month, year])
            for res in cursor:
                opname_csvwriter_map[res.operator_id].writerow([res.imei, res.last_seen])
        logger.info('Per-MNO blacklist violation generated successfully.')
    return _gen_metadata_for_reports(list(filename_op_map.keys()), report_dir)


def write_association_list_violations(logger: callable, config: callable, report_dir: str,
                                      conn: callable, month: int, year: int) -> callable:
    """Helper method to write association list violations reports.

     Arguments:
        logger: DIRBS logger object
        config: DIRBS config object
        report_dir: reporting directory
        conn: DIRBS PostgreSQL connection object
        month: reporting month
        year: reporting year
    Returns:
        Report Metadata
    """
    logger.info('Generating per-MNO association list violations...')
    with contextlib.ExitStack() as stack:
        operator_ids = [o.id for o in config.region_config.operators]
        filename_op_map = {'association_violations_{0}.csv'.format(o): o for o in operator_ids}
        opname_file_map = {o: stack.enter_context(open(os.path.join(report_dir, fn), 'w', encoding='utf-8'))
                           for fn, o in filename_op_map.items()}
        opname_csvwriter_map = {o: csv.writer(opname_file_map[o]) for o in operator_ids}

        for _, writer in opname_csvwriter_map.items():
            writer.writerow(['imei', 'imsi', 'msisdn', 'first_seen', 'last_seen'])

        with conn.cursor() as cursor:
            cursor.execute("""SELECT imei_norm imei, imsi, msisdn, first_seen, last_seen, operator_id
                                FROM monthly_network_triplets_per_mno_no_null_imeis mno
                               WHERE NOT EXISTS(SELECT 1
                                                  FROM (SELECT imei_norm, imsi
                                                          FROM device_association_list dal
                                                    INNER JOIN subscribers_registration_list srl
                                                               ON dal.uid = srl.uid) association
                                                 WHERE mno.imei_norm = association.imei_norm
                                                   AND mno.imsi = association.imsi)
                                 AND mno.triplet_month = %s
                                 AND mno.triplet_year = %s""", [month, year])  # noqa: Q449
            for res in cursor:
                opname_csvwriter_map[res.operator_id].writerow([res.imei,
                                                                res.imsi,
                                                                res.msisdn,
                                                                res.first_seen,
                                                                res.last_seen])
        logger.info('Per-MNO association list violations list generated successfully.')
    return _gen_metadata_for_reports(list(filename_op_map.keys()), report_dir)


def write_transient_msisdns(logger: callable, period: int, report_dir: str, conn: callable, config: callable,
                            num_of_imeis: int, current_date: str = None):
    """Helper method to write transient msisdns report.

    Arguments:
        logger: DIRBS logger object
        period: analysis period in days
        report_dir: output directory to write files into
        conn: DIRBS postgresql connection object
        config: DIRBS config object
        num_of_imeis: Number of IMEIs to be seen with
        current_date: setting custom current date for analysis
    Returns:
        Report metadata
    """
    logger.info('Generating per-operator possible transient MSISDNs list...')
    with contextlib.ExitStack() as stack:
        # push files to the exit stack so that they will closed properly at the end
        operator_ids = [o.id for o in config.region_config.operators]
        filename_op_map = {'transient_msisdns_{0}.csv'.format(o): o for o in operator_ids}
        opname_file_map = {o: stack.enter_context(open(os.path.join(report_dir, fn), 'w', encoding='utf-8'))
                           for fn, o in filename_op_map.items()}

        # create a map from operator name to csv writer
        opname_csvwriter_map = {o: csv.writer(opname_file_map[o]) for o in operator_ids}

        # write header to each file
        for _, writer in opname_csvwriter_map.items():
            writer.writerow(['msisdn'])

        # the operation begins here
        # compute time periods for analysis
        current_date = datetime.date.today() if current_date is None else current_date
        analysis_end_date = utils.compute_analysis_end_date(conn, current_date)
        analysis_start_date = analysis_end_date - relativedelta.relativedelta(days=period)
        logger.debug('Analysis start date: {0}, analysis_end_date: {1}'.format(analysis_start_date, analysis_end_date))
        with conn.cursor() as cursor:
            query_bit_counts_in_period = sql.SQL("""SELECT msisdn, imeis_count, operator_id
                                                      FROM (SELECT msisdn, operator_id, SUM(bit) AS imeis_count
                                                              FROM (SELECT msisdn, operator_id,
                                                                           get_bitmask_within_window(
                                                                                date_bitmask,
                                                                                first_seen,
                                                                                last_seen,
                                                                                {analysis_start_date},
                                                                                {analysis_start_dom},
                                                                                {analysis_end_date},
                                                                                {analysis_end_dom}
                                                                                ) AS date_bitmask
                                                                      FROM monthly_network_triplets_per_mno
                                                                     WHERE last_seen >= {analysis_start_date}
                                                                       AND first_seen < {analysis_end_date}
                                                                       AND is_valid_msisdn(msisdn)) mn
                                                        CROSS JOIN generate_series(0, 30) AS i
                                                        CROSS JOIN LATERAL get_bit(mn.date_bitmask::bit(31), i) AS bit
                                                          GROUP BY msisdn, operator_id) AS msisdns_to_imeis
                                                     WHERE imeis_count/{period} >= {num_of_imeis}""").format(
                analysis_start_date=sql.Literal(analysis_start_date),
                analysis_start_dom=sql.Literal(analysis_start_date.day),
                analysis_end_date=sql.Literal(analysis_end_date),
                analysis_end_dom=sql.Literal(analysis_end_date.day),
                period=sql.Literal(period),
                num_of_imeis=sql.Literal(num_of_imeis)
            )
            cursor.execute(query_bit_counts_in_period.as_string(conn))
            msisdn_to_imei_count_map = [
                {
                    'msisdn': res.msisdn,
                    'imei_count': res.imeis_count,
                    'operator_id': res.operator_id
                } for res in cursor]

            possible_transients = []  # dict to identify possible transients based on tests
            for val in msisdn_to_imei_count_map:
                imei_extraction_query = sql.SQL("""SELECT DISTINCT imei_norm
                                                     FROM monthly_network_triplets_country_no_null_imeis
                                                    WHERE msisdn = {msisdn}
                                                      AND last_seen >= {analysis_start_date}
                                                      AND first_seen < {analysis_end_date}
                                                 ORDER BY imei_norm ASC""").format(
                    msisdn=sql.Literal(val.get('msisdn')),
                    analysis_start_date=sql.Literal(analysis_start_date),
                    analysis_end_date=sql.Literal(analysis_end_date)
                )
                cursor.execute(imei_extraction_query)
                imei_list = [res.imei_norm for res in cursor if res.imei_norm.isnumeric()]
                tac_list = [int(imei[:8]) for imei in imei_list]
                imei_list = list(map(int, imei_list))

                analysis_tests = {
                    'identical_tac': False,
                    'consecutive_tac': False,
                    'arithmetic_tac': False,
                    'consecutive_imei': False,
                    'arithmetic_imei': False
                }

                logger.info('Performing TAC analysis on the data...')
                if len(set(tac_list)) == 1:
                    analysis_tests['identical_tac'] = True
                else:
                    if _have_consecutive_numbers(tac_list):
                        analysis_tests['consecutive_tac'] = True

                    if _is_arithmetic_series(tac_list):
                        analysis_tests['arithmetic_tac'] = True

                logger.info('Performing IMEIs analysis on data...')
                if _have_consecutive_numbers(imei_list):
                    analysis_tests['consecutive_imei'] = True

                if _is_arithmetic_series(imei_list):
                    analysis_tests['arithmetic_imei'] = True

                if any(analysis_tests.values()):
                    possible_transients.append(val)

            for item in possible_transients:
                opname_csvwriter_map[item.get('operator_id')].writerow([item.get('msisdn')])

        logger.info('Per-MNO possible transient MSISDN lists generated successfully.')
    return _gen_metadata_for_reports(list(filename_op_map.keys()), report_dir)


def _have_consecutive_numbers(input_list: list):
    """
    Helper method to detect weather a list of numbers are consecutive in order.

    Arguments:
        input_list: input list of integers
    Returns:
        Boolean: True/False
    """
    return True if sum(np.diff(sorted(input_list))) == (len(input_list) - 1) else False


def _is_arithmetic_series(input_list: list):
    """
    Helper method to detect weather a list of numbers are arithmetic in order.

    Arguments:
        input_list: input list of integers
    Returns:
        Boolean: True/False
    """
    diff_arr = np.diff(input_list)
    return np.all(diff_arr == diff_arr[0])
