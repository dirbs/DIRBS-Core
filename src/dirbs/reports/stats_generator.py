"""
DIRBS reporting class for generating data for operator/country reports.

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

import logging
import datetime
from concurrent import futures
from collections import defaultdict, namedtuple
from operator import attrgetter
from functools import partial
import json
import copy

from psycopg2 import sql
from psycopg2.extras import execute_values
from dateutil import relativedelta

from dirbs import report_schema_version
from dirbs.config.region import OperatorConfig
import dirbs.utils as utils
import dirbs.partition_utils as part_utils


ConditionTuple = namedtuple('ConditionTuple', ['label', 'blocking'])


def generate_monthly_report_stats(config, conn, month, year, statsd, metrics_run_root, run_id, refresh_data=True,
                                  debug_query_performance=False):
    """Either generates stats for the reports or returns the data_id to use when using cached data."""
    logger = logging.getLogger('dirbs.report')
    with conn.cursor() as cursor:
        # First, look for any existing report data for this month
        cursor.execute("""SELECT rdm.data_id,
                                 rdm.class_run_id
                            FROM report_data_metadata rdm
                            JOIN (SELECT MAX(data_id) AS data_id
                                    FROM report_data_metadata
                                   WHERE report_month = %s
                                     AND report_year = %s
                                     AND data_schema_version = %s) latest_data_id
                           USING (data_id)""",
                       [month, year, report_schema_version])
        result = cursor.fetchone()
        if result:
            data_id, class_run_id = result
        else:
            data_id, class_run_id = (None, None)

        # Commit connection here to prevent long-running transaction
        conn.commit()
        per_tac_compliance_data = None
        statsd.gauge('{0}refreshed_data'.format(metrics_run_root), int(refresh_data or not data_id))
        if not data_id or refresh_data:
            # If we don't have data for this month or we asked to generate new data, create a new entry in
            # report_metadata
            logger.info('No data previously generated for this month or refresh requested')
            data_id, class_run_id, per_tac_compliance_data = _refresh_data(config, conn, month, year, statsd,
                                                                           metrics_run_root, run_id,
                                                                           debug_query_performance)
            # Commit the connection so we can re-use this data even if subsequent operations fail
            conn.commit()
        else:
            logger.info('Previous report data detected for this month. Re-generating report using that data')
        return data_id, class_run_id, per_tac_compliance_data


def _store_report_data_metadata(conn, month, year, class_run_id):
    """Store new metadata about this data generation run and return the data_id for stats storage."""
    with conn.cursor() as cursor:
        cursor.execute("""INSERT INTO report_data_metadata(data_date, report_year, report_month,
                                                           data_schema_version, class_run_id)
                                      VALUES(%s, %s, %s, %s, %s)
                            RETURNING data_id""",
                       [datetime.date.today(), year, month, report_schema_version, class_run_id])
        return cursor.fetchone()[0]


def _sort_conditions(condition_tuples):
    """Sorts a list of condition_name, blocking tuples into the order expected by the report."""
    # Input format is (name, blocking) -> This put blocking conditions first, then sorts by name
    condition_tuples = sorted(condition_tuples, key=attrgetter('label'))
    condition_tuples = sorted(condition_tuples, key=attrgetter('blocking'), reverse=True)
    return condition_tuples


def _log_perf_metric(statsd, metric_root, stat_name, duration, operator_id=None, record_counts_map=None):
    """Utility method used to store a StatsD performance metric for an operator ID, stat_name and duration."""
    triplet_count = -1
    stat_name_norm = stat_name.lower().replace(' ', '_')
    if operator_id == OperatorConfig.COUNTRY_OPERATOR_NAME or operator_id is None:
        metric_key = '{0}runtime.per_stat.country.{1}'.format(metric_root, stat_name_norm)
        if record_counts_map is not None:
            triplet_count = record_counts_map[OperatorConfig.COUNTRY_OPERATOR_NAME]
    else:
        metric_key = '{0}runtime.per_stat.operators.{1}.{2}'.format(metric_root, operator_id, stat_name_norm)
        if record_counts_map is not None:
            triplet_count = record_counts_map[operator_id]

    statsd.gauge('{0}.raw'.format(metric_key), duration)
    if triplet_count > 0:
        norm_factor = 1000000 / triplet_count
        statsd.gauge('{0}.normalized_triplets'.format(metric_key), norm_factor * duration)


def _refresh_data(config, conn, month, year, statsd, metrics_run_root, run_id, debug_query_performance):
    """Refreshes reporting stats from the DB and stores aggregated results into the various reporting tables."""
    logger = logging.getLogger('dirbs.report')
    nworkers = config.multiprocessing_config.max_db_connections
    db_config = config.db_config
    # Sort conditions by primary and secondary key
    condition_tuples = [ConditionTuple(x.label, x.blocking) for x in config.conditions]
    condition_tuples = _sort_conditions(condition_tuples)
    if len(condition_tuples) == 0:
        logger.warning('No conditions defined in config: No stats on classification will be generated in the report')

    # We need to get the list of operators out of the config
    operators = [op.id for op in config.region_config.operators]
    # Calculate days in month
    days_in_month = (datetime.date(year, month, 1) + relativedelta.relativedelta(months=1, days=-1)).day
    # Init variables for storing data
    per_operator_record_counts = defaultdict(int)
    per_operator_monthly_stats = defaultdict(lambda: defaultdict(int))
    per_operator_daily_stats = defaultdict(lambda: [defaultdict(int) for i in range(0, days_in_month)])
    per_operator_top_model_imei_counts = {}
    per_operator_top_model_gross_adds = {}
    per_operator_imei_imsi_overloading = {}
    per_operator_imsi_imei_overloading = {}
    per_operator_condition_counts = {}
    per_operator_tac_compliance_data = {}
    per_operator_compliance_data = {}
    per_operator_daily_imei_imsi_overloading = {}

    # We use the per-operator record counts to normalize performance numbers, so we need to do this first
    # in a separate executor
    with futures.ProcessPoolExecutor(max_workers=nworkers) as executor:
        logger.info('Simultaneously calculating data volume for each operator using {0:d} workers...'
                    .format(nworkers))
        logger.info('Queueing jobs to calculate monthly record counts...')
        futures_to_cb = {}
        _queue_record_count_jobs(executor, futures_to_cb, per_operator_record_counts, db_config,
                                 operators, month, year, statsd, metrics_run_root, debug_query_performance)

        # Process futures as they are completed, calling the associated callback passing the
        # future as the only argument (other arguments to the callback get partially applied
        # during the queue* functions above)
        for f in futures.as_completed(futures_to_cb):
            futures_to_cb[f](f)

        logger.info('Simultaneously calculating report data using {0:d} workers...'
                    .format(nworkers))
        logger.info('Queueing jobs to calculate stats...')
        futures_to_cb = {}
        _queue_compliance_jobs(executor, futures_to_cb, per_operator_condition_counts,
                               per_operator_tac_compliance_data, per_operator_compliance_data,
                               per_operator_monthly_stats, db_config, operators, month, year, condition_tuples,
                               per_operator_record_counts, statsd, metrics_run_root, debug_query_performance, run_id)
        _queue_imsi_imei_overloading_jobs(executor, futures_to_cb, per_operator_imsi_imei_overloading,
                                          db_config, operators, month, year, per_operator_record_counts,
                                          statsd, metrics_run_root, debug_query_performance)
        _queue_imei_imsi_overloading_jobs(executor, futures_to_cb, per_operator_imei_imsi_overloading,
                                          db_config, operators, month, year, per_operator_record_counts,
                                          statsd, metrics_run_root, debug_query_performance)
        _queue_daily_imei_imsi_overloading_jobs(executor, futures_to_cb, per_operator_daily_imei_imsi_overloading,
                                                db_config, operators, month, year, per_operator_record_counts,
                                                statsd, metrics_run_root, debug_query_performance)
        _queue_monthly_stats_jobs(executor, futures_to_cb, per_operator_monthly_stats, db_config, operators,
                                  month, year, per_operator_record_counts, statsd, metrics_run_root,
                                  debug_query_performance)
        _queue_top_model_gross_adds_jobs(executor, futures_to_cb, per_operator_top_model_gross_adds,
                                         db_config, operators, month, year, per_operator_record_counts, statsd,
                                         metrics_run_root, debug_query_performance)
        _queue_top_model_imei_jobs(executor, futures_to_cb, per_operator_top_model_imei_counts,
                                   db_config, operators, month, year, per_operator_record_counts,
                                   statsd, metrics_run_root, debug_query_performance)
        _queue_distinct_id_counts_jobs(executor, futures_to_cb, per_operator_monthly_stats, per_operator_daily_stats,
                                       db_config, operators, month, year, per_operator_record_counts, statsd,
                                       metrics_run_root, debug_query_performance)
        logger.info('Queued jobs to calculate stats. Processing will begin now...')

        # Process futures as they are completed, calling the associated callback passing the
        # future as the only argument (other arguments to the callback get partially applied
        # during the queue* functions above)
        for f in futures.as_completed(futures_to_cb):
            futures_to_cb[f](f)

    # The hll relative error is given by the expression ±1.04/√(2 ** log2m)
    # Check parameter log2m using SELECT hll_print(imei_hll) FROM daily_per_mno_hll_sketches LIMIT 1
    # or look up for the parameter in the file hll.c in postgres db container
    log2m = 11
    theoretical_error = 1.04 / 2 ** (log2m / 2) * 100
    all_ops = operators + [OperatorConfig.COUNTRY_OPERATOR_NAME]
    for op in all_ops:
        # Check whether compliance stats add up for each operator and log warning if not
        mc = per_operator_monthly_stats[op]
        ti = mc['num_imeis']
        ci = mc['num_compliant_imeis']
        nci = mc['num_noncompliant_imeis']
        percentage_error = abs(ci + nci - ti) / ti * 100 if ti > 0 else 0.0
        logger.info('Percentage error in monthly IMEI count (HLL vs. exact) for '
                    'operator {0}: {1:.3f}% (theoretical max error {2:.3f}%)'
                    .format(op, percentage_error, theoretical_error))
        mc['num_imeis'] = ci + nci
        if op == OperatorConfig.COUNTRY_OPERATOR_NAME:
            imei_stats_metric_root = '{0}monthly_stats.country.'.format(metrics_run_root)
        else:
            imei_stats_metric_root = '{0}monthly_stats.operators.{1}.'.format(metrics_run_root, op)

        # Store all monthly stat metrics in StatsD
        for k, v in mc.items():
            statsd.gauge('{0}{1}'.format(imei_stats_metric_root, k), v)

    logger.info('Finished calculating report data')
    logger.info('Storing report data in DB...')
    data_id, class_run_id = _store_report_data(conn,
                                               operators,
                                               month,
                                               year,
                                               condition_tuples,
                                               per_operator_record_counts,
                                               per_operator_daily_stats,
                                               per_operator_monthly_stats,
                                               per_operator_condition_counts,
                                               per_operator_top_model_imei_counts,
                                               per_operator_top_model_gross_adds,
                                               per_operator_imei_imsi_overloading,
                                               per_operator_imsi_imei_overloading,
                                               per_operator_compliance_data,
                                               per_operator_daily_imei_imsi_overloading,
                                               statsd,
                                               metrics_run_root)
    logger.info('Finished storing report data in DB')
    return data_id, class_run_id, per_operator_tac_compliance_data


def _queue_record_count_jobs(executor, futures_to_cb, results, db_config, operators, month, year,
                             statsd, metrics_run_root, debug_query_performance):
    """Helper function to queue jobs to calculate the record counts for each operator."""
    for op in operators:
        futures_to_cb[executor.submit(_calc_record_count, db_config, month, year, op)] \
            = partial(_process_per_operator_monthly_future, op, 'monthly record count',
                      statsd, metrics_run_root, results, debug_query_performance)
    futures_to_cb[executor.submit(_calc_record_count, db_config, month, year)] \
        = partial(_process_per_operator_monthly_future,
                  OperatorConfig.COUNTRY_OPERATOR_NAME,
                  'monthly record count',
                  statsd,
                  metrics_run_root,
                  results,
                  debug_query_performance)


def _queue_distinct_id_counts_jobs(executor, futures_to_cb, monthly_results, daily_results, db_config, operators,
                                   month, year, per_operator_record_counts, statsd, metrics_run_root,
                                   debug_query_performance):
    """Helper function to queue distinct identifier counts jobs (both daily and monthly and for all operators)."""
    futures_to_cb[executor.submit(_calc_distinct_id_counts, db_config, month, year)] \
        = partial(_process_id_counts_future, per_operator_record_counts, statsd, metrics_run_root, monthly_results,
                  daily_results, debug_query_performance)


def _queue_monthly_stats_jobs(executor, futures_to_cb, results, db_config, operators, month, year,
                              per_operator_record_counts, statsd, metrics_run_root, debug_query_performance):
    """Helper function to queue monthly stats jobs."""
    futures_to_cb[executor.submit(_calc_imei_gross_adds, db_config, operators, month, year)] \
        = partial(_process_monthly_future, 'IMEI gross add', per_operator_record_counts, statsd,
                  metrics_run_root, results, debug_query_performance)
    futures_to_cb[executor.submit(_calc_invalid_id_null_counts, db_config, month, year)] \
        = partial(_process_monthly_future, 'invalid identifier (null)', per_operator_record_counts, statsd,
                  metrics_run_root, results, debug_query_performance)
    futures_to_cb[executor.submit(_calc_invalid_id_pair_and_triplet_counts, db_config, month, year)] \
        = partial(_process_monthly_future, 'invalid identifier (pairs and triplets)', per_operator_record_counts,
                  statsd, metrics_run_root, results, debug_query_performance)


def _queue_top_model_imei_jobs(executor, futures_to_cb, results, db_config, operators, month, year,
                               per_operator_record_counts, statsd, metrics_run_root, debug_query_performance):
    """Helper function to queue top model by IMEI stats jobs."""
    for op in operators:
        futures_to_cb[executor.submit(_calc_top_models_imei, db_config, month, year, op)] \
            = partial(_process_per_operator_monthly_future, op, 'Top 10 models by IMEI count',
                      statsd, metrics_run_root, results, debug_query_performance,
                      per_operator_record_counts=per_operator_record_counts)
    futures_to_cb[executor.submit(_calc_top_models_imei, db_config, month, year)] \
        = partial(_process_per_operator_monthly_future,
                  OperatorConfig.COUNTRY_OPERATOR_NAME,
                  'Top 10 models by IMEI count',
                  statsd,
                  metrics_run_root,
                  results,
                  debug_query_performance,
                  per_operator_record_counts=per_operator_record_counts)


def _queue_top_model_gross_adds_jobs(executor, futures_to_cb, results, db_config, operators, month, year,
                                     per_operator_record_counts, statsd, metrics_run_root, debug_query_performance):
    """Helper function to queue top model by IMEI stats jobs."""
    for op in operators:
        futures_to_cb[executor.submit(_calc_top_models_gross_adds, db_config, month, year, op)] \
            = partial(_process_per_operator_monthly_future, op, 'Top 10 models by IMEI gross adds',
                      statsd, metrics_run_root, results, debug_query_performance,
                      per_operator_record_counts=per_operator_record_counts)
    futures_to_cb[executor.submit(_calc_top_models_gross_adds, db_config, month, year)] \
        = partial(_process_per_operator_monthly_future,
                  OperatorConfig.COUNTRY_OPERATOR_NAME,
                  'Top 10 models by IMEI gross adds',
                  statsd,
                  metrics_run_root,
                  results,
                  debug_query_performance,
                  per_operator_record_counts=per_operator_record_counts)


def _queue_compliance_jobs(executor, futures_to_cb, condition_counts, tac_compliance_data, compliance_data,
                           monthly_stats, db_config, operators, month, year, condition_tuples,
                           per_operator_record_counts, statsd, metrics_run_root, debug_query_performance, run_id):
    """Helper function to queue compliance stats jobs."""
    for op in operators:
        futures_to_cb[executor.submit(_calc_compliance_data, db_config, month, year, condition_tuples, operator=op,
                                      run_id=run_id)] \
            = partial(_process_per_operator_compliance_future,
                      op,
                      condition_counts,
                      per_operator_record_counts,
                      statsd,
                      metrics_run_root,
                      tac_compliance_data,
                      compliance_data,
                      monthly_stats,
                      debug_query_performance)
    futures_to_cb[executor.submit(_calc_compliance_data, db_config, month, year, condition_tuples, run_id=run_id)] \
        = partial(_process_per_operator_compliance_future,
                  OperatorConfig.COUNTRY_OPERATOR_NAME,
                  condition_counts,
                  per_operator_record_counts,
                  statsd,
                  metrics_run_root,
                  tac_compliance_data,
                  compliance_data,
                  monthly_stats,
                  debug_query_performance)


def _queue_imei_imsi_overloading_jobs(executor, futures_to_cb, results,
                                      db_config, operators, month, year, per_operator_record_counts,
                                      statsd, metrics_run_root, debug_query_performance):
    """Helper function to queue IMEI-IMSI overloading jobs."""
    for op in operators:
        futures_to_cb[executor.submit(_calc_imei_imsi_overloading, db_config, month, year, op)] \
            = partial(_process_per_operator_monthly_future, op, 'IMEI-IMSI overloading',
                      statsd, metrics_run_root, results, debug_query_performance,
                      per_operator_record_counts=per_operator_record_counts)
    futures_to_cb[executor.submit(_calc_imei_imsi_overloading, db_config, month, year)] \
        = partial(_process_per_operator_monthly_future,
                  OperatorConfig.COUNTRY_OPERATOR_NAME,
                  'IMEI-IMSI overloading',
                  statsd,
                  metrics_run_root,
                  results,
                  debug_query_performance,
                  per_operator_record_counts=per_operator_record_counts)


def _queue_daily_imei_imsi_overloading_jobs(executor, futures_to_cb, results,
                                            db_config, operators, month, year, per_operator_record_counts,
                                            statsd, metrics_run_root, debug_query_performance):
    """Helper function to queue average IMEI-IMSI overloading jobs."""
    for op in operators:
        futures_to_cb[executor.submit(_calc_daily_imei_imsi_overloading, db_config, month, year, op)] \
            = partial(_process_per_operator_monthly_future, op, 'avg daily IMEI-IMSI overloading',
                      statsd, metrics_run_root, results, debug_query_performance,
                      per_operator_record_counts=per_operator_record_counts)
    futures_to_cb[executor.submit(_calc_daily_imei_imsi_overloading, db_config, month, year)] \
        = partial(_process_per_operator_monthly_future,
                  OperatorConfig.COUNTRY_OPERATOR_NAME,
                  'avg daily IMEI-IMSI overloading',
                  statsd,
                  metrics_run_root,
                  results,
                  debug_query_performance,
                  per_operator_record_counts=per_operator_record_counts)


def _queue_imsi_imei_overloading_jobs(executor, futures_to_cb, results,
                                      db_config, operators, month, year, per_operator_record_counts,
                                      statsd, metrics_run_root, debug_query_performance):
    """Helper function to queue IMSI-IMEI overloading jobs."""
    for op in operators:
        futures_to_cb[executor.submit(_calc_imsi_imei_overloading, db_config, month, year, op)] \
            = partial(_process_per_operator_monthly_future, op, 'IMSI-IMEI overloading',
                      statsd, metrics_run_root, results, debug_query_performance,
                      per_operator_record_counts=per_operator_record_counts)
    futures_to_cb[executor.submit(_calc_imsi_imei_overloading, db_config, month, year)] \
        = partial(_process_per_operator_monthly_future,
                  OperatorConfig.COUNTRY_OPERATOR_NAME,
                  'IMSI-IMEI overloading',
                  statsd,
                  metrics_run_root,
                  results,
                  debug_query_performance,
                  per_operator_record_counts=per_operator_record_counts)


def _monthly_network_triplets_partition(*, conn, month, year, operator=None):
    """Calculates which triplets partition should be queries based on operator (or lack of)."""
    if operator is None:
        # If operator is None, return back a country partition
        partition_name = part_utils.monthly_network_triplets_country_partition(month=month, year=year)
        base_tbl_type = 'monthly_network_triplets_country'
    else:
        partition_name = part_utils.monthly_network_triplets_per_mno_partition(operator_id=operator,
                                                                               month=month, year=year)
        base_tbl_type = 'monthly_network_triplets_per_mno'

    partition_id = sql.Identifier(partition_name)
    with conn.cursor() as cursor:
        cursor.execute(utils.table_exists_sql(), [partition_name])
        if not cursor.fetchone().exists:
            # Fake a partition if one does not exist so that we don't need to do checks throughout the module in
            # each check
            partition_name = 'report_temp_{0}'.format(partition_name)
            cursor.execute(sql.SQL('CREATE TEMP TABLE IF NOT EXISTS {0} (LIKE {1})')
                           .format(partition_id, sql.Identifier(base_tbl_type)))

    return partition_id


def _calc_record_count(db_config, month, year, operator=None):
    """Calculates the number of records being used during report generation. Useful for normalizing per metrics."""
    with utils.create_db_connection(db_config) as conn, conn.cursor() as cursor, utils.CodeProfiler() as cp:
        network_triplets_partition = _monthly_network_triplets_partition(conn=conn, operator=operator,
                                                                         month=month, year=year)
        cursor.execute(sql.SQL("""SELECT COUNT(*) FROM {0}""").format(network_triplets_partition))
        record_count = cursor.fetchone()[0]
    return record_count, cp.duration, [cp.duration]


def _calc_distinct_id_counts(db_config, month, year):
    """Calculate per-operator and country daily and monthly ID counts for a given month and year."""
    with utils.create_db_connection(db_config) as conn, conn.cursor() as cursor, utils.CodeProfiler() as cp:
        results = defaultdict(lambda: dict(num_triplets=0, num_imeis=0, num_imsis=0, num_msisdns=0,
                                           num_imei_imsis=0, num_imei_msisdns=0, num_imsi_msisdns=0))
        cursor.execute(
            """SELECT operator_id,
                      data_date,
                      (hll_cardinality(COALESCE(hll_union_agg(triplet_hll), hll_empty())))::BIGINT AS num_triplets,
                      (hll_cardinality(COALESCE(hll_union_agg(imei_hll), hll_empty())))::BIGINT AS num_imeis,
                      (hll_cardinality(COALESCE(hll_union_agg(imsi_hll), hll_empty())))::BIGINT AS num_imsis,
                      (hll_cardinality(COALESCE(hll_union_agg(msisdn_hll), hll_empty())))::BIGINT AS num_msisdns,
                      (hll_cardinality(COALESCE(hll_union_agg(imei_imsis_hll), hll_empty())))::BIGINT
                        AS num_imei_imsis,
                      (hll_cardinality(COALESCE(hll_union_agg(imei_msisdns_hll), hll_empty())))::BIGINT
                        AS num_imei_msisdns,
                      (hll_cardinality(COALESCE(hll_union_agg(imsi_msisdns_hll), hll_empty())))::BIGINT
                        AS num_imsi_msisdns
                 FROM daily_per_mno_hll_sketches
                WHERE date_part('month', data_date) = %(month)s
                  AND date_part('year', data_date) = %(year)s
             GROUP BY CUBE (operator_id, data_date)
            """,
            {'month': month, 'year': year}
        )

        results = [res._asdict() for res in cursor]

    return results, cp.duration, [cp.duration]


def _calc_invalid_id_null_counts(db_config, month, year):
    """Calculate per-operator monthly stats about invalid data (null) for a given month and year."""
    with utils.create_db_connection(db_config) as conn, conn.cursor() as cursor, utils.CodeProfiler() as cp:
        results = defaultdict(lambda: dict(num_null_imei_records=0,
                                           num_null_imsi_records=0,
                                           num_null_msisdn_records=0))
        durations = []
        for column, result_label in [('imei_norm', 'num_null_imei_records'),
                                     ('imsi', 'num_null_imsi_records'),
                                     ('msisdn', 'num_null_msisdn_records')]:
            with utils.CodeProfiler() as scp:
                # Calculate per-operator NULL counts
                cursor.execute(sql.SQL("""SELECT operator_id,
                                                 COUNT(*) AS cnt
                                            FROM monthly_network_triplets_per_mno
                                           WHERE triplet_month = %s
                                             AND triplet_year = %s
                                             AND {0} IS NULL
                                        GROUP BY operator_id""").format(sql.Identifier(column)),
                               [month, year])
                for res in cursor:
                    results[res.operator_id][result_label] = res.cnt
            durations.append(scp.duration)

            with utils.CodeProfiler() as scp:
                # Calculate per-country NULL counts
                cursor.execute(sql.SQL("""SELECT COUNT(*) AS cnt
                                            FROM monthly_network_triplets_country
                                           WHERE triplet_month = %s
                                             AND triplet_year = %s
                                             AND {0} IS NULL""").format(sql.Identifier(column)),
                               [month, year])
                for res in cursor:
                    results[OperatorConfig.COUNTRY_OPERATOR_NAME][result_label] = res.cnt
            durations.append(scp.duration)

    return _defaultdict_to_regular(results), cp.duration, durations


def _calc_invalid_id_pair_and_triplet_counts(db_config, month, year):
    """Calculate per-operator monthly stats about invalid data (pairs and triplets) for a given month and year."""
    with utils.create_db_connection(db_config) as conn, conn.cursor() as cursor, utils.CodeProfiler() as cp:
        results = defaultdict(lambda: dict(num_invalid_imei_imsis=0, num_invalid_imei_msisdns=0,
                                           num_invalid_triplets=0))
        durations = []
        for columns, result_label in [(['imei_norm', 'imsi'], 'num_invalid_imei_imsis'),
                                      (['imei_norm', 'msisdn'], 'num_invalid_imei_msisdns'),
                                      (['imei_norm', 'imsi', 'msisdn'], 'num_invalid_triplets')]:
            # First, generate stats for each operator, for each stat
            col_identifiers = sql.SQL(', ').join(map(sql.Identifier, columns))
            null_filters = sql.SQL('({0})') \
                .format(sql.SQL(' OR ').join(map(lambda x: sql.SQL('{0} IS NULL').format(sql.Identifier(x)), columns)))

            with utils.CodeProfiler() as scp:
                # Calculate per-operator invalid stat count
                cursor.execute(sql.SQL("""SELECT operator_id,
                                                 COUNT(*) AS cnt
                                            FROM (SELECT DISTINCT operator_id, {0}
                                                    FROM monthly_network_triplets_per_mno
                                                   WHERE triplet_month = %s
                                                     AND triplet_year = %s
                                                     AND {1}) sq
                                        GROUP BY operator_id""").format(col_identifiers, null_filters),
                               [month, year])
                for res in cursor:
                    results[res.operator_id][result_label] = res.cnt
            durations.append(scp.duration)

            with utils.CodeProfiler() as scp:
                # Calculate per-country invalid stat count
                cursor.execute(sql.SQL("""SELECT COUNT(*) AS cnt
                                            FROM (SELECT DISTINCT {0}
                                                    FROM monthly_network_triplets_country
                                                   WHERE triplet_month = %s
                                                     AND triplet_year = %s
                                                     AND {1}) sq""").format(col_identifiers, null_filters),
                               [month, year])
                for res in cursor:
                    results[OperatorConfig.COUNTRY_OPERATOR_NAME][result_label] = res.cnt
            durations.append(scp.duration)

    return _defaultdict_to_regular(results), cp.duration, durations


def _calc_imei_gross_adds(db_config, operators, month, year):
    """Helper function to calculate IMEI gross adds."""
    with utils.create_db_connection(db_config) as conn, conn.cursor() as cursor, utils.CodeProfiler() as cp:
        start_date, end_date = _calc_date_range(month, year)
        all_operators = copy.copy(operators)
        all_operators.append(None)  # For country -- _monthly_network_triplets_partition will return country partition
        results = defaultdict(dict)
        for op in all_operators:
            network_triplets_partition = _monthly_network_triplets_partition(conn=conn, operator=op,
                                                                             month=month, year=year)
            cursor.execute(
                sql.SQL("""SELECT COUNT(*) AS num_gross_adds
                             FROM network_imeis
                            WHERE network_imeis.first_seen >= %s
                              AND network_imeis.first_seen < %s
                              AND EXISTS (SELECT 1
                                            FROM {0}
                                           WHERE imei_norm = network_imeis.imei_norm)
                        """).format(network_triplets_partition),
                [start_date, end_date]
            )
            if op is None:
                op = OperatorConfig.COUNTRY_OPERATOR_NAME
            results[op]['num_gross_adds'] = cursor.fetchone().num_gross_adds

    return _defaultdict_to_regular(results), cp.duration, [cp.duration]


def _calc_date_range(month, year):
    """Returns start_date, end_date tuple for this reporting month."""
    start_date = datetime.date(year, month, 1)
    end_date = start_date + relativedelta.relativedelta(months=1)
    return start_date, end_date


def _calc_top_models_imei(db_config, month, year, operator=None):
    """Helper function to calculate the top models by IMEI count."""
    with utils.create_db_connection(db_config) as conn:
        network_triplets_partition = _monthly_network_triplets_partition(conn=conn, operator=operator,
                                                                         month=month, year=year)
        return _calc_top_models_common(
            conn,
            sql.SQL("""SELECT SUBSTRING(imei_norm, 1, 8) AS tac,
                              COUNT(DISTINCT imei_norm) AS imei_count
                         FROM {0}
                        WHERE imei_norm IS NOT NULL
                     GROUP BY tac""").format(network_triplets_partition)
        )


def _calc_top_models_gross_adds(db_config, month, year, operator=None):
    """Helper function to calculate the top models by gross adds."""
    with utils.create_db_connection(db_config) as conn:
        start_date, end_date = _calc_date_range(month, year)
        network_triplets_partition = _monthly_network_triplets_partition(conn=conn, operator=operator,
                                                                         month=month, year=year)
        return _calc_top_models_common(
            conn,
            sql.SQL("""SELECT SUBSTRING(imei_norm, 1, 8) AS tac,
                              COUNT(imei_norm) AS imei_count
                         FROM network_imeis
                        WHERE network_imeis.first_seen >= %s
                          AND network_imeis.first_seen < %s
                          AND EXISTS (SELECT 1
                                        FROM {0}
                                       WHERE imei_norm = network_imeis.imei_norm)
                     GROUP BY tac
                    """).format(network_triplets_partition),
            [start_date, end_date]
        )


def _calc_top_models_common(conn, subquery, subquery_params=None):
    """Helper function to calculate the top models using a supplied subquery to supply a list of TACs and counts."""
    if subquery_params is None:
        subquery_params = []

    with conn.cursor() as cursor, utils.CodeProfiler() as cp:
        cursor.execute(sql.SQL("""SELECT manufacturer,
                                         model,
                                         total_imei_count,
                                         string_agg(technology_generation, '/') AS tech_generations
                                    FROM (SELECT manufacturer,
                                                 model,
                                                 total_imei_count,
                                                 technology_generation
                                            FROM (SELECT model_name AS model,
                                                         manufacturer::TEXT AS manufacturer,
                                                         SUM(st.imei_count)::BIGINT AS total_imei_count,
                                                         bit_or(rat_bitmask) AS model_rat
                                                    FROM gsma_data
                                                    JOIN ({0}) AS st
                                                   USING (tac)
                                                GROUP BY manufacturer, model_name
                                                ORDER BY total_imei_count DESC
                                                   LIMIT 10) top_10_models
                                            JOIN LATERAL (SELECT bitmask_to_set_bit_positions(top_10_models.model_rat)
                                                         AS model_gsma_rank) model_ranks
                                                 ON TRUE
                                            JOIN (SELECT DISTINCT gsma_rank, technology_generation
                                                    FROM radio_access_technology_map) unique_map
                                                 ON unique_map.gsma_rank = model_ranks.model_gsma_rank
                                         ) uniques
                                GROUP BY manufacturer, model, total_imei_count
                                ORDER BY total_imei_count DESC
                               """).format(subquery),  # noqa: Q447, Q449
                       subquery_params)
        results = [{'model': result.model,
                    'manufacturer': result.manufacturer,
                    'tech_generations': result.tech_generations,
                    'imei_count': result.total_imei_count} for result in cursor]
    return results, cp.duration, [cp.duration]


def _calc_compliance_data(db_config, month, year, condition_tuples, *, run_id, operator=None):
    """Helper functions to calculate the compliance data for each operator."""
    # We fetch results 100000 at a time from cursors in here
    durations = []
    with utils.create_db_connection(db_config) as conn, utils.CodeProfiler() as cp, conn.cursor() as cursor:
        with utils.CodeProfiler() as scp:
            cursor.execute("""CREATE TEMP TABLE network_triplet_counts(
                                  imei_norm TEXT NOT NULL,
                                  per_imei_imsi_counts BIGINT NOT NULL,
                                  per_imei_msisdn_counts BIGINT NOT NULL,
                                  per_imei_triplet_counts BIGINT NOT NULL
                              )""")

            network_triplets_partition_id = _monthly_network_triplets_partition(conn=conn, month=month, year=year,
                                                                                operator=operator)
            cursor.execute(sql.SQL("""INSERT INTO network_triplet_counts(imei_norm,
                                                                         per_imei_imsi_counts,
                                                                         per_imei_msisdn_counts,
                                                                         per_imei_triplet_counts)
                                           SELECT imei_norm,
                                                  COUNT(DISTINCT imsi),
                                                  COUNT(DISTINCT msisdn),
                                                  COUNT(DISTINCT triplet_hash)
                                                        filter(WHERE imei_norm IS NOT NULL
                                                                 AND imsi IS NOT NULL
                                                                 AND msisdn IS NOT NULL)
                                             FROM {0}
                                            WHERE imei_norm IS NOT NULL
                                         GROUP BY imei_norm""").format(network_triplets_partition_id))  # noqa: Q447
            cursor.execute('CREATE UNIQUE INDEX ON network_triplet_counts(imei_norm)')
            cursor.execute('ANALYZE network_triplet_counts')
        durations.append(scp.duration)

        start_date, end_date = _calc_date_range(month, year)
        with utils.CodeProfiler() as scp:
            cursor.execute('CREATE TEMP TABLE network_gross_adds(imei_norm TEXT NOT NULL)')
            cursor.execute(sql.SQL("""INSERT INTO network_gross_adds(imei_norm)
                                           SELECT imei_norm
                                             FROM network_imeis
                                            WHERE network_imeis.first_seen >= %s
                                              AND network_imeis.first_seen < %s
                                              AND EXISTS (SELECT 1
                                                            FROM {network_triplets_partition_id}
                                                           WHERE imei_norm = network_imeis.imei_norm)
                                   """).format(network_triplets_partition_id=network_triplets_partition_id),
                           [start_date, end_date])
            cursor.execute('CREATE UNIQUE INDEX ON network_gross_adds(imei_norm)')
            cursor.execute('ANALYZE network_gross_adds')
        durations.append(scp.duration)

        # Generate the per-TAC compliance data
        per_tac_results, results, per_condition_counts, table_durations = \
            _generate_compliance_data_table(conn, year, month, operator, condition_tuples, run_id=run_id)
        durations.extend(table_durations)

        # Generate compliance breakdown
        with utils.CodeProfiler() as scp:
            compliance_breakdown = _generate_compliance_breakdown(per_tac_results)
        durations.append(scp.duration)

    return per_tac_results, results, per_condition_counts, compliance_breakdown, cp.duration, durations


def _generate_compliance_data_table(conn, year, month, operator, condition_tuples, *, run_id):
    """Generates per-TAC compliance data in table form."""
    # Get full list of matching IMEIs for each condition at end of period
    per_condition_counts = defaultdict(dict)
    network_triplets_partition_id = _monthly_network_triplets_partition(conn=conn, month=month, year=year,
                                                                        operator=operator)
    durations = []
    # Populate per-condition count defaults -- make sure we have an entry for conditions even if we have no matching
    # rows
    for c in condition_tuples:
        per_condition_counts[c.label] = dict(num_imeis=0, num_imei_gross_adds=0,
                                             num_triplets=0, num_imei_imsis=0, num_imei_msisdns=0)

    with conn.cursor() as cursor:
        # Create temp tables
        cursor.execute('CREATE TEMP TABLE matching_imeis(imei_norm TEXT NOT NULL, cond_name TEXT NOT NULL)')
        cursor.execute('CREATE TEMP TABLE matching_gross_adds(imei_norm TEXT NOT NULL, cond_name TEXT NOT NULL)')
        cursor.execute("""CREATE TEMP TABLE condition_info(cond_name TEXT NOT NULL,
                                                           cond_order INTEGER NOT NULL,
                                                           is_blocking BOOLEAN NOT NULL)""")

        # Popoulate condition order
        with utils.CodeProfiler() as scp:
            execute_values(cursor, 'INSERT INTO condition_info VALUES %s',
                           [(c.label, idx, c.blocking) for idx, c in enumerate(condition_tuples, start=1)])
            cursor.execute('CREATE UNIQUE INDEX ON condition_info(cond_name)')
            cursor.execute('ANALYZE condition_info')
        durations.append(scp.duration)

        # Populate matching imeis table
        with utils.CodeProfiler() as scp:
            cursor.execute(sql.SQL("""INSERT INTO matching_imeis
                                           SELECT imei_norm, cond_name
                                             FROM classification_state
                                            WHERE end_date IS NULL
                                              AND cond_name IN %s
                                              AND EXISTS (SELECT 1
                                                            FROM {network_triplets_partition_id}
                                                           WHERE imei_norm = classification_state.imei_norm)""")
                           .format(network_triplets_partition_id=network_triplets_partition_id),
                           [tuple([x.label for x in condition_tuples])])
            cursor.execute('CREATE INDEX ON matching_imeis(imei_norm)')
            cursor.execute('ANALYZE matching_imeis')
        durations.append(scp.duration)

        # Populate matching_gross_adds table
        with utils.CodeProfiler() as scp:
            cursor.execute("""INSERT INTO matching_gross_adds
                                   SELECT *
                                     FROM matching_imeis
                                    WHERE EXISTS (SELECT 1
                                                    FROM network_gross_adds
                                                   WHERE imei_norm = matching_imeis.imei_norm)""")
            cursor.execute('ANALYZE matching_gross_adds')
        durations.append(scp.duration)

        # Calculate counts per condition
        with utils.CodeProfiler() as scp:
            cursor.execute("""SELECT cond_name,
                                     COUNT(*) AS imei_count,
                                     COALESCE(SUM(per_imei_triplet_counts), 0) AS sum_per_imei_triplet_counts,
                                     COALESCE(SUM(per_imei_imsi_counts), 0) AS sum_per_imei_imsi_counts,
                                     COALESCE(SUM(per_imei_msisdn_counts), 0) AS sum_per_imei_msisdn_counts
                                FROM matching_imeis
                                JOIN network_triplet_counts
                               USING (imei_norm)
                            GROUP BY cond_name""")

            for res in cursor:
                per_condition_counts[res.cond_name]['num_imeis'] = res.imei_count
                per_condition_counts[res.cond_name]['num_triplets'] = res.sum_per_imei_triplet_counts
                per_condition_counts[res.cond_name]['num_imei_imsis'] = res.sum_per_imei_imsi_counts
                per_condition_counts[res.cond_name]['num_imei_msisdns'] = res.sum_per_imei_msisdn_counts
        durations.append(scp.duration)

        with utils.CodeProfiler() as scp:
            cursor.execute("""SELECT cond_name,
                                     COUNT(*) AS imei_count
                                FROM matching_gross_adds
                            GROUP BY cond_name""")

            for res in cursor:
                per_condition_counts[res.cond_name]['num_imei_gross_adds'] = res.imei_count
        durations.append(scp.duration)

        # Now generate an IMEI-level table that contains which conditions that IMEI as a array of booleans.
        # The first element in the array corresponds to the first element in condition_tuples and indicates
        # whether that IMEI meets that condition or not. All IMEIs are included in this so a fully-compliant
        # IMEI will also be in this table and all array elements will be False
        cursor.execute("""CREATE TEMP TABLE per_imei_compliance(imei_norm TEXT NOT NULL,
                                                                condition_status BOOLEAN[] NOT NULL,
                                                                is_gross_add BOOLEAN NOT NULL,
                                                                per_imei_imsi_counts BIGINT NOT NULL,
                                                                per_imei_msisdn_counts BIGINT NOT NULL,
                                                                per_imei_triplet_counts BIGINT NOT NULL,
                                                                meets_blocking BOOLEAN)""")
        with utils.CodeProfiler() as scp:
            cursor.execute("""INSERT INTO per_imei_compliance
                                   SELECT imei_norm,
                                          generate_imei_condition_status(met_condition_indices, %(num_conditions)s),
                                          ga_cond.exists,
                                          per_imei_imsi_counts,
                                          per_imei_msisdn_counts,
                                          per_imei_triplet_counts,
                                          meets_blocking
                                     FROM network_triplet_counts
                                LEFT JOIN (SELECT imei_norm,
                                                  array_agg(cond_order) AS met_condition_indices,
                                                  bool_or(is_blocking) AS meets_blocking
                                             FROM matching_imeis
                                             JOIN condition_info
                                            USING (cond_name)
                                         GROUP BY imei_norm) imeis_cond
                                    USING (imei_norm)
                                     JOIN LATERAL (SELECT EXISTS(SELECT 1
                                                                   FROM network_gross_adds
                                                                  WHERE imei_norm = network_triplet_counts.imei_norm)
                                                  ) ga_cond
                                          ON TRUE
                                    """,  # noqa: Q447, Q449
                           {'num_conditions': len(condition_tuples)})
            cursor.execute('ANALYZE per_imei_compliance')
        durations.append(scp.duration)

        # Now roll our IMEI level table up to the TAC level
        cursor.execute("""CREATE TEMP TABLE per_tac_compliance(tac TEXT NOT NULL,
                                                               condition_status BOOLEAN[] NOT NULL,
                                                               compliance_level SMALLINT NOT NULL,
                                                               num_imeis BIGINT NOT NULL,
                                                               num_imei_gross_adds BIGINT NOT NULL,
                                                               num_imei_imsis BIGINT NOT NULL,
                                                               num_imei_msisdns BIGINT NOT NULL,
                                                               num_subscriber_triplets BIGINT NOT NULL)""")
        with utils.CodeProfiler() as scp:
            cursor.execute("""INSERT INTO per_tac_compliance
                                   SELECT LEFT(imei_norm, 8) AS tac,
                                          condition_status,
                                          CASE FIRST(meets_blocking) WHEN TRUE THEN 0
                                                                     WHEN FALSE THEN 1
                                                                     ELSE 2 END AS compliance_level,
                                          COUNT(*) AS num_imeis,
                                          COUNT(*) filter (WHERE is_gross_add = TRUE) AS num_imei_gross_adds,
                                          SUM(per_imei_imsi_counts) AS num_imei_imsis,
                                          SUM(per_imei_msisdn_counts) AS num_imei_msisdns,
                                          SUM(per_imei_triplet_counts) AS num_triplets
                                     FROM per_imei_compliance
                                 GROUP BY LEFT(imei_norm, 8), condition_status""")  # noqa: Q447
            cursor.execute('ANALYZE per_tac_compliance')
        durations.append(scp.duration)

    # Create a data structure that is two-level map. The first level is indexed by
    # TAC. The next level is indexed by the tuple of condition flags
    per_tac_results = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    results = defaultdict(lambda: defaultdict(int))

    # Walk through per-TAC results
    with conn.cursor(name='per_tac_counts') as cursor, utils.CodeProfiler() as scp:
        cursor.execute('SELECT * FROM per_tac_compliance')
        for res in cursor:
            # Pop off the tac and condition_status, which are used as keys rather than values in the results
            res_dict = res._asdict()
            tac = res_dict.pop('tac')
            condition_flags = res_dict.pop('condition_status')
            compliance_level = res_dict.pop('compliance_level')
            per_tac_results[tac][tuple(condition_flags)]['compliance_level'] = compliance_level
            results[tuple(condition_flags)]['compliance_level'] = compliance_level

            # At this point, everything is something that should be stored in the results dictionaries
            for k, v in res_dict.items():
                per_tac_results[tac][tuple(condition_flags)][k] = v
                results[tuple(condition_flags)][k] += v
    durations.append(scp.duration)

    return _defaultdict_to_regular(per_tac_results), \
        _defaultdict_to_regular(results), \
        _defaultdict_to_regular(per_condition_counts), \
        durations


def _generate_compliance_breakdown(per_tac_results):
    """Generate aggregated total compliance breakdown, as used by the visual report."""
    results = {
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

    for tac, combinations in per_tac_results.items():
        for combination, data in combinations.items():
            if data['compliance_level'] == 0:
                results['num_noncompliant_imeis_blocking'] += data['num_imeis']
                results['num_noncompliant_imeis'] += data['num_imeis']
                results['num_noncompliant_triplets_blocking'] += data['num_subscriber_triplets']
                results['num_noncompliant_triplets'] += data['num_subscriber_triplets']
                results['num_noncompliant_imei_imsis_blocking'] += data['num_imei_imsis']
                results['num_noncompliant_imei_imsis'] += data['num_imei_imsis']
                results['num_noncompliant_imei_msisdns_blocking'] += data['num_imei_msisdns']
                results['num_noncompliant_imei_msisdns'] += data['num_imei_msisdns']
            elif data['compliance_level'] == 1:
                results['num_noncompliant_imeis_info_only'] += data['num_imeis']
                results['num_noncompliant_imeis'] += data['num_imeis']
                results['num_noncompliant_triplets_info_only'] += data['num_subscriber_triplets']
                results['num_noncompliant_triplets'] += data['num_subscriber_triplets']
                results['num_noncompliant_imei_imsis_info_only'] += data['num_imei_imsis']
                results['num_noncompliant_imei_imsis'] += data['num_imei_imsis']
                results['num_noncompliant_imei_msisdns_info_only'] += data['num_imei_msisdns']
                results['num_noncompliant_imei_imsis'] += data['num_imei_msisdns']
            else:
                results['num_compliant_imeis'] += data['num_imeis']
                results['num_compliant_triplets'] += data['num_subscriber_triplets']
                results['num_compliant_imei_imsis'] += data['num_imei_imsis']
                results['num_compliant_imei_msisdns'] += data['num_imei_msisdns']

    return results


def _defaultdict_to_regular(d):
    """Helper function to turn a defaultdict into a regular dict."""
    if isinstance(d, defaultdict):
        d = {k: _defaultdict_to_regular(v) for k, v in d.items()}
    return d


def _calc_imei_imsi_overloading(db_config, month, year, operator=None):
    """Helper function to determine IMEI-IMSI overloading for a year, month and operator."""
    with utils.create_db_connection(db_config) as conn, conn.cursor() as cursor, utils.CodeProfiler() as cp:
        network_triplets_partition = _monthly_network_triplets_partition(conn=conn, operator=operator,
                                                                         month=month, year=year)
        cursor.execute(
            sql.SQL("""SELECT COUNT(*) AS num_imeis,
                              seen_with_imsis
                         FROM (SELECT COUNT(DISTINCT imsi) AS seen_with_imsis
                                 FROM {0}
                             GROUP BY imei_norm) imsis_per_imei
                     GROUP BY seen_with_imsis""").format(network_triplets_partition)
        )
        results = [{'num_imeis': r.num_imeis, 'seen_with_imsis': r.seen_with_imsis} for r in cursor]
    return results, cp.duration, [cp.duration]


def _calc_daily_imei_imsi_overloading(db_config, month, year, operator=None, bin_width=0.1, _min_seen_days=5):
    """Helper function to determine average IMEI-IMSI overloading for a year, month and operator."""
    with utils.create_db_connection(db_config) as conn, conn.cursor() as cursor, utils.CodeProfiler() as cp:
        network_triplets_partition = _monthly_network_triplets_partition(conn=conn, operator=operator,
                                                                         month=month, year=year)
        cursor.execute(
            sql.SQL("""SELECT COUNT(*) AS num_imeis,
                              (bin_id * %(bin_width)s)::REAL AS bin_start,
                              ((bin_id + 1) * %(bin_width)s)::REAL AS bin_end
                         FROM (SELECT imei_norm,
                                      FLOOR(SUM(bitcount(combined_date_bitmask))::NUMERIC/
                                      bitcount(bit_or(combined_date_bitmask))/%(bin_width)s)::INT AS bin_id
                                 FROM (SELECT imei_norm,
                                              imsi,
                                              bit_or(date_bitmask) AS combined_date_bitmask
                                         FROM {0}
                                        WHERE imei_norm IS NOT NULL
                                          AND is_valid_imsi(imsi)
                                     GROUP BY imei_norm, imsi) all_imei_imsis
                             GROUP BY imei_norm
                                      HAVING bitcount(bit_or(combined_date_bitmask)) >= %(min_seen_days)s ) histogram
                     GROUP BY bin_id""").format(network_triplets_partition),  # noqa: Q447
            {'bin_width': bin_width, 'min_seen_days': _min_seen_days}
        )
        results = [{'num_imeis': r.num_imeis,
                    'bin_start': r.bin_start,
                    'bin_end': r.bin_end} for r in cursor]
    return results, cp.duration, [cp.duration]


def _calc_imsi_imei_overloading(db_config, month, year, operator=None):
    """Helper function to determine IMSI-IMEI overloading for a year, month and operator."""
    with utils.create_db_connection(db_config) as conn, conn.cursor() as cursor, utils.CodeProfiler() as cp:
        network_triplets_partition = _monthly_network_triplets_partition(conn=conn, operator=operator,
                                                                         month=month, year=year)
        cursor.execute(
            sql.SQL("""SELECT COUNT(*) AS num_imsis,
                              seen_with_imeis
                         FROM (SELECT COUNT(DISTINCT imei_norm) AS seen_with_imeis
                                 FROM {0}
                             GROUP BY imsi) imeis_per_imsi
                     GROUP BY seen_with_imeis""").format(network_triplets_partition)
        )
        results = [{'num_imsis': r.num_imsis, 'seen_with_imeis': r.seen_with_imeis} for r in cursor]
    return results, cp.duration, [cp.duration]


def _print_component_query_perfomance(component_durations, debug_query_performance):
    """Helper function to print out a log message showing the component durations of a task."""
    logger = logging.getLogger('dirbs.report')
    if debug_query_performance:
        durations_secs = ['{0:.3f}s'.format(d / 1000) for d in component_durations]
        logger.info('Component durations: {0}'.format(', '.join(durations_secs)))


def _process_id_counts_future(per_operator_record_counts, statsd, metrics_run_root, monthly_stats, daily_stats,
                              debug_query_performance, f):
    """Function to process the result of the distinct ID counts future."""
    logger = logging.getLogger('dirbs.report')
    results, total_duration, component_durations = f.result()
    logger.info('Calculated distinct identifier counts for all operators (duration {0:.3f}s)'
                .format(total_duration / 1000))
    _print_component_query_perfomance(component_durations, debug_query_performance)
    # Future returns a row of dicts, so process each results
    for result in results:
        # If the operator_id is None, this row is a rollup to the country level
        operator_id = result.pop('operator_id')
        if operator_id is None:
            operator_id = OperatorConfig.COUNTRY_OPERATOR_NAME

        # If the data_date is none, this row is a rollup to the monthly level, otherwise date
        data_date = result.pop('data_date')
        if data_date is None:
            # At this point, all remaining non-popped values should be stored in the states
            monthly_stats[operator_id].update(result)
        else:
            dom = data_date.day
            daily_stats[operator_id][dom - 1].update(result)

    _log_perf_metric(statsd, metrics_run_root, 'identifier_counts', total_duration,
                     record_counts_map=per_operator_record_counts)


def _process_monthly_future(type_string, per_operator_record_counts, statsd, metrics_run_root, monthly_stats,
                            debug_query_performance, f):
    """Helper function to process a monthly stat future and populate the results data structure."""
    logger = logging.getLogger('dirbs.report')
    results, total_duration, component_durations = f.result()
    logger.info('Calculated monthly {0} counts for all operators (duration {1:.3f}s)'
                .format(type_string, total_duration / 1000))
    _print_component_query_perfomance(component_durations, debug_query_performance)
    for operator, monthly_counts in results.items():
        monthly_stats[operator].update(monthly_counts)
    _log_perf_metric(statsd, metrics_run_root, type_string, total_duration,
                     record_counts_map=per_operator_record_counts)


def _process_per_operator_monthly_future(operator, type_string, statsd,
                                         metrics_run_root, monthly_stats, debug_query_performance, f,
                                         per_operator_record_counts=None):
    """Helper function to process a monthly stat future and populate the results data structure."""
    logger = logging.getLogger('dirbs.report')
    results, total_duration, component_durations = f.result()
    logger.info('Calculated {0} for operator {1} (duration {2:.3f}s)'
                .format(type_string, operator, total_duration / 1000))
    _print_component_query_perfomance(component_durations, debug_query_performance)
    monthly_stats[operator] = results
    _log_perf_metric(statsd, metrics_run_root, type_string, total_duration,
                     operator_id=operator, record_counts_map=per_operator_record_counts)


def _process_per_operator_compliance_future(operator, condition_counts, per_operator_record_counts, statsd,
                                            metrics_run_root, tac_compliance_data, compliance_data, monthly_stats,
                                            debug_query_performance, f):
    """Helper function to process a compliance future and populate the results data structures."""
    logger = logging.getLogger('dirbs.report')
    per_tac_results, results, per_condition_counts, compliance_breakdown, \
        total_duration, component_durations = f.result()
    logger.info('Calculated compliance data for operator {0} (duration {1:.3f}s)'
                .format(operator, total_duration / 1000))
    _print_component_query_perfomance(component_durations, debug_query_performance)
    tac_compliance_data[operator] = per_tac_results
    compliance_data[operator] = results
    condition_counts[operator] = per_condition_counts
    monthly_stats[operator].update(compliance_breakdown)
    _log_perf_metric(statsd, metrics_run_root, 'compliance_data', total_duration,
                     operator_id=operator, record_counts_map=per_operator_record_counts)


def _store_report_data(conn,
                       operators,
                       month,
                       year,
                       condition_tuples,
                       per_operator_record_counts,
                       per_operator_daily_stats,
                       per_operator_monthly_stats,
                       per_operator_condition_counts,
                       per_operator_top_model_imei_counts,
                       per_operator_top_model_gross_adds,
                       per_operator_imei_imsi_overloading,
                       per_operator_imsi_imei_overloading,
                       per_operator_compliance_data,
                       per_operator_daily_imei_imsi_overloading,
                       statsd,
                       metrics_run_root):
    """Store the data for the reporting in the DB using the supplied data_id as a key."""
    logger = logging.getLogger('dirbs.report')
    cond_run_info = utils.most_recently_run_condition_info(conn, [c.label for c in condition_tuples])
    successful_cond_run_info = {k: v for k, v in cond_run_info.items() if v is not None}
    if not successful_cond_run_info:
        class_run_id = None
    else:
        class_run_id = max([v['run_id'] for k, v in successful_cond_run_info.items()])
    data_id = _store_report_data_metadata(conn, month, year, class_run_id)
    all_ops = operators + [OperatorConfig.COUNTRY_OPERATOR_NAME]
    cond_configs_map = {k: v['config'] if v is not None else None for k, v in cond_run_info.items()}
    cond_report_date_map = {k: v['last_successful_run'] if v is not None else None for k, v in cond_run_info.items()}
    missing_cond_configs = [k for k, v in cond_run_info.items() if v is None]
    if len(missing_cond_configs) > 0:
        logger.warning('No classification config for the following conditions, meaning that that they have never been'
                       'run successfully: {0}'.format(', '.join(missing_cond_configs)))

    with conn.cursor() as cursor, utils.CodeProfiler() as cp:
        execute_values(cursor,
                       """INSERT INTO report_monthly_conditions(data_id,
                                                                cond_name,
                                                                sort_order,
                                                                was_blocking,
                                                                last_successful_config,
                                                                last_successful_run)
                               VALUES %s""",
                       [(data_id, c.label, idx, c.blocking, json.dumps(cond_configs_map[c.label]),
                        cond_report_date_map[c.label])
                        for idx, c in enumerate(condition_tuples)])

        execute_values(cursor,
                       """INSERT INTO report_daily_stats(data_id,
                                                         num_triplets,
                                                         num_imeis,
                                                         num_imsis,
                                                         num_msisdns,
                                                         data_date,
                                                         operator_id)
                               VALUES %s""",
                       [(data_id,
                         dc['num_triplets'],
                         dc['num_imeis'],
                         dc['num_imsis'],
                         dc['num_msisdns'],
                         datetime.date(year, month, idx + 1), op)
                        for op in all_ops
                        for idx, dc in enumerate(per_operator_daily_stats[op])])

        execute_values(cursor,
                       """INSERT INTO report_monthly_stats(data_id,
                                                           num_triplets,
                                                           num_imeis,
                                                           num_imsis,
                                                           num_msisdns,
                                                           num_gross_adds,
                                                           num_compliant_imeis,
                                                           num_noncompliant_imeis,
                                                           num_noncompliant_imeis_blocking,
                                                           num_noncompliant_imeis_info_only,
                                                           num_compliant_triplets,
                                                           num_noncompliant_triplets,
                                                           num_noncompliant_triplets_blocking,
                                                           num_noncompliant_triplets_info_only,
                                                           operator_id,
                                                           num_records,
                                                           num_null_imei_records,
                                                           num_null_imsi_records,
                                                           num_null_msisdn_records,
                                                           num_invalid_imei_imsis,
                                                           num_invalid_imei_msisdns,
                                                           num_invalid_triplets,
                                                           num_imei_imsis,
                                                           num_imei_msisdns,
                                                           num_imsi_msisdns,
                                                           num_compliant_imei_imsis,
                                                           num_noncompliant_imei_imsis,
                                                           num_noncompliant_imei_imsis_blocking,
                                                           num_noncompliant_imei_imsis_info_only,
                                                           num_compliant_imei_msisdns,
                                                           num_noncompliant_imei_msisdns,
                                                           num_noncompliant_imei_msisdns_blocking,
                                                           num_noncompliant_imei_msisdns_info_only)
                               VALUES %s""",
                       [(data_id,
                         mc['num_triplets'],
                         mc['num_imeis'],
                         mc['num_imsis'],
                         mc['num_msisdns'],
                         mc['num_gross_adds'],
                         mc['num_compliant_imeis'],
                         mc['num_noncompliant_imeis'],
                         mc['num_noncompliant_imeis_blocking'],
                         mc['num_noncompliant_imeis_info_only'],
                         mc['num_compliant_triplets'],
                         mc['num_noncompliant_triplets'],
                         mc['num_noncompliant_triplets_blocking'],
                         mc['num_noncompliant_triplets_info_only'],
                         op,
                         per_operator_record_counts[op],
                         mc['num_null_imei_records'],
                         mc['num_null_imsi_records'],
                         mc['num_null_msisdn_records'],
                         mc['num_invalid_imei_imsis'],
                         mc['num_invalid_imei_msisdns'],
                         mc['num_invalid_triplets'],
                         mc['num_imei_imsis'],
                         mc['num_imei_msisdns'],
                         mc['num_imsi_msisdns'],
                         mc['num_compliant_imei_imsis'],
                         mc['num_noncompliant_imei_imsis'],
                         mc['num_noncompliant_imei_imsis_blocking'],
                         mc['num_noncompliant_imei_imsis_info_only'],
                         mc['num_compliant_imei_msisdns'],
                         mc['num_noncompliant_imei_msisdns'],
                         mc['num_noncompliant_imei_msisdns_blocking'],
                         mc['num_noncompliant_imei_msisdns_info_only'])
                        for op, mc in ((op, per_operator_monthly_stats[op]) for op in all_ops)])

        execute_values(cursor,
                       """INSERT INTO report_monthly_condition_stats(data_id,
                                                                     operator_id,
                                                                     cond_name,
                                                                     num_imeis,
                                                                     num_triplets,
                                                                     num_imei_imsis,
                                                                     num_imei_msisdns,
                                                                     num_imei_gross_adds)
                               VALUES %s""",
                       [(data_id,
                         op,
                         label,
                         counts['num_imeis'],
                         counts['num_triplets'],
                         counts['num_imei_imsis'],
                         counts['num_imei_msisdns'],
                         counts['num_imei_gross_adds'])
                        for op, label, counts in ((op, label, counts)
                                                  for op in all_ops
                                                  for label, counts in per_operator_condition_counts[op].items())])

        execute_values(cursor,
                       """INSERT INTO report_monthly_top_models_imei(data_id,
                                                                     rank_pos,
                                                                     num_imeis,
                                                                     model,
                                                                     manufacturer,
                                                                     tech_generations,
                                                                     operator_id)
                               VALUES %s""",
                       [(data_id,
                         rank + 1,
                         tm['imei_count'],
                         '' if tm['model'] is None else tm['model'],
                         '' if tm['manufacturer'] is None else tm['manufacturer'],
                         tm['tech_generations'],
                         op)
                        for op, rank, tm in ((op, rank, tm)
                                             for op in all_ops
                                             for rank, tm in enumerate(per_operator_top_model_imei_counts[op]))])

        execute_values(cursor,
                       """INSERT INTO report_monthly_top_models_gross_adds(data_id,
                                                                           rank_pos,
                                                                           num_imeis,
                                                                           model,
                                                                           manufacturer,
                                                                           tech_generations,
                                                                           operator_id)
                               VALUES %s""",
                       [(data_id,
                         rank + 1,
                         tm['imei_count'],
                         '' if tm['model'] is None else tm['model'],
                         '' if tm['manufacturer'] is None else tm['manufacturer'],
                         tm['tech_generations'],
                         op)
                        for op, rank, tm in ((op, rank, tm)
                                             for op in all_ops
                                             for rank, tm in enumerate(per_operator_top_model_gross_adds[op]))])

        execute_values(cursor,
                       """INSERT INTO report_monthly_imei_imsi_overloading(data_id,
                                                                           num_imeis,
                                                                           seen_with_imsis,
                                                                           operator_id)
                               VALUES %s""",
                       [(data_id,
                         rec['num_imeis'],
                         rec['seen_with_imsis'],
                         op)
                        for op, rec in ((op, rec)
                                        for op in all_ops
                                        for rec in per_operator_imei_imsi_overloading[op])])

        execute_values(cursor,
                       """INSERT INTO report_monthly_average_imei_imsi_overloading(data_id,
                                                                                   num_imeis,
                                                                                   bin_start,
                                                                                   bin_end,
                                                                                   operator_id)
                               VALUES %s""",
                       [(data_id,
                         rec['num_imeis'],
                         rec['bin_start'],
                         rec['bin_end'],
                         op)
                        for op, rec in ((op, rec)
                                        for op in all_ops
                                        for rec in per_operator_daily_imei_imsi_overloading[op])])

        execute_values(cursor,
                       """INSERT INTO report_monthly_imsi_imei_overloading(data_id,
                                                                           num_imsis,
                                                                           seen_with_imeis,
                                                                           operator_id)
                               VALUES %s""",
                       [(data_id,
                         rec['num_imsis'],
                         rec['seen_with_imeis'],
                         op)
                        for op, rec in ((op, rec)
                                        for op in all_ops
                                        for rec in per_operator_imsi_imei_overloading[op])])

        execute_values(cursor,
                       """INSERT INTO report_monthly_condition_stats_combinations(data_id,
                                                                                  combination,
                                                                                  num_imeis,
                                                                                  num_imei_gross_adds,
                                                                                  num_imei_imsis,
                                                                                  num_imei_msisdns,
                                                                                  num_subscriber_triplets,
                                                                                  compliance_level,
                                                                                  operator_id)
                               VALUES %s""",
                       [(data_id,
                         list(combination),
                         counts['num_imeis'],
                         counts['num_imei_gross_adds'],
                         counts['num_imei_imsis'],
                         counts['num_imei_msisdns'],
                         counts['num_subscriber_triplets'],
                         counts['compliance_level'],
                         op)
                        for op, combination, counts in ((op, combination, counts)
                                                        for op in all_ops
                                                        for combination, counts in
                                                        per_operator_compliance_data[op].items())])

    # Store performance datapoint for writing to DB
    statsd.gauge('{0}runtime.store_report_data'.format(metrics_run_root), cp.duration)

    return data_id, class_run_id
