"""
DIRBS CLI for IMEI classification. Installed by setuptools as a dirbs-classify console script.

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

import copy
import sys
from concurrent import futures
from collections import defaultdict

import click
from psycopg2 import sql

from dirbs.utils import hash_string_64bit
import dirbs.cli.common as common
from dirbs.condition import Condition
import dirbs.metadata as metadata


class ClassifyLockException(Exception):
    """Indicates that we couldn't acquire the lock for classification for this run."""

    pass


class ClassifySanityCheckFailedException(Exception):
    """Indicates that the sanity checks failed for classification."""

    pass


@click.command()
@common.setup_initial_logging
@click.option('--conditions',
              help='By default, dirbs-classify classifies on all conditions. Specify a comma-separated list '
                   'of condition names if you wish to classify only on those conditions. The condition name '
                   'corresponds to the label parameter of the condition in the DIRBS configuration file.',
              callback=common.validate_conditions,
              default=None)
@click.option('--safety-check/--no-safety-check',
              help='DANGEROUS: Disables safety check that ensures that no more than a certain ratio of IMEIs '
                   'will be classified.',
              is_flag=True,
              default=True)
@click.option('--curr-date',
              help='DANGEROUS: Sets current date in YYYYMMDD format for testing. By default, '
                   'uses system current date.',
              callback=common.validate_date)
@click.option('--disable-sanity-checks', is_flag=True,
              help='If set sanity checks on classification will be disabled')
@click.version_option()
@common.parse_verbosity_option
@common.parse_db_options
@common.parse_statsd_options
@common.parse_multiprocessing_options
@click.pass_context
@common.unhandled_exception_handler
@common.configure_logging
@common.cli_wrapper(command='dirbs-classify', required_role='dirbs_core_classify')
def cli(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root,
        conditions, safety_check, curr_date, disable_sanity_checks):
    """
    DIRBS script to classify IMEIs.

    Iterates through all configured conditions and write to the classification_state table.
    """
    _warn_about_curr_date(curr_date, logger)
    _warn_about_disabled_safety_check(safety_check, logger)

    # If we didn't specify a condition, use all configured conditions
    if conditions is None:
        conditions = config.conditions

    # Query the job metadata table for all successful classification runs
    successful_job_runs = metadata.query_for_command_runs(metadata_conn, 'dirbs-classify', successful_only=True)
    if successful_job_runs and not disable_sanity_checks and not _perform_sanity_checks(
            config, successful_job_runs[0].extra_metadata):
        raise ClassifySanityCheckFailedException(
            'Sanity checks failed, configurations are not identical to the last successful classification')

    logger.info('Classifying using conditions: {0}'.format(','.join([c.label for c in conditions])))

    # Store metadata
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       curr_date=curr_date.isoformat() if curr_date is not None else None,
                                       conditions=[c.as_dict() for c in conditions],
                                       operators=[op.as_dict() for op in config.region_config.operators],
                                       amnesty=config.amnesty_config.as_dict())

    # Per-condition intermediate tables
    intermediate_tables = []

    # Flag indicating whether we had a failure to change exit code
    had_errored_condition = False

    try:
        locked = False
        with conn, conn.cursor() as cursor:
            # Lock to prevent multiple simultaneous classifications
            cursor.execute('SELECT pg_try_advisory_lock(%s::BIGINT)', [hash_string_64bit('dirbs-classify')])
            locked = cursor.fetchone()[0]
            if not locked:
                raise ClassifyLockException('Could not acquire lock for classification. '
                                            'Are there any other dirbs-classify instances running at the moment?')

            # Calculate total IMEI count
            if safety_check:
                logger.info('Counting number of IMEIs in network_imeis for safety check...')
                cursor.execute('SELECT COUNT(*) FROM network_imeis')
                total_imei_count = cursor.fetchone()[0]
                logger.info('Finished counting number of IMEIs in network_imeis for safety check')
            else:
                total_imei_count = -1

        matched_imei_counts = {}
        nworkers = config.multiprocessing_config.max_db_connections
        condition_objs = [Condition(cond_config) for cond_config in conditions]

        with futures.ProcessPoolExecutor(max_workers=nworkers) as executor:
            logger.info('Simultaneously classifying {0:d} dimensions using up to {1:d} workers...'
                        .format(len(conditions), nworkers))

            calc_futures_to_condition = {}
            update_futures_to_condition = {}
            per_condition_state = defaultdict(lambda: dict(num_completed_calc_jobs=0, num_total_calc_jobs=0,
                                                           num_completed_update_jobs=0, num_total_update_jobs=0,
                                                           num_matched_imeis=0))
            for c in condition_objs:
                # Make sure we record all temporary tables so that we can cleanup later
                intermediate_tables.append(c.intermediate_tbl_name(run_id))
                # Queue the condition calculations and keep track
                for f in c.queue_calc_imeis_jobs(executor, config, run_id, curr_date):
                    calc_futures_to_condition[f] = c
                    per_condition_state[c.label]['num_total_calc_jobs'] += 1

            # Process calculation futures
            for condition, job_state in _completed_calc_jobs(calc_futures_to_condition, per_condition_state, logger):
                max_ratio = condition.config.max_allowed_matching_ratio
                num_matched_imeis = job_state['num_matched_imeis']
                max_matched_imeis = max_ratio * total_imei_count
                if safety_check and total_imei_count > 0 and num_matched_imeis > max_matched_imeis:
                    ratio = min(num_matched_imeis / total_imei_count, 1)
                    logger.error("Refusing to classify using condition \'{0}\': "
                                 'This condition matches more than the maximum number of IMEIs allowed by the '
                                 "condition\'s configuration "
                                 '(matched_imeis={1:d}, ratio={2:f}, max_ratio={3:f})'
                                 .format(condition.label, num_matched_imeis, ratio, max_ratio))
                    had_errored_condition = True
                else:
                    # Queue the classification state updates and keep track
                    for f in condition.queue_update_classification_state_jobs(executor, config, run_id, curr_date):
                        update_futures_to_condition[f] = condition
                        per_condition_state[condition.label]['num_total_update_jobs'] += 1

            # Process update futures
            for condition, job_state in _completed_update_jobs(update_futures_to_condition,
                                                               per_condition_state,
                                                               logger):
                # Update metadata about matched IMEI counts every time each condition finishes
                matched_imei_counts[condition.label] = job_state['num_matched_imeis']
                metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                                   matched_imei_counts=matched_imei_counts)
                # Output StatsD stats
                statsd.gauge('{0}matched_imeis.{1}'.format(metrics_run_root, condition.label.lower()),
                             job_state['num_matched_imeis'])

    finally:
        _do_final_cleanup(conn, logger, locked, intermediate_tables)

        # If we had an error condition, generate an error return code on exit
        if had_errored_condition:
            sys.exit(1)


def _warn_about_curr_date(curr_date, logger):
    """
    Function to print out warning about setting curr_date in production.

    :param curr_date: user supplied current date
    :param logger: dirbs logger instance
    """
    if curr_date is not None:
        logger.warning('*************************************************************************')
        logger.warning('WARNING: --curr-date option passed to dirbs-classify')
        logger.warning('*************************************************************************')
        logger.warning('')
        logger.warning('This should not be done in a production DIRBS deployment for the following reasons:')
        logger.warning('')
        logger.warning('1. If an IMEI is classified by a condition for the first time by this invocation of')
        logger.warning('   dirbs-classify, a block date will be calculated and stored in the classification_state')
        logger.warning('   table based on the current date and the grace period for condition. This is stored and')
        logger.warning('   will remain the block date for that IMEI even if dirbs-classify is run again without')
        logger.warning('   --curr-date set. This is by design so that changes to conditions and grace periods')
        logger.warning('   do not affect the block date previously communicated to a subscriber.')
        logger.warning('2. Classifying based on old data can have effects on reporting numbers, where DIRBS says that')
        logger.warning('   a certain number of IMEIs met a condition on a certain date, but on that particular run')
        logger.warning('   the data being analyzed was not current data.')
        logger.warning('')


def _warn_about_disabled_safety_check(safety_check, logger):
    """Function to print out warning about disabling safety check in production.

    :param safety_check: safety check param
    :param logger: dirbs logger instance
    """
    if not safety_check:
        logger.warning('*************************************************************************')
        logger.warning('WARNING: --no-safety-check option passed to dirbs-classify')
        logger.warning('*************************************************************************')
        logger.warning('')
        logger.warning('This should not be done in a production DIRBS deployment for the following reasons:')
        logger.warning('')
        logger.warning('1. The safety check is in place to prevent a misconfigured condition from classifying')
        logger.warning('   a large proportion of the subscriber population. In the worst case, a list could')
        logger.warning('   then be generated and a large number of subscribers would be notified or blacklisted.')
        logger.warning('   Even in the best case where the error is found before list generation, this generates')
        logger.warning('   bloat in the classification_state table that must be pruned to avoid a performance impact')
        logger.warning('   in other parts of DIRBS Core.')
        logger.warning('')


def _completed_calc_jobs(futures_to_condition, per_condition_state, logger):
    """
    Function to process the IMEI calculation jobs and yield results once a condition is completed.

    :param futures_to_condition: list of condition futures
    :param per_condition_state: list of condition states
    :param logger: dirbs logger instance
    """
    for f in futures.as_completed(futures_to_condition):
        num_matched_imeis, duration = f.result()
        condition = futures_to_condition[f]
        state = per_condition_state[condition.label]
        state['num_completed_calc_jobs'] += 1
        state['num_matched_imeis'] += num_matched_imeis
        logger.debug("Processed {0:d} of {1:d} jobs to calculate matching IMEIs for condition \'{2}\' "
                     '(duration {3:.3f}s)'
                     .format(state['num_completed_calc_jobs'],
                             state['num_total_calc_jobs'],
                             condition.label,
                             duration / 1000))

        if state['num_completed_calc_jobs'] == state['num_total_calc_jobs']:
            logger.info("Finished calculating {0:d} matching IMEIs for condition \'{1}\'"
                        .format(state['num_matched_imeis'], condition.label))
            yield condition, state


def _completed_update_jobs(futures_to_condition, per_condition_state, logger):
    """
    Function to process the classification_state update jobs and yield results once a condition is completed.

    :param futures_to_condition: list of condition futures
    :param per_condition_state: per condition state
    :param logger: dirbs logger instance
    """
    for f in futures.as_completed(futures_to_condition):
        duration = f.result()
        condition = futures_to_condition[f]
        state = per_condition_state[condition.label]
        state['num_completed_update_jobs'] += 1

        logger.debug("Processed {0:d} of {1:d} jobs to update classification_state table for condition \'{2}\'"
                     ' (duration {3:.3f}s)'
                     .format(state['num_completed_update_jobs'],
                             state['num_total_update_jobs'],
                             condition.label,
                             duration / 1000))

        if state['num_completed_update_jobs'] == state['num_total_update_jobs']:
            logger.info("Finished updating classification_state table for condition \'{0}\'"
                        .format(condition.label))
            yield condition, state


def _perform_sanity_checks(config, extra_metadata):
    """
    Method to perform sanity checks on current classification run.

    :param config: dirbs config instance
    :param extra_metadata: job extra metadata dict obj
    :return: bool (true/false)
    """
    curr_conditions = [c.as_dict() for c in config.conditions]
    curr_operators = [op.as_dict() for op in config.region_config.operators]
    curr_amnesty = config.amnesty_config.as_dict()

    if curr_conditions == extra_metadata['conditions'] and \
            curr_operators == extra_metadata['operators'] and \
            curr_amnesty == extra_metadata['amnesty']:
        return True
    return False


def _do_final_cleanup(conn, logger, is_locked, tables_to_delete):
    """
    Function to perform final cleanup to remove intermediate tables and release locks.

    :param conn: database connection obj
    :param logger: dirbs logger obj
    :param is_locked: bool (to check if there is postgres advisory lock)
    :param tables_to_delete: list of tables to delete
    """
    if is_locked:
        with conn.cursor() as cursor:
            cursor.execute('SELECT pg_advisory_unlock(%s::BIGINT)', [hash_string_64bit('dirbs-classify')])

    with conn.cursor() as cursor:
        remaining_tables_to_delete = copy.copy(tables_to_delete)
        for t in tables_to_delete:
            try:
                cursor.execute(sql.SQL('DROP TABLE IF EXISTS {0} CASCADE').format(sql.Identifier(t)))
                conn.commit()
                remaining_tables_to_delete.remove(t)
            except:  # noqa: E722
                for t_not_deleted in remaining_tables_to_delete:
                    logger.warning('Failed to drop table {0} due to exception. Please issue '
                                   "\'DROP TABLE IF EXISTS {0}\' manually!".format(t_not_deleted))
                raise
