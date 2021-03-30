"""
DIRBS CLI for pruning old monthly_network_triplets data or obsolete classification_state data.

Installed by setuptools as a dirbs-prune console script.
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

from dateutil import relativedelta
from psycopg2 import sql
import click

import dirbs.cli.common as common
import dirbs.metadata as metadata
import dirbs.utils as utils
import dirbs.partition_utils as partition_utils


@click.group(no_args_is_help=False)
@common.setup_initial_logging
@click.version_option()
@common.parse_verbosity_option
@common.parse_db_options
@common.parse_statsd_options
@click.option('--curr-date',
              help='Sets current date in YYYYMMDD format for testing. By default, uses system current date.',
              callback=common.validate_date,
              default=None)
@click.pass_context
@common.configure_logging
def cli(ctx, curr_date):
    """DIRBS script to prune obsolete data from the DIRBS Core PostgreSQL database."""
    ctx.obj['CURR_DATE'] = curr_date


@cli.command()
@click.pass_context
@common.unhandled_exception_handler
@common.cli_wrapper(command='dirbs-prune', subcommand='triplets', required_role='dirbs_core_power_user')
def triplets(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root):
    """Prune old monthly_network_triplets data."""
    curr_date = ctx.obj['CURR_DATE']

    # Store metadata
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       curr_date=curr_date.isoformat() if curr_date is not None else None,
                                       retention_months=config.retention_config.months_retention)

    if curr_date is None:
        curr_date = datetime.date.today()

    with conn.cursor() as cursor:
        logger.info('Pruning monthly_network_triplets data outside the retention window from database...')
        retention_months = config.retention_config.months_retention
        first_month_to_drop = datetime.date(curr_date.year, curr_date.month, 1) - relativedelta.relativedelta(
            months=retention_months)
        logger.info('monthly_network_triplets partitions older than {0} will be pruned'
                    .format(first_month_to_drop))

        country_monthly_partitions = utils.child_table_names(conn, 'monthly_network_triplets_country')
        operator_partitions = utils.child_table_names(conn, 'monthly_network_triplets_per_mno')
        operator_monthly_partitions = []
        for op_partition in operator_partitions:
            operator_monthly_partitions.extend(utils.child_table_names(conn, op_partition))

        parent_tbl_names = ['monthly_network_triplets_country', 'monthly_network_triplets_per_mno']
        rows_before = {}
        for tbl in parent_tbl_names:
            logger.debug('Calculating original number of rows in {0} table...'.format(tbl))
            cursor.execute(sql.SQL('SELECT COUNT(*) FROM {0}'.format(tbl)))
            rows_before[tbl] = cursor.fetchone()[0]
            logger.debug('Calculated original number of rows in {0} table'.format(tbl))
            statsd.gauge('{0}.{1}.rows_before'.format(metrics_run_root, tbl), rows_before[tbl])
        metadata.add_optional_job_metadata(metadata_conn, command, run_id, rows_before=rows_before)

        total_rows_pruned = 0
        total_partitions = country_monthly_partitions + operator_monthly_partitions
        for tblname in total_partitions:
            invariants_list = utils.table_invariants_list(conn, [tblname], ['triplet_month', 'triplet_year'])
            assert len(invariants_list) <= 1
            if len(invariants_list) == 0:
                logger.warning('Found empty partition {0}. Dropping...'.format(tblname))
                cursor.execute(sql.SQL("""DROP TABLE {0} CASCADE""").format(sql.Identifier(tblname)))
            else:
                month, year = tuple(invariants_list[0])

                # Check if table year/month is outside the retention window
                if (datetime.date(year, month, 1) < first_month_to_drop):
                    # Calculate number of rows in the partition table
                    cursor.execute(sql.SQL("""SELECT COUNT(*) FROM {0}""").format(sql.Identifier(tblname)))
                    partition_table_rows = cursor.fetchone()[0]
                    total_rows_pruned += partition_table_rows

                    logger.info('Dropping table {0} with {1} rows...'.format(tblname, partition_table_rows))
                    cursor.execute(sql.SQL("""DROP TABLE {0} CASCADE""").format(sql.Identifier(tblname)))
                    logger.info('Dropped table {0}'.format(tblname))

        rows_after = {}
        for tbl in parent_tbl_names:
            logger.debug('Calculating new number of rows in {0} table...'.format(tbl))
            cursor.execute(sql.SQL('SELECT COUNT(*) FROM {0}'.format(tbl)))
            rows_after[tbl] = cursor.fetchone()[0]
            logger.debug('Calculated new number of rows in {0} table'.format(tbl))
            statsd.gauge('{0}.{1}.rows_after'.format(metrics_run_root, tbl), rows_after[tbl])
        metadata.add_optional_job_metadata(metadata_conn, command, run_id, rows_after=rows_after)

        total_rows_before = sum(rows_before.values())
        total_rows_after = sum(rows_after.values())

        assert (total_rows_before - total_rows_after) == total_rows_pruned
        logger.info('Pruned {0:d} rows of monthly_network_triplets data outside the retention window from database'
                    .format(total_rows_pruned))


@cli.command(name='classification_state')
@click.pass_context
@common.unhandled_exception_handler
@common.cli_wrapper(command='dirbs-prune', subcommand='classification_state', required_role='dirbs_core_power_user')
def classification_state(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root,
                         metrics_run_root):
    """Prune obsolete classification_state data."""
    curr_date = ctx.obj['CURR_DATE']

    # Store metadata
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       curr_date=curr_date.isoformat() if curr_date is not None else None,
                                       retention_months=config.retention_config.months_retention)

    logger.info('Pruning classification_state table to remove any classification state data related to '
                'obsolete conditions and data with end_date outside the retention window..')

    cond_config_list = [c.label for c in config.conditions]
    retention_months = config.retention_config.months_retention

    if curr_date is None:
        curr_date = datetime.date.today()

    first_month_to_drop = datetime.date(curr_date.year, curr_date.month, 1) - relativedelta.relativedelta(
        months=retention_months)
    logger.info('Classification state data with end_date earlier than {0} will be '
                'pruned'.format(first_month_to_drop))

    with utils.db_role_setter(conn, role_name='dirbs_core_power_user'), conn.cursor() as cursor:
        logger.debug('Calculating original number of rows in classification_state table...')
        cursor.execute('SELECT COUNT(*) FROM classification_state')
        rows_before = cursor.fetchone()[0]
        logger.debug('Calculated original number of rows in classification_state table')
        statsd.gauge('{0}rows_before'.format(metrics_run_root), rows_before)
        metadata.add_optional_job_metadata(metadata_conn, command, run_id, rows_before=rows_before)

        # Calculate number of rows in the classification table outside retention window
        cursor.execute(sql.SQL("""SELECT COUNT(*)
                                    FROM classification_state
                                   WHERE end_date < %s """), [first_month_to_drop])
        total_rows_out_window_to_prune = cursor.fetchone()[0]
        logger.info('Found {0:d} rows of classification_state table '
                    'with end_date outside the retention window to prune.'.format(total_rows_out_window_to_prune))

        # Calculate number of rows in the classification with conditions no longer existing
        cursor.execute(sql.SQL("""SELECT COUNT(*)
                                    FROM classification_state
                                   WHERE NOT starts_with_prefix(cond_name, %s)"""), [cond_config_list])
        total_rows_no_cond_to_prune = cursor.fetchone()[0]
        logger.info('Found {0:d} rows of classification_state table with conditions '
                    'no longer existing to prune.'.format(total_rows_no_cond_to_prune))

        logger.debug('Re-creating classification_state table...')
        # Basically, we just re-partition the classification_state table to re-create it, passing a src_filter_sql
        # parameter
        num_phys_imei_shards = partition_utils.num_physical_imei_shards(conn)
        src_filter_sql = cursor.mogrify("""WHERE (end_date > %s
                                              OR end_date IS NULL)
                                             AND cond_name LIKE ANY(%s)""",
                                        [first_month_to_drop, cond_config_list])
        partition_utils.repartition_classification_state(conn, num_physical_shards=num_phys_imei_shards,
                                                         src_filter_sql=str(src_filter_sql, encoding=conn.encoding))
        logger.debug('Re-created classification_state table')

        logger.debug('Calculating new number of rows in classification_state table...')
        cursor.execute('SELECT COUNT(*) FROM classification_state')
        rows_after = cursor.fetchone()[0]
        logger.debug('Calculated new number of rows in classification_state table')
        statsd.gauge('{0}rows_after'.format(metrics_run_root), rows_after)
        metadata.add_optional_job_metadata(metadata_conn, command, run_id, rows_after=rows_after)

        logger.info('Pruned {0:d} rows from classification_state table'.format(rows_after - rows_before))


def _warn_about_prune_all(prune_all, logger):
    """
    Function to print out warning about setting all in production.

    :param prune_all: prune all flag
    :param logger: dirbs logger obj
    """
    if prune_all is not False:
        logger.warning('*************************************************************************')
        logger.warning('WARNING: --prune_all option passed to dirbs-prune blacklist')
        logger.warning('*************************************************************************')
        logger.warning('')
        logger.warning('This should not be done in a production DIRBS deployment for the following reasons:')
        logger.warning('')
        logger.warning('1. All the IMEI falling in the specified pruning period will be pruned from blacklist')
        logger.warning('   irrespective of any condition specified, all the previously blacklisted IMEIs will now')
        logger.warning('   be removed from blacklist and allowed on network to operate.')
        logger.warning('')


@cli.command()
@click.pass_context
@common.unhandled_exception_handler
@common.cli_wrapper(command='dirbs-prune', subcommand='blacklist', required_role='dirbs_core_power_user')
@click.argument('condition_name', required=False, callback=common.validate_conditions)
@click.option('--prune-all',
              is_flag=True,
              help='DANGEROUS: If set, will set end_date to all the imeis falling in the specified period')
def blacklist(ctx, config, statsd, logger, run_id, conn, metadata_conn, command,
              metrics_root, metrics_run_root, condition_name, prune_all):
    """Expire IMEIs outside the blacklist retention period from blacklist."""
    current_date = datetime.date.today()
    retention_days = config.retention_config.blacklist_retention

    if condition_name is None and prune_all is False:
        logger.info('Error: one of the arguments "condition_name" or "--prune-all" is required')
        metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                           curr_date=current_date.isoformat(),
                                           retention_days=retention_days,
                                           job_executed=False)
    elif condition_name is not None and prune_all is True:
        logger.info('Error: only one of the arguments "condition_name" or "--prune-all" is required')
        metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                           curr_date=current_date.isoformat(),
                                           retention_days=retention_days,
                                           job_executed=False)
    elif retention_days == 0:
        logger.info('Blacklist will not be prune, as retention value is set to {0}'.format(retention_days))
        metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                           curr_date=current_date.isoformat(),
                                           retention_days=retention_days,
                                           job_executed=False)
    else:
        _warn_about_prune_all(prune_all, logger)
        logger.info('Pruning blacklist to remove any data related to specified condition '
                    'outside the retention window.')
        last_retention_date = datetime.date(current_date.year,
                                            current_date.month,
                                            current_date.day) - datetime.timedelta(retention_days)

        # store metadata
        logger.info('Blacklist entries with start_date earlier than {0} will be pruned'.format(last_retention_date))
        metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                           curr_date=current_date.isoformat(),
                                           retention_days=retention_days,
                                           job_executed=True,
                                           last_retention_date=last_retention_date.isoformat())

        with utils.db_role_setter(conn, role_name='dirbs_core_power_user'), conn.cursor() as cursor:
            logger.debug('Calculating original number of rows with block_date in classification_state table...')

            cursor.execute("""SELECT COUNT(*)
                                FROM classification_state
                               WHERE block_date IS NOT NULL
                                 AND end_date IS NULL""")
            rows_before = cursor.fetchone()[0]

            logger.debug('Calculated original number of rows (having block_date) in classification_state table')
            statsd.gauge('{0}rows_before'.format(metrics_run_root), rows_before)
            metadata.add_optional_job_metadata(metadata_conn, command, run_id, rows_before=rows_before)

            # if its a condition based pruning
            if not prune_all:
                cursor.execute(sql.SQL("""SELECT COUNT(*)
                                            FROM classification_state
                                           WHERE start_date < %s
                                             AND cond_name = %s
                                             AND end_date IS NULL
                                             AND block_date IS NOT NULL"""),
                               [last_retention_date, condition_name[0].label])
                total_rows_to_prune = cursor.fetchone()[0]

                logger.info('Found {0:d} rows of classification_state table '
                            'with start_date for {1} dimension outside the blacklist '
                            'retention window.'.format(total_rows_to_prune, condition_name[0].label))

                if total_rows_to_prune > 0:
                    cursor.execute(sql.SQL("""UPDATE classification_state
                                                 SET end_date = '{0}'
                                               WHERE start_date < '{1}'
                                                 AND cond_name = '{2}'
                                                 AND end_date IS NULL
                                                 AND block_date IS NOT NULL""".format(current_date.isoformat(),
                                                                                      last_retention_date,
                                                                                      condition_name[0].label)))

                logger.info('Pruned {0:d} rows from blacklist for {1} dimension'.format(
                    total_rows_to_prune, condition_name[0].label))

            # prune without any condition
            else:
                cursor.execute(sql.SQL("""SELECT COUNT(*)
                                            FROM classification_state
                                           WHERE start_date < %s
                                             AND end_date IS NULL
                                             AND block_date IS NOT NULL"""), [last_retention_date])
                total_rows_to_prune = cursor.fetchone()[0]

                logger.info('Found {0:d} rows of classification_state table '
                            'with start_date outside the blacklist retention window.'.format(total_rows_to_prune))

                if total_rows_to_prune > 0:
                    cursor.execute(sql.SQL("""UPDATE classification_state
                                                 SET end_date = '{0}'
                                               WHERE start_date < '{1}'
                                                 AND end_date IS NULL
                                                 AND block_date IS NOT NULL""".format(current_date.isoformat(),
                                                                                      last_retention_date)))
                logger.info('Pruned {0:d} rows from blacklist'.format(total_rows_to_prune))

            logger.debug('Calculating remaining number of rows with block_date (end_date is null) '
                         'in classification_state table...')
            cursor.execute("""SELECT COUNT(*)
                                FROM classification_state
                               WHERE block_date IS NOT NULL
                                 AND end_date IS NULL""")
            rows_after = cursor.fetchone()[0]

            logger.debug('Calculated remaining number of rows (having block_date and end_date null) '
                         'in classification_state table')
            statsd.gauge('{0}rows_after'.format(metrics_run_root), rows_after)
            metadata.add_optional_job_metadata(metadata_conn, command, run_id, rows_after=rows_after)


@cli.command()
@click.pass_context
@common.unhandled_exception_handler
@common.cli_wrapper(command='dirbs-prune', subcommand='lists', required_role='dirbs_core_power_user')
def lists(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root):
    """Prune obsolete lists data."""
    curr_date = ctx.obj['CURR_DATE']

    # store metadata
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       retention_months=config.retention_config.months_retention)

    logger.info('Pruning lists tables to remove any obsolete data with end_time outside the retention window..')
    retention_months = config.retention_config.months_retention

    if curr_date is None:
        curr_date = datetime.date.today()

    first_month_to_drop = datetime.date(curr_date.year, curr_date.month, 1) - relativedelta.relativedelta(
        months=retention_months)
    logger.info('Lists data with end_time earlier than {0} will be pruned'.format(first_month_to_drop))

    with utils.db_role_setter(conn, role_name='dirbs_core_power_user'), conn.cursor() as cursor:
        logger.debug('Calculating original number of rows in lists tables...')
        row_count_sql = sql.SQL("""SELECT blacklist_row_count, noft_lists_row_count, excp_lists_row_count
                                     FROM (SELECT COUNT(*)
                                             FROM blacklist) AS blacklist_row_count,
                                          (SELECT COUNT(*)
                                             FROM notifications_lists) AS noft_lists_row_count,
                                          (SELECT COUNT(*)
                                             FROM exceptions_lists) AS excp_lists_row_count""")
        cursor.execute(row_count_sql)
        rows_before = cursor.fetchone()
        blacklist_rows_before = int(rows_before.blacklist_row_count.strip('()'))
        notflist_rows_before = int(rows_before.noft_lists_row_count.strip('()'))
        excplist_rows_before = int(rows_before.excp_lists_row_count.strip('()'))
        rows_before = blacklist_rows_before + notflist_rows_before + excplist_rows_before
        logger.debug('Calculated original number of rows in lists tables...')
        statsd.gauge('{0}blacklist_rows_before'.format(metrics_run_root), blacklist_rows_before)
        statsd.gauge('{0}notifications_lists_rows_before'.format(metrics_run_root), notflist_rows_before)
        statsd.gauge('{0}exceptions_lists_rows_before'.format(metrics_run_root), excplist_rows_before)
        metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                           blacklist_rows_before=blacklist_rows_before,
                                           notifications_lists_rows_before=notflist_rows_before,
                                           exceptions_lists_rows_before=excplist_rows_before)

        # Calculate number of rows in the lists table outside the retention window
        job_metadata_filter_sql = """SELECT run_id
                                       FROM job_metadata
                                      WHERE command = 'dirbs-listgen'
                                        AND end_time < '{0}'""".format(first_month_to_drop)

        cursor.execute(sql.SQL("""SELECT COUNT(*)
                                    FROM blacklist
                                   WHERE start_run_id IN ({0})""".format(job_metadata_filter_sql)))
        total_bl_rows_out_window_to_prune = cursor.fetchone()[0]
        logger.info('Found {0:d} rows of blacklist table outside the retention window to prune'.format(
            total_bl_rows_out_window_to_prune))

        cursor.execute(sql.SQL("""SELECT COUNT(*)
                                    FROM notifications_lists
                                   WHERE start_run_id IN ({0})""".format(job_metadata_filter_sql)))
        total_nl_rows_out_window_to_prune = cursor.fetchone()[0]
        logger.info('Found {0:d} rows of notifications lists table outside the retention window to prune'.format(
            total_nl_rows_out_window_to_prune))

        cursor.execute(sql.SQL("""SELECT COUNT(*)
                                    FROM exceptions_lists
                                   WHERE start_run_id IN ({0})""".format(job_metadata_filter_sql)))
        total_nl_rows_out_window_to_prune = cursor.fetchone()[0]
        logger.info('Found {0:d} rows of exceptions lists table outside the retention window to prune'.format(
            total_nl_rows_out_window_to_prune))

        # We repartition the tables to re-create them, passing a condition sql
        logger.debug('Re-creating blacklist table...')
        num_phys_imei_shards = partition_utils.num_physical_imei_shards(conn)
        src_filter_sql = cursor.mogrify("""WHERE start_run_id NOT IN ({0})""".format(
            job_metadata_filter_sql))
        partition_utils.repartition_blacklist(conn, num_physical_shards=num_phys_imei_shards,
                                              src_filter_sql=str(src_filter_sql, encoding=conn.encoding))
        logger.debug('Re-created blacklist table')

        logger.debug('Re-creating notifications lists table...')
        partition_utils.repartition_notifications_lists(conn, num_physical_shards=num_phys_imei_shards,
                                                        src_filter_sql=str(src_filter_sql,
                                                                           encoding=conn.encoding))
        logger.debug('Re-created notifications lists table')

        logger.debug('Re-creating exceptions lists table...')
        partition_utils.repartition_exceptions_lists(conn, num_physical_shards=num_phys_imei_shards,
                                                     src_filter_sql=str(src_filter_sql,
                                                                        encoding=conn.encoding))
        logger.debug('Re-created exceptions lists table')

        logger.debug('Calculating new number of rows in lists tables...')
        cursor.execute(row_count_sql)
        rows_after = cursor.fetchone()
        blacklist_rows_after = int(rows_after.blacklist_row_count.strip('()'))
        notflist_rows_after = int(rows_after.noft_lists_row_count.strip('()'))
        excplist_rows_after = int(rows_after.excp_lists_row_count.strip('()'))
        rows_after = blacklist_rows_after + notflist_rows_after + excplist_rows_after
        logger.debug('Calculated new number of rows in lists tables')
        statsd.gauge('{0}blacklist_rows_after'.format(metrics_run_root), blacklist_rows_after)
        statsd.gauge('{0}notifications_lists_rows_after'.format(metrics_run_root), notflist_rows_after)
        statsd.gauge('{0}exceptions_lists_rows_after'.format(metrics_run_root), excplist_rows_after)
        metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                           blacklist_rows_before=blacklist_rows_after,
                                           notifications_lists_rows_before=notflist_rows_after,
                                           exceptions_lists_rows_before=excplist_rows_after)
        logger.info('Pruned {0:d} rows from lists tables'.format(rows_after - rows_before))
