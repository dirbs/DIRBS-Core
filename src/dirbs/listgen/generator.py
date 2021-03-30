"""
DIRBS class for generating lists (blacklist, per-MNO notification lists and per-MNO exception lists).

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
import os
import re
import contextlib
import zipfile
import glob
from functools import partial
from concurrent import futures
import csv
import logging
from collections import defaultdict

from psycopg2 import sql
from psycopg2.extras import execute_values

from dirbs.utils import CodeProfiler, create_db_connection, hash_string_64bit, \
    most_recently_run_condition_info, compute_md5_hash, compute_analysis_end_date, log_analysis_window, \
    table_exists_sql, db_role_setter
import dirbs.metadata as metadata
import dirbs.partition_utils as partition_utils


class ListGenerationLockException(Exception):
    """Indicates that we couldn't acquire the lock for list generation for this run."""

    pass


class ListGenerationInvalidBaseException(Exception):
    """Indicates that the run ID specified as the base for deltas is not valid."""

    pass


class ListGenerationSanityChecksFailedException(Exception):
    """Indicates that the sanity checks for the process has failed."""

    pass


class ListsGenerator:
    """Class responsible for generating all classification lists (blacklists, notification lists, exception lists)."""

    def __init__(self, *, config, logger, run_id, conn, metadata_conn, output_dir, conditions=None,
                 curr_date=None, no_full_lists=False, no_cleanup=False, base_run_id=-1, disable_sanity_checks=False):
        """Constructor to initialize list generator processor.

        Arguments:
            config -- dirbs config object
            logger -- dirbs logger object
            run_id -- current run_id of the job
            conn -- dirbs database connection required for job processing
            metadata_conn -- dirbs database connection to write metadata about the job
            output_dir -- path of the output dir to write lists to
        Keyword Arguments:
            conditions -- list of blocking conditions to generate lists of (default None)
            curr_date -- date to use as current date for generation (default None)
            no_full_lists -- boolean to indicate full or delta list generation only (default False)
            no_cleanup -- boolean to indicate weather to clean up temporary tables or not (default False)
            base_run_id -- run id to use as a base for the current job (default -1, means no run id)
            disable_sanity_checks -- boolean to disable sanity checks on configurations (default false)
        """
        self._config = config
        self._logger = logger
        self._run_id = run_id
        self._conn = conn
        self._metadata_conn = metadata_conn
        self._no_cleanup = no_cleanup
        self._no_full_lists = no_full_lists
        self._curr_date = curr_date
        self._disable_sanity_checks = disable_sanity_checks
        self._condtions = conditions
        self._lookback_days = self._config.listgen_config.lookback_days
        self._restrict_exceptions_list = self._config.listgen_config.restrict_exceptions_list
        self._include_barred_imeis = self._config.listgen_config.include_barred_imeis
        self._generate_check_digit = self._config.listgen_config.generate_check_digit
        self._output_invalid_imeis = self._config.listgen_config.output_invalid_imeis
        self._notify_imsi_change = self._config.listgen_config.notify_imsi_change
        self._operators = self._config.region_config.operators
        self._amnesty = self._config.amnesty_config
        self._intermediate_table_names = []

        # Query the job metadata table for all successful list generation runs
        successful_job_runs = metadata.query_for_command_runs(self._metadata_conn,
                                                              'dirbs-listgen',
                                                              successful_only=True)
        # self._successful_job_run = successful_job_runs
        if base_run_id == -1:
            if not successful_job_runs:
                self._logger.warning('No previous successful dirbs-listgen run found. Deltas will be entire lists...')
                self._base_run_id = -1
            else:
                if not disable_sanity_checks and not self._perform_sanity_checks():
                    raise ListGenerationSanityChecksFailedException(
                        'Sanity checks failed, configurations are not identical to the last successful list generation'
                    )
                self._base_run_id = successful_job_runs[0].run_id
        else:
            run_ids = [r.run_id for r in successful_job_runs]
            if base_run_id not in run_ids:
                raise ListGenerationInvalidBaseException(
                    'Specified base run id {0:d} not found in list of successful dirbs-listgen runs'.format(
                        base_run_id))
            if not disable_sanity_checks and not self._perform_sanity_checks(base_run_id):
                raise ListGenerationSanityChecksFailedException(
                    'Sanity checks failed, configurations are not identical to the last successful list generation')
            self._base_run_id = base_run_id

        if self._base_run_id != -1:
            self._logger.info('Using previous successful dirbs-listgen run id {0:d} as base for delta lists...'
                              .format(self._base_run_id))

        # We need at least 4 workers for list generation, as top-level list generation futures will themselves create
        # futures and so on.
        self._nworkers = max(4, self._config.multiprocessing_config.max_db_connections)

        # Get blocking conditions, work out the most recent successful classification run across all blocking
        # conditions and get the maximum run_id of those
        with self._conn as conn:
            if self._condtions is None:
                self._blocking_conditions = [c for c in self._config.conditions if c.blocking]
            else:
                self._blocking_conditions = self._condtions
            if not self._blocking_conditions:
                logger.warning('No blocking conditions configured, blacklist and notification lists will be empty.')
            else:
                condtion_labels = [c.label for c in self._blocking_conditions]
                logger.info('Lists will be generated for these blocking conditions: {0}'.format(condtion_labels))

            blocking_conditions_config = most_recently_run_condition_info(conn,
                                                                          [c.label for c in self._blocking_conditions],
                                                                          successful_only=True)
            if blocking_conditions_config:
                self._class_run_id = max([v['run_id'] for k, v in blocking_conditions_config.items()])
            else:
                self._class_run_id = None

        # Make directory based on timestamp and class_run_id -- we always use now() here to better ensure
        # uniqueness
        self._date_str = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        if self._class_run_id is None:
            self._output_dir = os.path.join(output_dir, 'listgen__{0}'.format(self._date_str))
        else:
            self._output_dir = os.path.join(output_dir,
                                            'listgen__{0}__classid_{1:d}'.format(self._date_str, self._class_run_id))
        os.makedirs(self._output_dir)

        # Add optional metadata
        metadata.add_optional_job_metadata(self._metadata_conn, 'dirbs-listgen', self._run_id,
                                           output_dir=os.path.abspath(str(output_dir)),
                                           curr_date=self._curr_date.isoformat()
                                           if self._curr_date is not None else None,
                                           no_full_lists=self._no_full_lists,
                                           no_cleanup=self._no_cleanup,
                                           lookback_days=self._lookback_days,
                                           restrict_exceptions_list=self._restrict_exceptions_list,
                                           include_barred_imeis=self._include_barred_imeis,
                                           generate_check_digit=self._generate_check_digit,
                                           output_invalid_imeis=self._output_invalid_imeis,
                                           notify_imsi_change=self._notify_imsi_change,
                                           base_run_id=self._base_run_id,
                                           blocking_conditions=[c.as_dict() for c in self._blocking_conditions],
                                           classification_run_id=self._class_run_id,
                                           operators=[op.as_dict() for op in self._operators],
                                           amnesty=self._amnesty.as_dict())

    def generate_lists(self):
        """Function that generates the CSV list outputs in the output directory."""
        has_lock = False
        try:
            # Ensure we are the only dirbs-listgen process running
            has_lock = self._try_acquire_listgen_lock()
            if not has_lock:
                raise ListGenerationLockException('Could not acquire lock for list generation. '
                                                  'Are there any other dirbs-listgen instances running at the moment?')

            # We change our role to dirbs_core_listgen so that any created tables and sequences have the right
            # owners
            with db_role_setter(self._conn, role_name='dirbs_core_listgen'), self._conn.cursor() as cursor:
                # Generate intermediate tables containing new full lists
                self._create_intermediate_new_tables()

                # # Generate intermediate tables containing list from most recent version
                self._create_intermediate_old_tables()

                # # Now we generate intermediate tables containing the new and the current lists
                self._create_intermediate_delta_tables()

                # # Now store deltas
                self._store_list_deltas()

            # Generate CSV files, both the full lists and the deltas between the new lists and the specified base
            self._write_csv_lists()

        finally:
            if has_lock:
                self._release_listgen_lock()

            if not self._no_cleanup:
                with self._conn as conn, conn.cursor() as cursor:
                    for tblname in self._intermediate_table_names:
                        self._logger.debug('Cleanup: dropping intermediate table {0}...'.format(tblname))
                        cursor.execute(sql.SQL('DROP TABLE {0} CASCADE').format(sql.Identifier(tblname)))
                        self._logger.debug('Cleanup: dropped intermediate table {0}'.format(tblname))
            else:
                self._logger.warning('Skipping intermediate table cleanup due to command-line option')

    def __getstate__(self):
        """Custom function to allow use of this class in ProcessPoolExecutor."""
        state = self.__dict__.copy()
        # We can't and shouldn't use _conn, _metadata_conn and _logger in a separate process
        del state['_conn']
        del state['_metadata_conn']
        del state['_logger']
        return state

    def __setstate__(self, state):
        """Custom function to allow use of this class in ProcessPoolExecutor."""
        self.__dict__.update(state)

    @property
    def _lock_key(self):
        """Key to use for locking listgen to ensure only one concurrent list generation."""
        return hash_string_64bit('dirbs-listgen')

    @property
    def _blacklist_tblname(self):
        """Blacklist table name."""
        return 'blacklist'

    @property
    def _notifications_lists_tblname(self):
        """Notifications list table name."""
        return 'notifications_lists'

    def _notifications_lists_part_tblname(self, operator_id):
        """Per-MNO notifications list partition name."""
        return '{0}_{1}'.format(self._notifications_lists_tblname, operator_id)

    @property
    def _exceptions_lists_tblname(self):
        """Exceptions list table name."""
        return 'exceptions_lists'

    def _exceptions_lists_part_tblname(self, operator_id):
        """Per-MNO exceptions list partition name."""
        return '{0}_{1}'.format(self._exceptions_lists_tblname, operator_id)

    @property
    def _blacklist_old_tblname(self):
        """Name to use for the temporary base blacklist used for generating the delta."""
        return 'listgen_temp_{0}_old_blacklist'.format(self._run_id)

    @property
    def _notifications_lists_old_tblname(self):
        """Name to use for the temporary base notifications list used for generating the delta."""
        return 'listgen_temp_{0}_old_notifications_lists'.format(self._run_id)

    def _notifications_lists_old_part_tblname(self, operator_id):
        """Name to use for the temporary per-MNO base notifications list partition used for generating the delta."""
        return '{0}_{1}'.format(self._notifications_lists_old_tblname, operator_id)

    @property
    def _exceptions_lists_old_tblname(self):
        """Name to use for the temporary base exceptions list used for generating the delta."""
        return 'listgen_temp_{0}_old_exceptions_lists'.format(self._run_id)

    def _exceptions_lists_old_part_tblname(self, operator_id):
        """Name to use for the temporary per-MNO base exceptions list partition used for generating the delta."""
        return '{0}_{1}'.format(self._exceptions_lists_old_tblname, operator_id)

    @property
    def _blacklist_new_tblname(self):
        """Name to use for the blacklisted IMEI intermediate table."""
        return 'listgen_temp_{0}_new_blacklist'.format(self._run_id)

    @property
    def _notifications_lists_new_tblname(self):
        """Name to use for the parent table for the partitioned per-MNO notifications table."""
        return 'listgen_temp_{0}_new_notifications_lists'.format(self._run_id)

    def _notifications_lists_new_part_tblname(self, operator_id):
        """Name to use for each MNO partition of the per-MNO notifications table."""
        return '{0}_{1}'.format(self._notifications_lists_new_tblname, operator_id)

    @property
    def _exceptions_lists_new_tblname(self):
        """Name to use for the parent table for the partitioned per-MNO exceptions table."""
        return 'listgen_temp_{0}_new_exceptions_lists'.format(self._run_id)

    def _exceptions_lists_new_part_tblname(self, operator_id):
        """Name to use for each MNO partition of the per-MNO exceptions table."""
        return '{0}_{1}'.format(self._exceptions_lists_new_tblname, operator_id)

    @property
    def _blacklist_delta_tblname(self):
        """Name to use for the blacklisted IMEI intermediate table."""
        return 'listgen_temp_{0}_delta_blacklist'.format(self._run_id)

    @property
    def _notifications_lists_delta_tblname(self):
        """Name to use for the parent table for the partitioned per-MNO notifications table."""
        return 'listgen_temp_{0}_delta_notifications_lists'.format(self._run_id)

    def _notifications_lists_delta_part_tblname(self, operator_id):
        """Name to use for each MNO partition of the per-MNO notifications table."""
        return '{0}_{1}'.format(self._notifications_lists_delta_tblname, operator_id)

    @property
    def _exceptions_lists_delta_tblname(self):
        """Name to use for the parent table for the partitioned per-MNO exceptions table."""
        return 'listgen_temp_{0}_delta_exceptions_lists'.format(self._run_id)

    def _exceptions_lists_delta_part_tblname(self, operator_id):
        """Name to use for each MNO partition of the per-MNO exceptions table."""
        return '{0}_{1}'.format(self._exceptions_lists_delta_tblname, operator_id)

    @property
    def _blocking_conditions_new_tblname(self):
        """Name to use for the blocking conditions intermediate table."""
        return 'listgen_temp_{0}_new_blocking_conditions_table'.format(self._run_id)

    @property
    def _mnc_mcc_new_tblname(self):
        """Name to use for the intermediate MCC-MNC -> operator lookup table."""
        return 'listgen_temp_{0}_new_mcc_mnc_table'.format(self._run_id)

    @property
    def _notifications_imei_new_tblname(self):
        """Name to use for the intermediate IMEIs to notify table."""
        return 'listgen_temp_{0}_new_notifications_imeis'.format(self._run_id)

    @property
    def _notifications_triplets_new_tblname(self):
        """Name to use for the intermediate triplets to notify table."""
        return 'listgen_temp_{0}_new_notifications_triplets'.format(self._run_id)

    @property
    def _pairings_imei_imsi_new_tblname(self):
        """Name to use for the IMEI/IMSI pairing intermediate table."""
        return 'listgen_temp_{0}_new_pairings_imei_imsis'.format(self._run_id)

    @property
    def _notification_list_columns(self):
        notification_list_cols = ['imei', 'imsi', 'msisdn', 'block_date', 'reasons']
        include_amnesty_column = sql.SQL('')
        if self._amnesty.amnesty_enabled:
            notification_list_cols.append('amnesty_granted')
            include_amnesty_column = sql.SQL(', amnesty_granted')
        return notification_list_cols, include_amnesty_column

    def _try_acquire_listgen_lock(self):
        """Try to acquire the lock ensuring only one list generation is proceeding concurrently."""
        with self._conn as conn, conn.cursor() as cursor:
            cursor.execute('SELECT pg_try_advisory_lock(%s::BIGINT)', [self._lock_key])
            return cursor.fetchone()[0]

    def _release_listgen_lock(self):
        """Release the listgen lock preventing other listgen from executing."""
        with self._conn as conn, conn.cursor() as cursor:
            cursor.execute('SELECT pg_advisory_unlock(%s::BIGINT)', [self._lock_key])

    def _add_pk(self, conn, *, tblname, pk_columns):
        """Helper function to DRY out adding a primary key to tables."""
        idx_metadatum = partition_utils.IndexMetadatum(idx_cols=pk_columns, is_unique=True)
        partition_utils.add_indices(conn, tbl_name=tblname, idx_metadata=[idx_metadatum])

    def _analyze_helper(self, cursor, tblname):
        """Function to DRY out exact command to use when ANALYZE'ing new tables."""
        cursor.execute(sql.SQL('ANALYZE {0}').format(sql.Identifier(tblname)))

    def _perform_sanity_checks(self, base_run_id=None):
        """Method to perform sanity checks on list gen."""
        current_loopback_days = self._lookback_days
        current_blocking_conditions = [c.as_dict() for c in self._config.conditions if c.blocking]
        current_operator_configs = [op.as_dict() for op in self._operators]
        current_amnesty_configs = self._amnesty.as_dict()

        if base_run_id:
            base_successful_job_run = metadata.query_for_command_runs(self._metadata_conn,
                                                                      'dirbs-listgen',
                                                                      successful_only=True,
                                                                      run_id=base_run_id)
            base_successful_job_run = base_successful_job_run[0]
            base_successful_job_run = base_successful_job_run.extra_metadata

        else:
            base_successful_job_run = metadata.query_for_command_runs(self._metadata_conn,
                                                                      'dirbs-listgen',
                                                                      successful_only=True)
            base_successful_job_run = base_successful_job_run[0]
            base_successful_job_run = base_successful_job_run.extra_metadata

        if current_loopback_days == base_successful_job_run['lookback_days'] and \
                current_blocking_conditions == base_successful_job_run['blocking_conditions'] and \
                current_operator_configs == base_successful_job_run['operators'] and \
                current_amnesty_configs == base_successful_job_run['amnesty']:
            return True
        return False

    def _create_operator_partitions(self, conn, *, parent_tbl_name, child_name_fn,
                                    fillfactor=100, allow_existing=False, is_unlogged=True):
        """Helper function to DRY out adding child partitions to a parent table."""
        with conn.cursor() as cursor:
            for op in self._operators:
                op_shard_name = child_name_fn(op.id)
                if allow_existing:
                    cursor.execute(table_exists_sql(), [op_shard_name])
                    if cursor.fetchone().exists:
                        continue

                partition_utils.create_per_mno_lists_partition(conn,
                                                               operator_id=op.id,
                                                               parent_tbl_name=parent_tbl_name,
                                                               tbl_name=op_shard_name,
                                                               unlogged=is_unlogged,
                                                               fillfactor=fillfactor)

    def _get_total_record_count(self, conn, tblname):
        """Helper function to get the total number of records."""
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL('SELECT COUNT(*) FROM {0}').format(sql.Identifier(tblname)))
            return cursor.fetchone()[0]

    def _queue_intermediate_table_job(self, executor, futures_to_cb, fn, description):
        """Function to queue a job to calculate an intermediate table."""
        self._logger.debug('Calculating intermediate table containing {0} [QUEUED]...'.format(description))
        futures_to_cb[executor.submit(fn, executor)] = partial(self._process_intermediate_table_job_result,
                                                               description)

    def _process_intermediate_table_job_result(self, description, future):
        """Function to process the results of a job to calculate an intermediate table."""
        rows, duration = future.result()
        msg = 'Calculated intermediate table containing {0} (duration {1:.3f}s)' \
            .format(description, duration / 1000)
        if rows != -1:
            msg += ' [{0:d} rows inserted]'.format(rows)
        self._logger.info(msg)

    def _run_intermediate_table_job(self, conn, fn, description=None):
        """Function to synchronsouly run an intermediate table job."""
        if description is not None:
            self._logger.debug('Calculating intermediate table containing {0}...'.format(description))
        rows, duration = fn(conn)
        if description is not None:
            msg = 'Calculated intermediate table containing {0} (duration {1:.3f}s)' \
                .format(description, duration / 1000)
            if rows != -1:
                msg += ' [{0:d} rows inserted]'.format(rows)
            self._logger.info(msg)

    def _queue_delta_table_job(self, executor, futures_to_cb, fn, description):
        """Function to queue a job to update a persistent delta table table."""
        self._logger.debug('Storing delta in table containing {0} [QUEUED]...'.format(description))
        futures_to_cb[executor.submit(fn, executor)] = partial(self._process_delta_table_job_result,
                                                               description)

    def _process_delta_table_job_result(self, description, future):
        """Function to process the results of a job to store data in delta table table."""
        per_type_counts, duration = future.result()
        per_type_counts_string = ', '.join(['{0} {1} changes'.format(v, k) for k, v in per_type_counts.items()])
        msg = 'Stored delta in table containing {0} (duration {1:.3f}s) [{2}]' \
            .format(description, duration / 1000, per_type_counts_string)
        self._logger.info(msg)

    def _wait_for_futures(self, futures_to_cb):
        """Function to wait for any futures in the futures_to_cb dict, calling associated callbacks as completed."""
        for f in futures.as_completed(futures_to_cb):
            # Call the associated callback for this future passing the future itself as the only argument.
            # Other required arguments to the callback should already partially applied to the callback
            # at this point.
            futures_to_cb[f](f)

    def _create_intermediate_new_tables(self):
        """Create temp tables for blacklisted IMEIs, IMEIs to notify, etc."""
        with futures.ThreadPoolExecutor(max_workers=self._nworkers) as executor:
            # Create all the tables and populate initial tables. Need to commit so that threadpool jobs can
            # see the results
            with self._conn:
                self._run_intermediate_table_job(self._conn, self._create_intermediate_new_tables_structure)
                # Create utility tables
                self._run_intermediate_table_job(self._conn, self._populate_blocking_conditions_table,
                                                 description='blocking conditions')
                self._run_intermediate_table_job(self._conn, self._populate_mcc_mnc_table,
                                                 description='MCC-MNC operator mappings')
                # Create new blacklist table
                self._run_intermediate_table_job(self._conn, self._populate_new_blacklist,
                                                 description='IMEIs to blacklist')

            # Create required notifications and pairings tables in parallel before we can kick off the per-MNO
            # pairing and and notifications. These jobs have the responsibilities of kicking off those per-MNO jobs
            futures_to_cb = {}
            self._queue_intermediate_table_job(executor,
                                               futures_to_cb,
                                               self._populate_new_notifications_lists,
                                               'per-MNO notifications for all operators')
            self._queue_intermediate_table_job(executor,
                                               futures_to_cb,
                                               self._populate_new_exceptions_lists,
                                               'per-MNO exceptions for all operators')
            self._wait_for_futures(futures_to_cb)

    def _create_intermediate_new_tables_structure(self, conn):
        """Create table structure for new tables used to create deltas."""
        table_names = []
        with conn.cursor() as cursor, CodeProfiler() as cp:
            tblname = self._blacklist_new_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                        imei_norm                     TEXT NOT NULL,
                                        virt_imei_shard               SMALLINT NOT NULL,
                                        block_date                    DATE NOT NULL,
                                        reasons                       TEXT[] NOT NULL,
                                        is_valid                      BOOLEAN,
                                        imei_norm_with_check_digit    TEXT
                                      ) PARTITION BY RANGE (virt_imei_shard)
                                   """).format(sql.Identifier(tblname)))
            partition_utils.create_imei_shard_partitions(conn, tbl_name=tblname, unlogged=True)
            table_names.append(tblname)

            tblname = self._notifications_lists_new_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                        operator_id                 TEXT NOT NULL,
                                        imei_norm                   TEXT NOT NULL,
                                        virt_imei_shard             SMALLINT NOT NULL,
                                        imsi                        TEXT NOT NULL,
                                        msisdn                      TEXT NOT NULL,
                                        block_date                  DATE NOT NULL,
                                        reasons                     TEXT[] NOT NULL,
                                        is_valid                    BOOLEAN,
                                        amnesty_granted             BOOLEAN,
                                        imei_norm_with_check_digit  TEXT
                                      ) PARTITION BY LIST (operator_id)
                                   """).format(sql.Identifier(tblname)))
            table_names.append(tblname)
            self._create_operator_partitions(conn,
                                             parent_tbl_name=tblname,
                                             child_name_fn=self._notifications_lists_new_part_tblname,
                                             is_unlogged=True)

            tblname = self._exceptions_lists_new_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                        operator_id                 TEXT NOT NULL,
                                        imei_norm                   TEXT NOT NULL,
                                        virt_imei_shard             SMALLINT NOT NULL,
                                        imsi                        TEXT NOT NULL,
                                        is_valid                    BOOLEAN,
                                        imei_norm_with_check_digit  TEXT,
                                        is_blacklisted              BOOLEAN,
                                        is_barred                   BOOLEAN,
                                        have_barred_tac             BOOLEAN,
                                        msisdn                      TEXT NOT NULL
                                      ) PARTITION BY LIST (operator_id)
                                   """).format(sql.Identifier(tblname)))
            table_names.append(tblname)
            self._create_operator_partitions(conn,
                                             parent_tbl_name=tblname,
                                             child_name_fn=self._exceptions_lists_new_part_tblname,
                                             is_unlogged=True)

            tblname = self._blocking_conditions_new_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                          cond_name     TEXT NOT NULL,
                                          reason        TEXT NOT NULL
                                      )""")
                           .format(sql.Identifier(tblname)))
            table_names.append(tblname)

            tblname = self._mnc_mcc_new_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                          mcc_mnc_pattern       TEXT NOT NULL,
                                          operator_id           TEXT NOT NULL
                                      )""")
                           .format(sql.Identifier(tblname)))
            table_names.append(tblname)

            tblname = self._notifications_imei_new_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                          imei_norm                     TEXT NOT NULL,
                                          virt_imei_shard               SMALLINT NOT NULL,
                                          block_date                    DATE NOT NULL,
                                          reasons                       TEXT[] NOT NULL,
                                          is_valid                      BOOLEAN,
                                          amnesty_granted               BOOLEAN,
                                          imei_norm_with_check_digit    TEXT
                                      ) PARTITION BY RANGE (virt_imei_shard)""")
                           .format(sql.Identifier(tblname)))
            partition_utils.create_imei_shard_partitions(conn, tbl_name=tblname, unlogged=True)
            table_names.append(tblname)

            tblname = self._notifications_triplets_new_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                          imei_norm                     TEXT NOT NULL,
                                          virt_imei_shard               SMALLINT NOT NULL,
                                          imsi                          TEXT NOT NULL,
                                          msisdn                        TEXT NOT NULL,
                                          block_date                    DATE NOT NULL,
                                          reasons                       TEXT[] NOT NULL,
                                          is_valid                      BOOLEAN,
                                          amnesty_granted               BOOLEAN,
                                          imei_norm_with_check_digit    TEXT,
                                          home_operator                 TEXT,
                                          fallback_operators            TEXT[]
                                      ) PARTITION BY RANGE (virt_imei_shard)""")
                           .format(sql.Identifier(tblname)))
            partition_utils.create_imei_shard_partitions(conn, tbl_name=tblname, unlogged=True)
            table_names.append(tblname)

            tblname = self._pairings_imei_imsi_new_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                          imei_norm                     TEXT NOT NULL,
                                          virt_imei_shard               SMALLINT NOT NULL,
                                          imsi                          TEXT NOT NULL,
                                          is_valid                      BOOLEAN,
                                          imei_norm_with_check_digit    TEXT,
                                          home_operator                 TEXT,
                                          is_blacklisted                BOOLEAN,
                                          is_barred                     BOOLEAN,
                                          have_barred_tac               BOOLEAN,
                                          msisdn                        TEXT NOT NULL
                                      ) PARTITION BY RANGE (virt_imei_shard) """)
                           .format(sql.Identifier(tblname)))
            partition_utils.create_imei_shard_partitions(conn, tbl_name=tblname, unlogged=True, fillfactor=45)
            table_names.append(tblname)

        self._intermediate_table_names.extend(table_names)
        return -1, cp.duration

    def _populate_blocking_conditions_table(self, conn):
        """Function to populate the blocking conditions table for this run."""
        with conn.cursor() as cursor, CodeProfiler() as cp:
            tblname = self._blocking_conditions_new_tblname
            execute_values(cursor,
                           sql.SQL("""INSERT INTO {0}(cond_name, reason)
                                           VALUES %s""").format(sql.Identifier(tblname)).as_string(cursor),
                           [(c.label, c.reason) for c in self._blocking_conditions])
            self._add_pk(conn, tblname=tblname, pk_columns=['cond_name'])
            self._analyze_helper(cursor, tblname)

        # Need to get table count since execute_values doesn't retain insert count
        num_records = self._get_total_record_count(conn, tblname)
        return num_records, cp.duration

    def _populate_mcc_mnc_table(self, conn):
        """Function to populate the MCC-MNC -> operator ID lookup table for this run."""
        with conn.cursor() as cursor, CodeProfiler() as cp:
            tblname = self._mnc_mcc_new_tblname
            execute_values(cursor,
                           sql.SQL("""INSERT INTO {0}(mcc_mnc_pattern, operator_id)
                                           VALUES %s""").format(sql.Identifier(tblname)).as_string(cursor),
                           [(p['mcc'] + p['mnc'] + '%', op.id) for op in self._operators for p in op.mcc_mnc_pairs])
            self._add_pk(conn, tblname=tblname, pk_columns=['mcc_mnc_pattern'])
            self._analyze_helper(cursor, tblname)

        # Need to get table count since execute_values doesn't retain insert count
        num_records = self._get_total_record_count(conn, tblname)
        return num_records, cp.duration

    def _populate_new_blacklist(self, conn):
        """Function to create and generate the blacklisted IMEIs temp table for this run."""
        with CodeProfiler() as cp:
            tblname = self._blacklist_new_tblname
            num_records = self._populate_new_blacklist_or_notifications_imei_table(conn,
                                                                                   tblname, is_blacklist=True)

        return num_records, cp.duration

    @property
    def _is_valid_and_check_digit_queries(self):
        """Property generating a tuple of queries to generate is_valid and imei_norm_with_check_digit values."""
        if not self._output_invalid_imeis or self._generate_check_digit:
            is_valid_query = sql.SQL('SELECT is_valid_imei_norm(imei_norm) AS is_valid')
        else:
            is_valid_query = sql.SQL('SELECT NULL::BOOLEAN AS is_valid')

        if self._generate_check_digit:
            imei_norm_with_check_digit_query = sql.SQL("""SELECT CASE
                                                                 WHEN is_valid = TRUE
                                                                 THEN luhn_check_digit_append(imei_norm)
                                                                 ELSE imei_norm
                                                                 END AS imei_norm_with_check_digit""")
        else:
            imei_norm_with_check_digit_query = sql.SQL('SELECT NULL::TEXT AS imei_norm_with_check_digit')

        return is_valid_query, imei_norm_with_check_digit_query

    @property
    def _blacklisted_pairings_filter_query(self):
        """Property generates a SQL fragment which restricts the pairing list to blacklisted IMEIs."""
        if self._restrict_exceptions_list:
            blacklisted_filter_sql = sql.SQL('is_blacklisted IS TRUE')
        else:
            blacklisted_filter_sql = sql.SQL('TRUE')
        return blacklisted_filter_sql

    @property
    def _barred_pairings_filter_query(self):
        """Property generates a SQL fragment which restricts barred IMEIs from exceptions list."""
        if not self._include_barred_imeis:
            barred_filter_sql = sql.SQL('is_barred IS FALSE AND have_barred_tac IS FALSE')
        else:
            barred_filter_sql = sql.SQL('TRUE')
        return barred_filter_sql

    def _populate_new_blacklist_or_notifications_imei_table(self, conn, tblname, *, is_blacklist):
        """Helper function to DRY out populating either an IMEI table for blacklist or notifications."""
        is_valid_query, imei_norm_with_check_digit_query = self._is_valid_and_check_digit_queries

        if is_blacklist:
            block_date_filter = sql.SQL('block_date <= %s')
            exclude_blacklisted_imeis_query = sql.SQL('')
            include_amnesty_column = sql.SQL('')

        else:
            block_date_filter = sql.SQL('block_date > %s')
            exclude_blacklisted_imeis_query = \
                sql.SQL("""AND NOT EXISTS (SELECT 1
                                             FROM {blacklist_tblname}
                                            WHERE imei_norm = cs.imei_norm)""").format(
                    blacklist_tblname=sql.Identifier(self._blacklist_new_tblname))
            include_amnesty_column = sql.SQL(', amnesty_granted')

        # Populate table
        query = sql.SQL("""INSERT INTO {tblname}(imei_norm,
                                                 virt_imei_shard,
                                                 block_date,
                                                 reasons,
                                                 is_valid,
                                                 imei_norm_with_check_digit
                                                 {include_amnesty_column})
                                SELECT imei_norm,
                                       virt_imei_shard,
                                       min_block_date,
                                       reasons,
                                       is_valid,
                                       imei_norm_with_check_digit
                                       {include_amnesty_column}
                                  FROM (SELECT imei_norm,
                                               FIRST(virt_imei_shard) AS virt_imei_shard,
                                               bool_and(amnesty_granted) AS amnesty_granted,
                                               MIN(block_date) AS min_block_date,
                                               array_agg(DISTINCT reason ORDER BY reason) AS reasons
                                          FROM (SELECT *
                                                  FROM classification_state
                                                  JOIN {blocking_conditions_tblname}
                                                 USING (cond_name)
                                                 WHERE end_date IS NULL
                                                   AND block_date IS NOT NULL
                                                   AND {block_date_filter}) cs
                                         WHERE NOT EXISTS(SELECT 1
                                                            FROM golden_list gl
                                                           WHERE hashed_imei_norm = md5(cs.imei_norm)::UUID)
                                               {exclude_blacklisted_imeis_query}
                                      GROUP BY imei_norm) bl_imeis,
                                       LATERAL ({is_valid_query}) is_valid_tbl,
                                       LATERAL ({imei_norm_with_check_digit_query}) check_digit_tbl
                        """).format(tblname=sql.Identifier(tblname),  # noqa: Q447
                                    include_amnesty_column=include_amnesty_column,
                                    is_valid_query=is_valid_query,
                                    imei_norm_with_check_digit_query=imei_norm_with_check_digit_query,
                                    blocking_conditions_tblname=sql.Identifier(self._blocking_conditions_new_tblname),
                                    block_date_filter=block_date_filter,
                                    exclude_blacklisted_imeis_query=exclude_blacklisted_imeis_query)

        # Get current date for the purposes of blacklisting. Most of the time, this will be NULL and use the date
        # of the run
        if self._curr_date is not None:
            curr_date = self._curr_date
        else:
            # Query for metadata date for this run_id
            job_start_time = metadata.job_start_time_by_run_id(self._metadata_conn, self._run_id)
            assert job_start_time is not None
            curr_date = job_start_time.date()

        with conn.cursor() as cursor:
            cursor.execute(query, [curr_date])
            num_records = cursor.rowcount
            self._add_pk(conn, tblname=tblname, pk_columns=['imei_norm'])
            self._analyze_helper(cursor, tblname)
            return num_records

    def _populate_new_notifications_lists(self, executor):
        """Top-level job function to populate per-MNO notification lists."""
        with create_db_connection(self._config.db_config) as conn, CodeProfiler() as cp:
            # First generate the table of IMEIs to notify and commit so that other tables can see results
            with conn:
                self._run_intermediate_table_job(conn,
                                                 self._populate_new_notifications_imei_table,
                                                 description='IMEIS to notify')

            # Queue jobs to calculate the triplets to notify. This is done in parallel as this tends to be the
            # slowest part of the list generation process
            num_phys_shards = partition_utils.num_physical_imei_shards(conn)
            virt_imei_shard_ranges = partition_utils.virt_imei_shard_bounds(num_phys_shards)
            per_shard_jobs = {}
            for shard_num, shard_range in enumerate(virt_imei_shard_ranges, start=1):
                virt_imei_range_start, virt_imei_range_end = shard_range
                self._queue_intermediate_table_job(executor,
                                                   per_shard_jobs,
                                                   partial(self._populate_new_notifications_triplets_single_shard,
                                                           virt_imei_range_start,
                                                           virt_imei_range_end),
                                                   'triplets to notify (shard {shard_num} of {num_phys_shards})'
                                                   .format(shard_num=shard_num, num_phys_shards=num_phys_shards))

            self._wait_for_futures(per_shard_jobs)

            # Now that we have calculate all the prerequisites, populate each MNO's notification list in parallel
            per_mno_jobs = {}
            for op in self._operators:
                self._queue_intermediate_table_job(executor,
                                                   per_mno_jobs,
                                                   partial(self._populate_new_notifications_list, op.id),
                                                   'per-MNO notifications for {0}'.format(op.id))
            self._wait_for_futures(per_mno_jobs)

            # ANALYZE parent table, which analyzes children as well
            with conn.cursor() as cursor:
                self._analyze_helper(cursor, self._notifications_lists_new_tblname)

        return -1, cp.duration

    def _populate_new_notifications_imei_table(self, conn):
        """Function to populate the new table of unique IMEIs to notify."""
        with CodeProfiler() as cp:
            tblname = self._notifications_imei_new_tblname
            num_records = self._populate_new_blacklist_or_notifications_imei_table(conn, tblname, is_blacklist=False)

        return num_records, cp.duration

    @property
    def _home_network_query(self):
        """Property generating a query to find the home network in the MCC-MNC table."""
        return sql.SQL("""SELECT operator_id
                            FROM {mcc_mnc_table}
                           WHERE imsi LIKE mcc_mnc_pattern
                           LIMIT 1
                       """).format(mcc_mnc_table=sql.Identifier(self._mnc_mcc_new_tblname))

    def _populate_new_notifications_triplets_single_shard(self, virt_imei_range_start, virt_imei_range_end, executor):
        """Function to create and generate the new table of unique IMEI/IMSI/MSISDN notifications triplets."""
        with create_db_connection(self._config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            notifications_imeis_shard = \
                partition_utils.imei_shard_name(base_name=self._notifications_imei_new_tblname,
                                                virt_imei_range_start=virt_imei_range_start,
                                                virt_imei_range_end=virt_imei_range_end)
            notifications_triplets_shard = \
                partition_utils.imei_shard_name(base_name=self._notifications_triplets_new_tblname,
                                                virt_imei_range_start=virt_imei_range_start,
                                                virt_imei_range_end=virt_imei_range_end)
            pairing_list_shard = partition_utils.imei_shard_name(base_name='historic_pairing_list',
                                                                 virt_imei_range_start=virt_imei_range_start,
                                                                 virt_imei_range_end=virt_imei_range_end)

            # Note: IMSI can't be NULL in pairing list as it is constrained in table DDL
            #
            # 20170504: Re: discussion with Daniel it was decided to exclude triplets with a NULL IMSI or MSISDN
            #           from notifications. Without an MSISDN there is no easy way for an operator to contact the
            #           subscriber. Without an IMSI we can't determine accurately whether they were already paired.
            #           We expect both NULL IMSI and MSISDN to be transient, weird events that do not consistently
            #           happen for the same subscriber. Therefore they will be notified anyway based on the non-NULL
            #           IMSI/MSISDN that is expected to be seen either on the same day or at some point during the
            #           configured lookback window.

            # check weather to notify the IMSI change or not, default behavior is to not however can be altered
            if self._notify_imsi_change:
                imsi_change_filter = sql.SQL('AND imsi = network_triplets.imsi')
            else:
                imsi_change_filter = sql.SQL('AND imsi = network_triplets.imsi OR msisdn = network_triplets.msisdn')

            query = sql.SQL(
                """INSERT INTO {notifications_triplets_shard}(imei_norm,
                                                              virt_imei_shard,
                                                              imsi,
                                                              msisdn,
                                                              block_date,
                                                              reasons,
                                                              is_valid,
                                                              amnesty_granted,
                                                              imei_norm_with_check_digit,
                                                              home_operator,
                                                              fallback_operators)
                        SELECT imei_norm,
                               FIRST(network_triplets.virt_imei_shard),
                               imsi,
                               msisdn,
                               FIRST(block_date),
                               FIRST(reasons),
                               FIRST(is_valid),
                               FIRST(amnesty_granted),
                               FIRST(imei_norm_with_check_digit),
                               FIRST(home_network_tbl.operator_id) AS home_operator,
                               array_agg(DISTINCT network_triplets.operator_id)
                                         filter(WHERE network_triplets.operator_id IS NOT NULL) AS fallback_operators
                          FROM {notifications_imeis_shard}
                    INNER JOIN monthly_network_triplets_per_mno network_triplets
                         USING (imei_norm)
                     LEFT JOIN LATERAL ({home_network_query}) home_network_tbl
                               ON TRUE
                         WHERE NOT EXISTS (SELECT 1
                                             FROM {pairing_list_shard}
                                            WHERE end_date IS NULL
                                              AND imei_norm = network_triplets.imei_norm
                                                  {notify_filter})
                           AND imei_norm IS NOT NULL
                           AND imsi IS NOT NULL
                           AND msisdn IS NOT NULL
                           AND last_seen >= %(lookback_start_date)s
                           AND first_seen < %(lookback_end_date)s
                           AND network_triplets.virt_imei_shard >= %(virt_imei_range_start)s
                           AND network_triplets.virt_imei_shard < %(virt_imei_range_end)s
                      GROUP BY imei_norm,
                               imsi,
                               msisdn
                """).format(notifications_triplets_shard=sql.Identifier(notifications_triplets_shard),  # noqa: Q447
                            notifications_imeis_shard=sql.Identifier(notifications_imeis_shard),
                            pairing_list_shard=sql.Identifier(pairing_list_shard),
                            home_network_query=self._home_network_query,
                            notify_filter=imsi_change_filter)

            lookback_end_date = compute_analysis_end_date(conn, self._curr_date)
            lookback_start_date = lookback_end_date - datetime.timedelta(days=self._lookback_days)
            logger = logging.getLogger('dirbs.listgen')
            log_analysis_window(logger, lookback_start_date, lookback_end_date,
                                start_message='Notifications lists using lookback window')
            cursor.execute(query, {'lookback_start_date': lookback_start_date,
                                   'lookback_end_date': lookback_end_date,
                                   'virt_imei_range_start': virt_imei_range_start,
                                   'virt_imei_range_end': virt_imei_range_end})
            num_records = cursor.rowcount
            self._add_pk(conn, tblname=notifications_triplets_shard, pk_columns=['imei_norm', 'imsi', 'msisdn'])

        return num_records, cp.duration

    def _populate_new_notifications_list(self, operator_id, executor):
        """Function to allocate new notifications triplets to new per-MNO notifications list tables."""
        with create_db_connection(self._config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            operator_partition_name = self._notifications_lists_new_part_tblname(operator_id)
            notifications_triplets_tblname = self._notifications_triplets_new_tblname
            cursor.execute(sql.SQL("""INSERT INTO {operator_partition_name}(imei_norm,
                                                                            virt_imei_shard,
                                                                            imsi,
                                                                            msisdn,
                                                                            block_date,
                                                                            reasons,
                                                                            operator_id,
                                                                            is_valid,
                                                                            amnesty_granted,
                                                                            imei_norm_with_check_digit)
                                           SELECT imei_norm,
                                                  virt_imei_shard,
                                                  imsi,
                                                  msisdn,
                                                  block_date,
                                                  reasons,
                                                  %s,
                                                  is_valid,
                                                  amnesty_granted,
                                                  imei_norm_with_check_digit
                                             FROM {notifications_tblname}
                                            WHERE home_operator = %s
                                               OR (home_operator IS NULL
                                              AND %s = ANY(fallback_operators))
                                   """).format(operator_partition_name=sql.Identifier(operator_partition_name),
                                               notifications_tblname=sql.Identifier(notifications_triplets_tblname)),
                           [operator_id, operator_id, operator_id])
            num_records = cursor.rowcount
            self._add_pk(conn, tblname=operator_partition_name, pk_columns=['imei_norm', 'imsi', 'msisdn'])

        return num_records, cp.duration

    def _populate_new_exceptions_lists(self, executor):
        """Top-level job function to create the new per-MNO pairings lists."""
        with create_db_connection(self._config.db_config) as conn, CodeProfiler() as cp:
            # Commit so that queued jobs can see results
            with conn:
                # First generate the table of IMEI/IMSIs to pair, along with home network.
                self._run_intermediate_table_job(conn,
                                                 self._populate_new_pairings_imei_imsi_table,
                                                 description='IMEI/IMSI pairings')

            # Setup jobs to create per-MNO partitions that allocate notifications from the triplets to notify table
            per_mno_jobs = {}
            for op in self._operators:
                self._queue_intermediate_table_job(executor,
                                                   per_mno_jobs,
                                                   partial(self._populate_new_exceptions_list, op.id),
                                                   'per-MNO exceptions for {0}'.format(op.id))
            self._wait_for_futures(per_mno_jobs)

            # ANALYZE parent table, which analyzes children as well
            with conn.cursor() as cursor:
                self._analyze_helper(cursor, self._exceptions_lists_new_tblname)

        return -1, cp.duration

    def _populate_new_pairings_imei_imsi_table(self, conn):
        """Function to populate the pairings list IMEI/IMSI temp table for this run."""
        with conn.cursor() as cursor, CodeProfiler() as cp:
            tblname = self._pairings_imei_imsi_new_tblname
            # Queries to generate is_valid and imei_norm_with_check_digit which do nothing if not required
            is_valid_query, imei_norm_with_check_digit_query = self._is_valid_and_check_digit_queries

            if self._restrict_exceptions_list:
                is_blacklisted_query = \
                    sql.SQL("""SELECT EXISTS (SELECT 1
                                                FROM {blacklist_tblname}
                                               WHERE imei_norm = pl.imei_norm
                                                 AND virt_imei_shard
                                                        = calc_virt_imei_shard(pl.imei_norm)) AS is_blacklisted
                            """).format(blacklist_tblname=sql.Identifier(self._blacklist_new_tblname))
            else:
                is_blacklisted_query = sql.SQL('SELECT NULL::BOOLEAN AS is_blacklisted')

            if not self._include_barred_imeis:
                is_barred_query = \
                    sql.SQL("""SELECT EXISTS (SELECT 1
                                                FROM barred_list
                                               WHERE imei_norm = pl.imei_norm
                                                 AND virt_imei_shard
                                                        = calc_virt_imei_shard(pl.imei_norm)) AS is_barred""")

                have_barred_tac_query = \
                    sql.SQL("""SELECT EXISTS (SELECT 1
                                                FROM barred_tac_list
                                               WHERE tac = LEFT(pl.imei_norm, 8)) AS have_barred_tac""")
            else:
                is_barred_query = sql.SQL('SELECT NULL::BOOLEAN AS is_barred')
                have_barred_tac_query = sql.SQL('SELECT NULL::BOOLEAN AS have_barred_tac')

            query = sql.SQL("""INSERT INTO {tblname}(imei_norm,
                                                     virt_imei_shard,
                                                     imsi,
                                                     is_valid,
                                                     imei_norm_with_check_digit,
                                                     home_operator,
                                                     is_blacklisted,
                                                     is_barred,
                                                     have_barred_tac,
                                                     msisdn)
                                    SELECT imei_norm,
                                           virt_imei_shard,
                                           imsi,
                                           is_valid,
                                           imei_norm_with_check_digit,
                                           operator_id,
                                           is_blacklisted,
                                           is_barred,
                                           have_barred_tac,
                                           msisdn
                                      FROM pairing_list pl,
                                           LATERAL ({is_valid_query}) is_valid_tbl,
                                           LATERAL ({imei_norm_with_check_digit_query}) check_digit_tbl,
                                           LATERAL ({is_blacklisted_query}) is_blacklisted_tbl,
                                           LATERAL ({is_barred_query}) is_barred_tbl,
                                           LATERAL ({have_barred_tac_query}) have_barred_tac_tbl
                                 LEFT JOIN LATERAL ({home_network_query}) home_network_tbl
                                           ON TRUE
                            """).format(tblname=sql.Identifier(tblname),
                                        is_valid_query=is_valid_query,
                                        imei_norm_with_check_digit_query=imei_norm_with_check_digit_query,
                                        is_blacklisted_query=is_blacklisted_query,
                                        is_barred_query=is_barred_query,
                                        have_barred_tac_query=have_barred_tac_query,
                                        home_network_query=self._home_network_query)

            cursor.execute(query)
            num_records = cursor.rowcount
            self._add_pk(conn, tblname=tblname, pk_columns=['imei_norm', 'imsi', 'msisdn'])
            self._analyze_helper(cursor, tblname)

        return num_records, cp.duration

    def _populate_new_exceptions_list(self, operator_id, executor):
        """Function to populate the per-MNO exception lists based on home and fallback networks."""
        with create_db_connection(self._config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            operator_partition_name = self._exceptions_lists_new_part_tblname(operator_id)
            imei_imsi_pairings_tblname = self._pairings_imei_imsi_new_tblname
            # Insert all pairings where this MNO is the home operator or there is no home operator
            cursor.execute(sql.SQL("""INSERT INTO {operator_partition_name}(imei_norm,
                                                                            virt_imei_shard,
                                                                            imsi,
                                                                            operator_id,
                                                                            is_valid,
                                                                            imei_norm_with_check_digit,
                                                                            is_blacklisted,
                                                                            is_barred,
                                                                            have_barred_tac,
                                                                            msisdn)
                                           SELECT imei_norm,
                                                  virt_imei_shard,
                                                  imsi,
                                                  %s,
                                                  is_valid,
                                                  imei_norm_with_check_digit,
                                                  is_blacklisted,
                                                  is_barred,
                                                  have_barred_tac,
                                                  msisdn
                                             FROM {imei_imsi_pairings_tblname}
                                            WHERE home_operator = %s
                                               OR home_operator IS NULL
                                   """).format(operator_partition_name=sql.Identifier(operator_partition_name),
                                               imei_imsi_pairings_tblname=sql.Identifier(imei_imsi_pairings_tblname)),
                           [operator_id, operator_id])
            num_records = cursor.rowcount
            self._add_pk(conn, tblname=operator_partition_name, pk_columns=['imei_norm', 'imsi', 'msisdn'])

        return num_records, cp.duration

    def _create_intermediate_old_tables(self):
        """Creates tables containing the old version of the lists that we are comparing against."""
        with self._conn as conn, futures.ThreadPoolExecutor(max_workers=self._nworkers) as executor:
            # Create tables and commit transaction
            with conn:
                self._run_intermediate_table_job(conn, self._create_intermediate_old_tables_structure)

            futures_to_cb = {}
            self._queue_intermediate_table_job(executor,
                                               futures_to_cb,
                                               self._populate_old_blacklist,
                                               'old blacklist')
            for op in self._operators:
                self._queue_intermediate_table_job(executor,
                                                   futures_to_cb,
                                                   partial(self._populate_old_notifications_list, op.id),
                                                   'old notifications for {0}'.format(op.id))
                self._queue_intermediate_table_job(executor,
                                                   futures_to_cb,
                                                   partial(self._populate_old_exceptions_list, op.id),
                                                   'old exceptions for {0}'.format(op.id))

            self._wait_for_futures(futures_to_cb)

            # ANALYZE parent tables, which analyzes children as well
            with conn.cursor() as cursor:
                self._analyze_helper(cursor, self._exceptions_lists_old_tblname)
                self._analyze_helper(cursor, self._notifications_lists_old_tblname)

    def _create_intermediate_old_tables_structure(self, conn):
        """Create table structure for old tables used to create deltas."""
        table_names = []
        with conn.cursor() as cursor, CodeProfiler() as cp:
            tblname = self._blacklist_old_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                        imei_norm       TEXT NOT NULL,
                                        virt_imei_shard SMALLINT NOT NULL,
                                        block_date      DATE NOT NULL,
                                        reasons         TEXT[] NOT NULL
                                      ) PARTITION BY RANGE (virt_imei_shard)
                                   """).format(sql.Identifier(tblname)))
            partition_utils.create_imei_shard_partitions(conn, tbl_name=tblname, unlogged=True)
            table_names.append(tblname)

            tblname = self._notifications_lists_old_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                        operator_id     TEXT NOT NULL,
                                        imei_norm       TEXT NOT NULL,
                                        virt_imei_shard SMALLINT NOT NULL,
                                        imsi            TEXT NOT NULL,
                                        msisdn          TEXT NOT NULL,
                                        block_date      DATE NOT NULL,
                                        reasons         TEXT[] NOT NULL,
                                        amnesty_granted BOOLEAN
                                      ) PARTITION BY LIST (operator_id)
                                   """).format(sql.Identifier(tblname)))
            table_names.append(tblname)
            self._create_operator_partitions(conn,
                                             parent_tbl_name=tblname,
                                             child_name_fn=self._notifications_lists_old_part_tblname,
                                             is_unlogged=True)

            tblname = self._exceptions_lists_old_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                        operator_id     TEXT NOT NULL,
                                        imei_norm       TEXT NOT NULL,
                                        virt_imei_shard SMALLINT NOT NULL,
                                        imsi            TEXT NOT NULL,
                                        msisdn          TEXT NOT NULL
                                      ) PARTITION BY LIST (operator_id)
                                   """).format(sql.Identifier(tblname)))
            table_names.append(tblname)
            self._create_operator_partitions(conn,
                                             parent_tbl_name=tblname,
                                             child_name_fn=self._exceptions_lists_old_part_tblname,
                                             is_unlogged=True)

        self._intermediate_table_names.extend(table_names)
        return -1, cp.duration

    def _populate_old_blacklist(self, executor):
        """Function to populate the old blacklist."""
        with create_db_connection(self._config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            tblname = self._blacklist_old_tblname
            cursor.execute(sql.SQL("""INSERT INTO {0}(imei_norm, virt_imei_shard, block_date, reasons)
                                           SELECT imei_norm, virt_imei_shard, block_date, reasons
                                             FROM gen_blacklist()
                                   """).format(sql.Identifier(tblname)))
            num_records = cursor.rowcount
            self._add_pk(conn, tblname=tblname, pk_columns=['imei_norm'])
            self._analyze_helper(cursor, tblname)

        return num_records, cp.duration

    def _populate_old_exceptions_list(self, operator_id, executor):
        """Function to populate the old exceptions list for a given operator id."""
        with create_db_connection(self._config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            tblname = self._exceptions_lists_old_part_tblname(operator_id)
            cursor.execute(sql.SQL("""INSERT INTO {0}(operator_id, imei_norm, virt_imei_shard, imsi, msisdn)
                                           SELECT %s, imei_norm, virt_imei_shard, imsi, msisdn
                                             FROM gen_exceptions_list(%s)
                                   """).format(sql.Identifier(tblname)),
                           [operator_id, operator_id])
            num_records = cursor.rowcount
            self._add_pk(conn, tblname=tblname, pk_columns=['imei_norm', 'imsi', 'msisdn'])

        return num_records, cp.duration

    def _populate_old_notifications_list(self, operator_id, executor):
        """Function to populate the old notifications list for a given operator id."""
        with create_db_connection(self._config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            tblname = self._notifications_lists_old_part_tblname(operator_id)
            cursor.execute(sql.SQL("""INSERT INTO {0}(operator_id,
                                                      imei_norm,
                                                      virt_imei_shard,
                                                      imsi,
                                                      msisdn,
                                                      block_date,
                                                      reasons,
                                                      amnesty_granted)
                                           SELECT %s, imei_norm, virt_imei_shard, imsi, msisdn, block_date, reasons,
                                                  amnesty_granted
                                             FROM gen_notifications_list(%s)
                                   """).format(sql.Identifier(tblname)),
                           [operator_id, operator_id])
            num_records = cursor.rowcount
            self._add_pk(conn, tblname=tblname, pk_columns=['imei_norm', 'imsi', 'msisdn'])

        return num_records, cp.duration

    def _create_intermediate_delta_tables(self):
        """Creates tables containing the deltas between the old and new lists."""
        with self._conn as conn:
            self._run_intermediate_table_job(conn, self._create_intermediate_delta_tables_structure)

        with self._conn as conn, futures.ThreadPoolExecutor(max_workers=self._nworkers) as executor:
            futures_to_cb = {}
            self._queue_intermediate_table_job(executor,
                                               futures_to_cb,
                                               self._populate_delta_blacklist,
                                               'delta blacklist')
            for op in self._operators:
                self._queue_intermediate_table_job(executor,
                                                   futures_to_cb,
                                                   partial(self._populate_delta_notifications_list, op.id),
                                                   'delta notifications for {0}'.format(op.id))
                self._queue_intermediate_table_job(executor,
                                                   futures_to_cb,
                                                   partial(self._populate_delta_exceptions_list, op.id),
                                                   'delta exceptions for {0}'.format(op.id))
            self._wait_for_futures(futures_to_cb)

            # ANALYZE parent tables, which analyzes children as well
            with conn.cursor() as cursor:
                self._analyze_helper(cursor, self._notifications_lists_delta_tblname)
                self._analyze_helper(cursor, self._exceptions_lists_delta_tblname)

    def _create_intermediate_delta_tables_structure(self, conn):
        """Create table structure for delta tables used to update stored lists."""
        table_names = []
        with conn.cursor() as cursor, CodeProfiler() as cp:
            tblname = self._blacklist_delta_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                          LIKE {blacklist_delta_tbl} INCLUDING DEFAULTS
                                                                     INCLUDING IDENTITY
                                                                     INCLUDING CONSTRAINTS
                                                                     INCLUDING STORAGE
                                                                     INCLUDING COMMENTS
                                      ) PARTITION BY RANGE (virt_imei_shard)
                                   """).format(sql.Identifier(tblname),
                                               blacklist_delta_tbl=sql.Identifier(self._blacklist_tblname)))
            partition_utils.create_imei_shard_partitions(conn, tbl_name=tblname, unlogged=True)
            table_names.append(tblname)

            tblname = self._notifications_lists_delta_tblname
            notifications_delta_tbl = sql.Identifier(self._notifications_lists_tblname)
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                          LIKE {notifications_delta_tbl} INCLUDING DEFAULTS
                                                                         INCLUDING IDENTITY
                                                                         INCLUDING CONSTRAINTS
                                                                         INCLUDING STORAGE
                                                                         INCLUDING COMMENTS
                                      ) PARTITION BY LIST (operator_id)
                                   """).format(sql.Identifier(tblname),
                                               notifications_delta_tbl=notifications_delta_tbl))
            table_names.append(tblname)
            self._create_operator_partitions(conn,
                                             parent_tbl_name=tblname,
                                             child_name_fn=self._notifications_lists_delta_part_tblname,
                                             is_unlogged=True)

            tblname = self._exceptions_lists_delta_tblname
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                          LIKE {exceptions_delta_tbl} INCLUDING DEFAULTS
                                                                      INCLUDING IDENTITY
                                                                      INCLUDING CONSTRAINTS
                                                                      INCLUDING STORAGE
                                                                      INCLUDING COMMENTS
                                      ) PARTITION BY LIST (operator_id)
                                   """).format(sql.Identifier(tblname),
                                               exceptions_delta_tbl=sql.Identifier(self._exceptions_lists_tblname)))
            table_names.append(tblname)
            self._create_operator_partitions(conn,
                                             parent_tbl_name=tblname,
                                             child_name_fn=self._exceptions_lists_delta_part_tblname,
                                             is_unlogged=True)

        self._intermediate_table_names.extend(table_names)
        return -1, cp.duration

    def _populate_delta_blacklist(self, executor):
        """Function to populate the old blacklist."""
        with create_db_connection(self._config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            delta_tbl_name = self._blacklist_delta_tblname
            delta_tbl = sql.Identifier(delta_tbl_name)
            old_tbl = sql.Identifier(self._blacklist_old_tblname)
            new_tbl = sql.Identifier(self._blacklist_new_tblname)
            # First, generate a delta for any 'blocked' records (new adds)
            cursor.execute(sql.SQL("""INSERT INTO {delta_tbl}(imei_norm,
                                                              virt_imei_shard,
                                                              block_date,
                                                              reasons,
                                                              start_run_id,
                                                              end_run_id,
                                                              delta_reason)
                                           SELECT imei_norm, virt_imei_shard, block_date, reasons, %s, NULL, 'blocked'
                                             FROM {new_tbl} nt
                                            WHERE NOT EXISTS(SELECT 1
                                                               FROM {old_tbl}
                                                              WHERE imei_norm = nt.imei_norm
                                                                AND virt_imei_shard = nt.virt_imei_shard)
                                   """).format(delta_tbl=delta_tbl, old_tbl=old_tbl, new_tbl=new_tbl),  # noqa: Q449
                           [self._run_id])
            num_records = cursor.rowcount
            # Next, generate a delta for any 'unblocked' records (new removals) (table direction flipped)
            cursor.execute(sql.SQL("""INSERT INTO {delta_tbl}(imei_norm,
                                                              virt_imei_shard,
                                                              block_date,
                                                              reasons,
                                                              start_run_id,
                                                              end_run_id,
                                                              delta_reason)
                                           SELECT imei_norm, virt_imei_shard, block_date,
                                                  reasons, %s, NULL, 'unblocked'
                                             FROM {old_tbl} ot
                                            WHERE NOT EXISTS(SELECT 1
                                                               FROM {new_tbl}
                                                              WHERE imei_norm = ot.imei_norm
                                                                AND virt_imei_shard = ot.virt_imei_shard)
                                   """).format(delta_tbl=delta_tbl, old_tbl=old_tbl, new_tbl=new_tbl),  # noqa: Q449
                           [self._run_id])
            num_records += cursor.rowcount
            # Next, generate a delta for any 'changed' records
            cursor.execute(sql.SQL("""INSERT INTO {delta_tbl}(imei_norm,
                                                              virt_imei_shard,
                                                              block_date,
                                                              reasons,
                                                              start_run_id,
                                                              end_run_id,
                                                              delta_reason)
                                           SELECT nt.imei_norm, nt.virt_imei_shard, nt.block_date,
                                                  nt.reasons, %s, NULL, 'changed'
                                             FROM {new_tbl} nt
                                             JOIN {old_tbl} ot
                                            USING (imei_norm)
                                            WHERE ot.block_date <> nt.block_date
                                               OR ot.reasons <> nt.reasons
                                   """).format(delta_tbl=delta_tbl, old_tbl=old_tbl, new_tbl=new_tbl),
                           [self._run_id])
            num_records += cursor.rowcount
            self._analyze_helper(cursor, delta_tbl_name)

        return num_records, cp.duration

    def _populate_delta_exceptions_list(self, operator_id, executor):
        """Function to populate the old exceptions list for a given operator id."""
        with create_db_connection(self._config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            delta_tbl_name = self._exceptions_lists_delta_part_tblname(operator_id)
            delta_tbl = sql.Identifier(delta_tbl_name)
            old_tbl = sql.Identifier(self._exceptions_lists_old_part_tblname(operator_id))
            new_tbl = sql.Identifier(self._exceptions_lists_new_part_tblname(operator_id))
            # First, generate a delta for any 'added' records (new adds)
            cursor.execute(sql.SQL("""INSERT INTO {delta_tbl}(operator_id,
                                                              imei_norm,
                                                              virt_imei_shard,
                                                              imsi,
                                                              start_run_id,
                                                              end_run_id,
                                                              delta_reason,
                                                              msisdn)
                                           SELECT %s, imei_norm, virt_imei_shard, imsi, %s, NULL, 'added', msisdn
                                             FROM {new_tbl} nt
                                            WHERE NOT EXISTS(SELECT 1
                                                               FROM {old_tbl}
                                                              WHERE imei_norm = nt.imei_norm
                                                                AND virt_imei_shard = nt.virt_imei_shard
                                                                AND imsi = nt.imsi
                                                                AND msisdn = nt.msisdn)
                                   """).format(delta_tbl=delta_tbl, old_tbl=old_tbl, new_tbl=new_tbl),  # noqa: Q449
                           [operator_id, self._run_id])
            num_records = cursor.rowcount
            # Next, generate a delta for any 'removed' records (new removals) (table direction flipped)
            cursor.execute(sql.SQL("""INSERT INTO {delta_tbl}(operator_id,
                                                              imei_norm,
                                                              virt_imei_shard,
                                                              imsi,
                                                              start_run_id,
                                                              end_run_id,
                                                              delta_reason,
                                                              msisdn)
                                           SELECT %s, imei_norm, virt_imei_shard, imsi, %s, NULL, 'removed', msisdn
                                             FROM {old_tbl} ot
                                            WHERE NOT EXISTS(SELECT 1
                                                               FROM {new_tbl}
                                                              WHERE imei_norm = ot.imei_norm
                                                                AND virt_imei_shard = ot.virt_imei_shard
                                                                AND imsi = ot.imsi
                                                                AND msisdn = ot.msisdn)
                                   """).format(delta_tbl=delta_tbl, old_tbl=old_tbl, new_tbl=new_tbl),  # noqa: Q449
                           [operator_id, self._run_id])
            num_records += cursor.rowcount

        return num_records, cp.duration

    def _populate_delta_notifications_list(self, operator_id, executor):
        """Function to populate the old notifications list for a given operator id."""
        with create_db_connection(self._config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            delta_tbl_name = self._notifications_lists_delta_part_tblname(operator_id)
            delta_tbl = sql.Identifier(delta_tbl_name)
            old_tbl = sql.Identifier(self._notifications_lists_old_part_tblname(operator_id))
            new_tbl = sql.Identifier(self._notifications_lists_new_part_tblname(operator_id))
            # First, generate a delta for any 'new' triplets (new adds)
            cursor.execute(sql.SQL("""INSERT INTO {delta_tbl}(operator_id,
                                                              imei_norm,
                                                              virt_imei_shard,
                                                              imsi,
                                                              msisdn,
                                                              block_date,
                                                              reasons,
                                                              amnesty_granted,
                                                              start_run_id,
                                                              end_run_id,
                                                              delta_reason)
                                           SELECT %s, imei_norm, virt_imei_shard, imsi, msisdn, block_date, reasons,
                                                  amnesty_granted, %s, NULL, 'new'
                                             FROM {new_tbl} nt
                                            WHERE NOT EXISTS(SELECT 1
                                                               FROM {old_tbl}
                                                              WHERE imei_norm = nt.imei_norm
                                                                AND virt_imei_shard = nt.virt_imei_shard
                                                                AND imsi = nt.imsi
                                                                AND msisdn = nt.msisdn)
                                   """).format(delta_tbl=delta_tbl, old_tbl=old_tbl, new_tbl=new_tbl),  # noqa: Q449
                           [operator_id, self._run_id])
            num_records = cursor.rowcount

            # weather to notify the IMSI change or not, default behaviour is to not however it can be
            # altered in the config file variable notify_imsi_change
            if self._notify_imsi_change:
                imsi_change_filter = sql.SQL('AND imsi = ot.imsi')
            else:
                imsi_change_filter = sql.SQL('AND imsi = ot.imsi OR msisdn = ot.msisdn')

            # Next, generate a delta for any 'resolved' and 'blacklisted' triplets (new removals)
            blacklist_tbl = sql.Identifier(self._blacklist_new_tblname)
            notifications_imei_tbl = sql.Identifier(self._notifications_imei_new_tblname)
            pairings_tbl = sql.Identifier(self._pairings_imei_imsi_new_tblname)
            cursor.execute(sql.SQL(
                """INSERT INTO {delta_tbl}(operator_id,
                                           imei_norm,
                                           virt_imei_shard,
                                           imsi,
                                           msisdn,
                                           block_date,
                                           reasons,
                                           amnesty_granted,
                                           start_run_id,
                                           end_run_id,
                                           delta_reason)
                        SELECT %s,
                               imei_norm,
                               virt_imei_shard,
                               imsi,
                               msisdn,
                               block_date,
                               reasons,
                               amnesty_granted,
                               %s,
                               NULL,
                               CASE WHEN is_paired THEN 'resolved' -- If paired, definitely resolved
                                    WHEN is_blocked THEN 'blacklisted' -- If blacklisted, definitely blacklisted
                                    WHEN is_notified THEN 'no_longer_seen' -- If IMEI still notified, no longer seen
                                    ELSE 'resolved' -- If IMEI no longer notified, is resolved
                               END
                          FROM {old_tbl} ot,
                               LATERAL (SELECT EXISTS (SELECT 1
                                                         FROM {blacklist_tbl}
                                                        WHERE imei_norm = ot.imei_norm
                                                          AND virt_imei_shard = ot.virt_imei_shard) AS is_blocked
                                                      ) blocked_tbl,
                               LATERAL (SELECT EXISTS (SELECT 1
                                                         FROM {notifications_imei_tbl}
                                                        WHERE imei_norm = ot.imei_norm
                                                          AND virt_imei_shard = ot.virt_imei_shard) AS is_notified
                                                      ) notify_tbl,
                               LATERAL (SELECT EXISTS (SELECT 1
                                                         FROM {pairings_tbl}
                                                        WHERE imei_norm = ot.imei_norm
                                                          AND virt_imei_shard = ot.virt_imei_shard
                                                              {notify_filter}) AS is_paired) pairing_tbl
                         WHERE NOT EXISTS(SELECT 1
                                            FROM {new_tbl}
                                           WHERE imei_norm = ot.imei_norm
                                             AND virt_imei_shard = ot.virt_imei_shard
                                             AND imsi = ot.imsi
                                             AND msisdn = ot.msisdn)
                                   """).format(delta_tbl=delta_tbl, old_tbl=old_tbl,   # noqa: Q447, Q449
                                               new_tbl=new_tbl,
                                               blacklist_tbl=blacklist_tbl,
                                               notifications_imei_tbl=notifications_imei_tbl,
                                               pairings_tbl=pairings_tbl,
                                               notify_filter=imsi_change_filter),
                [operator_id, self._run_id])
            num_records += cursor.rowcount
            # Finally, generate a delta for any triplets where the reasons/date have changed
            cursor.execute(sql.SQL("""INSERT INTO {delta_tbl}(operator_id,
                                                              imei_norm,
                                                              virt_imei_shard,
                                                              imsi,
                                                              msisdn,
                                                              block_date,
                                                              reasons,
                                                              amnesty_granted,
                                                              start_run_id,
                                                              end_run_id,
                                                              delta_reason)
                                           SELECT %s,
                                                  nt.imei_norm,
                                                  nt.virt_imei_shard,
                                                  nt.imsi,
                                                  nt.msisdn,
                                                  nt.block_date,
                                                  nt.reasons,
                                                  nt.amnesty_granted, %s,
                                                  NULL,
                                                  'changed'
                                             FROM {new_tbl} nt
                                             JOIN {old_tbl} ot
                                            USING (imei_norm, imsi, msisdn)
                                            WHERE ot.block_date <> nt.block_date
                                               OR ot.reasons <> nt.reasons
                                   """).format(delta_tbl=delta_tbl, old_tbl=old_tbl, new_tbl=new_tbl),
                           [operator_id, self._run_id])
            num_records += cursor.rowcount

        return num_records, cp.duration

    def _store_list_deltas(self):
        """Write the delta lists to the real tables and update the end dates of any outdated records."""
        # Create tables and commit immediately
        with self._conn as conn:
            self._run_intermediate_table_job(conn, self._create_missing_delta_storage_partitions)

        with self._conn as conn, futures.ThreadPoolExecutor(max_workers=self._nworkers) as executor:
            futures_to_cb = {}
            self._queue_delta_table_job(executor,
                                        futures_to_cb,
                                        self._store_blacklist_delta,
                                        'blacklist')
            for op in self._operators:
                self._queue_delta_table_job(executor,
                                            futures_to_cb,
                                            partial(self._store_notifications_list_delta, op.id),
                                            'notifications for {0}'.format(op.id))
                self._queue_delta_table_job(executor,
                                            futures_to_cb,
                                            partial(self._store_exceptions_list_delta, op.id),
                                            'exceptions for {0}'.format(op.id))

            self._wait_for_futures(futures_to_cb)

            # ANALYZE parent tables, which analyzes children as well
            with conn.cursor() as cursor:
                self._analyze_helper(cursor, self._notifications_lists_tblname)
                self._analyze_helper(cursor, self._exceptions_lists_tblname)

    def _create_missing_delta_storage_partitions(self, conn):
        """Loops through the operators and makes sure we have a notifications/exception partition available."""
        with CodeProfiler() as cp:
            # We deliberately don't create indexes straight away as the first time we create a partition it is
            # likely to be a bulk insert where indexes will hurt performance
            for parent_name, child_name_fn in [(self._notifications_lists_tblname,
                                                self._notifications_lists_part_tblname),
                                               (self._exceptions_lists_tblname, self._exceptions_lists_part_tblname)]:
                self._create_operator_partitions(conn,
                                                 parent_tbl_name=parent_name,
                                                 child_name_fn=child_name_fn,
                                                 is_unlogged=False,
                                                 allow_existing=True,
                                                 fillfactor=45)

        return -1, cp.duration

    def _create_missing_notifications_partition_indices(self, conn, operator_id):
        """Called to ensure that indices are created on notifications lists partitions."""
        with CodeProfiler() as cp:
            tbl_name = self._notifications_lists_part_tblname(operator_id)
            idx_metadata = [partition_utils.IndexMetadatum(idx_cols=['start_run_id']),
                            partition_utils.IndexMetadatum(idx_cols=['end_run_id']),
                            partition_utils.IndexMetadatum(idx_cols=['imei_norm', 'imsi', 'msisdn'],
                                                           is_unique=True,
                                                           partial_sql='WHERE end_run_id IS NULL')]
            partition_utils.add_indices(conn, tbl_name=tbl_name, idx_metadata=idx_metadata, if_not_exists=True)

        return -1, cp.duration

    def _create_missing_exceptions_partition_indices(self, conn, operator_id):
        """Called to ensure that indices are created on exceptions lists partitions."""
        with CodeProfiler() as cp:
            tbl_name = self._exceptions_lists_part_tblname(operator_id)
            idx_metadata = [partition_utils.IndexMetadatum(idx_cols=['start_run_id']),
                            partition_utils.IndexMetadatum(idx_cols=['end_run_id']),
                            partition_utils.IndexMetadatum(idx_cols=['imei_norm', 'imsi', 'msisdn'],
                                                           is_unique=True,
                                                           partial_sql='WHERE end_run_id IS NULL')]
            partition_utils.add_indices(conn, tbl_name=tbl_name, idx_metadata=idx_metadata, if_not_exists=True)

        return -1, cp.duration

    def _store_blacklist_delta(self, executor):
        """Job to update the blacklist from the computed delta table."""
        per_type_counts = {}
        with create_db_connection(self._config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            tbl = sql.Identifier(self._blacklist_tblname)
            delta_tbl = sql.Identifier(self._blacklist_delta_tblname)
            # We set the end_run_id on any current row where we have a change in the delta table
            cursor.execute(sql.SQL("""UPDATE {tbl} bl
                                         SET end_run_id = %s
                                       WHERE end_run_id IS NULL
                                         AND EXISTS (SELECT 1
                                                       FROM {delta_tbl}
                                                      WHERE imei_norm = bl.imei_norm)
                                   """).format(tbl=tbl, delta_tbl=delta_tbl),
                           [self._run_id])
            per_type_counts['invalidated'] = cursor.rowcount
            # Now we should be able to just insert the delta list into the blacklist
            cursor.execute(sql.SQL("""INSERT INTO {tbl}(imei_norm,
                                                        virt_imei_shard,
                                                        block_date,
                                                        reasons,
                                                        start_run_id,
                                                        end_run_id,
                                                        delta_reason)
                                           SELECT imei_norm,
                                                  virt_imei_shard,
                                                  block_date,
                                                  reasons,
                                                  start_run_id,
                                                  end_run_id,
                                                  delta_reason
                                             FROM {delta_tbl}
                                   """).format(tbl=tbl, delta_tbl=delta_tbl),
                           [self._run_id])
            per_type_counts['new'] = cursor.rowcount
            self._analyze_helper(cursor, self._blacklist_tblname)

        return per_type_counts, cp.duration

    def _store_notifications_list_delta(self, operator_id, executor):
        """Job to update a notifications_list partition from the computed delta table."""
        per_type_counts = {}
        with create_db_connection(self._config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            tbl = sql.Identifier(self._notifications_lists_part_tblname(operator_id))
            delta_tbl = sql.Identifier(self._notifications_lists_delta_part_tblname(operator_id))
            # We set the end_run_id on any current row where we have a change in the delta table
            cursor.execute(sql.SQL("""UPDATE {tbl} nl
                                         SET end_run_id = %s
                                       WHERE end_run_id IS NULL
                                         AND EXISTS (SELECT 1
                                                       FROM {delta_tbl}
                                                      WHERE imei_norm = nl.imei_norm
                                                        AND virt_imei_shard = nl.virt_imei_shard
                                                        AND imsi = nl.imsi
                                                        AND msisdn = nl.msisdn)
                                   """).format(tbl=tbl, delta_tbl=delta_tbl),
                           [self._run_id])
            per_type_counts['invalidated'] = cursor.rowcount
            # Now we should be able to just insert the delta list into the notifications list
            cursor.execute(sql.SQL("""INSERT INTO {tbl}(operator_id,
                                                        imei_norm,
                                                        virt_imei_shard,
                                                        imsi,
                                                        msisdn,
                                                        block_date,
                                                        reasons,
                                                        amnesty_granted,
                                                        start_run_id,
                                                        end_run_id,
                                                        delta_reason)
                                           SELECT operator_id,
                                                  imei_norm,
                                                  virt_imei_shard,
                                                  imsi,
                                                  msisdn,
                                                  block_date,
                                                  reasons,
                                                  amnesty_granted,
                                                  start_run_id,
                                                  end_run_id,
                                                  delta_reason
                                             FROM {delta_tbl}
                                   """).format(tbl=tbl, delta_tbl=delta_tbl),
                           [self._run_id])
            per_type_counts['new'] = cursor.rowcount
            self._create_missing_notifications_partition_indices(conn, operator_id)

        return per_type_counts, cp.duration

    def _store_exceptions_list_delta(self, operator_id, executor):
        """Job to update a exceptions_list partition from the computed delta table."""
        per_type_counts = {}
        with create_db_connection(self._config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            tbl = sql.Identifier(self._exceptions_lists_part_tblname(operator_id))
            delta_tbl = sql.Identifier(self._exceptions_lists_delta_part_tblname(operator_id))
            # We set the end_run_id on any current row where we have a change in the delta table
            cursor.execute(sql.SQL("""UPDATE {tbl} el
                                         SET end_run_id = %s
                                       WHERE end_run_id IS NULL
                                         AND EXISTS (SELECT 1
                                                       FROM {delta_tbl}
                                                      WHERE imei_norm = el.imei_norm
                                                        AND virt_imei_shard = el.virt_imei_shard
                                                        AND imsi = el.imsi
                                                        AND msisdn = el.msisdn)
                                   """).format(tbl=tbl, delta_tbl=delta_tbl),
                           [self._run_id])
            per_type_counts['invalidated'] = cursor.rowcount
            # Now we should be able to just insert the delta list into the exceptions list
            cursor.execute(sql.SQL("""INSERT INTO {tbl}(operator_id,
                                                        imei_norm,
                                                        virt_imei_shard,
                                                        imsi,
                                                        start_run_id,
                                                        end_run_id,
                                                        delta_reason,
                                                        msisdn)
                                           SELECT operator_id, imei_norm, virt_imei_shard, imsi,
                                                  start_run_id, end_run_id, delta_reason, msisdn
                                             FROM {delta_tbl}
                                   """).format(tbl=tbl, delta_tbl=delta_tbl),
                           [self._run_id])
            per_type_counts['new'] = cursor.rowcount
            self._create_missing_exceptions_partition_indices(conn, operator_id)

        return per_type_counts, cp.duration

    def _write_csv_lists(self):  # noqa: C901
        """Write full CSV lists from the intermediate tables."""
        with futures.ProcessPoolExecutor(max_workers=self._nworkers) as executor:
            # We use ProcessPoolExecutor as these tasks are CPU intensive. This means that we do not have access
            # to the self._conn, self._metadata_conn and self._logger objects within the functions (they are removed
            # during pickling, see __getstate__)
            #
            futures_to_cb = {}
            md = defaultdict(list)
            self._queue_csv_writer_job(executor, futures_to_cb, self._write_delta_csv_blacklist, 'delta blacklist', md)
            for op in self._operators:
                for fn, desc in \
                        [(self._write_delta_csv_notifications_list, 'delta notifications list for operator {0}'),
                         (self._write_delta_csv_exceptions_list, 'delta exceptions list for operator {0}')]:
                    self._queue_csv_writer_job(executor, futures_to_cb, partial(fn, op.id), desc.format(op.id), md)

            if not self._no_full_lists:
                for fn, desc in [(self._write_full_csv_blacklist, 'full blacklist')]:
                    self._queue_csv_writer_job(executor, futures_to_cb, fn, desc, md)
                for op in self._operators:
                    for fn, desc in \
                            [(self._write_full_csv_notifications_list, 'full notifications list for operator {0}'),
                             (self._write_full_csv_exceptions_list, 'full exceptions list for operator {0}')]:
                        self._queue_csv_writer_job(executor, futures_to_cb, partial(fn, op.id), desc.format(op.id), md)

            self._wait_for_futures(futures_to_cb)

        metadata.add_optional_job_metadata(self._metadata_conn, 'dirbs-listgen', self._run_id, **md)

        self._logger.info('Zipping up lists...')
        with zipfile.ZipFile(os.path.join(self._output_dir, '{0}_blacklist.zip'.format(self._date_str)), 'w') as zf:
            for csv_path in glob.glob(os.path.join(self._output_dir, '{0}_blacklist*.csv'.format(self._date_str))):
                zf.write(csv_path, arcname=os.path.basename(csv_path))
                os.remove(csv_path)

        for op in self._operators:
            with zipfile.ZipFile(os.path.join(self._output_dir,
                                              '{0}_notifications_{1}.zip'.format(self._date_str, op.id)), 'w') as zf:
                for csv_path in glob.glob(os.path.join(self._output_dir,
                                                       '{0}_notifications_{1}*.csv'.format(self._date_str, op.id))):
                    zf.write(csv_path, arcname=os.path.basename(csv_path))
                    os.remove(csv_path)
            with zipfile.ZipFile(os.path.join(self._output_dir,
                                              '{0}_exceptions_{1}.zip'.format(self._date_str, op.id)), 'w') as zf:
                for csv_path in glob.glob(os.path.join(self._output_dir,
                                                       '{0}_exceptions_{1}*.csv'.format(self._date_str, op.id))):
                    zf.write(csv_path, arcname=os.path.basename(csv_path))
                    os.remove(csv_path)
        self._logger.info('Zipped up lists')

    def _queue_csv_writer_job(self, executor, futures_to_cb, fn, description, metadata_storage):
        """Function to queue a job to calculate an intermediate table."""
        self._logger.debug('Queueing CSV job to write out {0}...'.format(description))
        futures_to_cb[executor.submit(fn)] = partial(self._process_csv_writer_job, description, metadata_storage)

    def _process_csv_writer_job(self, description, metadata_storage, future):
        """Function to process the results of a job to calculate an intermediate table."""
        list_metadata, list_type, duration = future.result()
        self._logger.info('Wrote CSV file for {0} (duration {1:.3f}s)'.format(description, duration / 1000))
        if list_metadata is not None:
            # Delta lists return multiple metadata entries and need to append them
            if isinstance(list_metadata, list):
                metadata_storage[list_type].extend(list_metadata)
            else:
                metadata_storage[list_type].append(list_metadata)

    @property
    def _valid_filter_query(self):
        """Property generating a SQL fragment to filter intermediate tables by the is_valid flag if necessary."""
        if self._output_invalid_imeis:
            valid_filter_sql = sql.SQL('TRUE')
        else:
            valid_filter_sql = sql.SQL('is_valid IS TRUE')
        return valid_filter_sql

    @property
    def _output_imei_column(self):
        """Property generating a SQL fragment use the imei_norm_with_check_digit column as the IMEI if necessary."""
        if self._generate_check_digit:
            imei_col_name = sql.Identifier('imei_norm_with_check_digit')
        else:
            imei_col_name = sql.Identifier('imei_norm')
        return imei_col_name

    def _write_full_csv_blacklist(self):
        """Write full CSV blacklist from the intermediate table."""
        tblname = self._blacklist_new_tblname
        filename = os.path.join(self._output_dir, '{0}_blacklist.csv'.format(self._date_str))
        cursor_name = 'listgen_write_full_csv_blacklist'
        with create_db_connection(self._config.db_config) as conn, \
                conn.cursor(name=cursor_name) as cursor, open(filename, 'w') as csvfile, CodeProfiler() as cp:
            csv_writer = csv.DictWriter(csvfile, fieldnames=['imei', 'block_date', 'reasons'], extrasaction='ignore')
            csv_writer.writeheader()
            cursor.execute(sql.SQL("""SELECT {imei_col} AS imei,
                                             TO_CHAR(block_date, 'YYYYMMDD') AS block_date,
                                             array_to_string(reasons, '|') AS reasons
                                        FROM {tblname}
                                       WHERE {valid_filter}
                                   """).format(imei_col=self._output_imei_column,
                                               tblname=sql.Identifier(tblname),
                                               valid_filter=self._valid_filter_query))
            num_written_records = 0
            for row_data in cursor:
                csv_writer.writerow(row_data._asdict())
                num_written_records += 1
            num_records = self._get_total_record_count(conn, tblname)

        return self._gen_metadata_for_list(filename,
                                           num_records=num_records,
                                           num_written_records=num_written_records), 'blacklist', cp.duration

    def _write_delta_csv_blacklist(self):
        """Write delta CSV blacklist from the blacklist table."""
        cursor_name = 'listgen_write_delta_csv_blacklist'
        is_valid_query, imei_norm_with_check_digit_query = self._is_valid_and_check_digit_queries

        with create_db_connection(self._config.db_config) as conn, \
                conn.cursor(name=cursor_name) as cursor, contextlib.ExitStack() as stack, CodeProfiler() as cp:
            allowed_delta_reasons = self._allowed_delta_reasons(conn, 'blacklist')
            fnames = {r: os.path.join(self._output_dir, '{0}_blacklist_delta_{1:d}_{2:d}_{3}.csv'
                                      .format(self._date_str,
                                              self._base_run_id,
                                              self._run_id,
                                              r))
                      for r in allowed_delta_reasons}
            files = {r: stack.enter_context(open(fn, 'w', encoding='utf8')) for r, fn in fnames.items()}
            csv_writers = {r: csv.DictWriter(f,
                                             fieldnames=['imei', 'block_date', 'reasons'],
                                             extrasaction='ignore') for r, f in files.items()}
            for csv_writer in csv_writers.values():
                csv_writer.writeheader()

            cursor.execute(sql.SQL("""SELECT {imei_col} AS imei,
                                             TO_CHAR(block_date, 'YYYYMMDD') AS block_date,
                                             array_to_string(reasons, '|') AS reasons,
                                             delta_reason
                                        FROM (SELECT *
                                                FROM gen_delta_blacklist(%s),
                                                     LATERAL ({is_valid_query}) iv,
                                                     LATERAL ({imei_norm_with_check_digit_query}) cd) changes
                                       WHERE {valid_filter}
                                   """).format(is_valid_query=is_valid_query,
                                               imei_norm_with_check_digit_query=imei_norm_with_check_digit_query,
                                               valid_filter=self._valid_filter_query,
                                               imei_col=self._output_imei_column),
                           [self._base_run_id])

            metrics = {r: defaultdict(int) for r in allowed_delta_reasons}
            for row_data in cursor:
                row_data_dict = row_data._asdict()
                delta_reason = row_data_dict.pop('delta_reason')
                csv_writer = csv_writers[delta_reason]
                csv_writer.writerow(row_data_dict)
                metrics[delta_reason]['num_records'] += 1

        return [self._gen_metadata_for_list(
            fn, **metrics[r]) for r, fn in fnames.items()], 'blacklist_delta', cp.duration

    def _write_full_csv_notifications_list(self, operator_id):
        """Write full CSV, per-MNO notifications list for a given MNO from the intermediate tables."""
        tblname = self._notifications_lists_new_part_tblname(operator_id)
        filename = os.path.join(self._output_dir, '{0}_notifications_{1}.csv'.format(self._date_str, operator_id))
        cursor_name = 'listgen_write_full_csv_notifications_list_{0}'.format(operator_id)
        notification_list_columns, include_amnesty_column = self._notification_list_columns
        with create_db_connection(self._config.db_config) as conn, \
                conn.cursor(name=cursor_name) as cursor, open(filename, 'w') as csvfile, CodeProfiler() as cp:
            csv_writer = csv.DictWriter(csvfile,
                                        fieldnames=notification_list_columns,
                                        extrasaction='ignore')
            csv_writer.writeheader()
            cursor.execute(sql.SQL("""SELECT {imei_col} AS imei,
                                             imsi,
                                             msisdn,
                                             TO_CHAR(block_date, 'YYYYMMDD') AS block_date,
                                             array_to_string(reasons, '|') AS reasons
                                             {include_amnesty_column}
                                        FROM {tblname}
                                       WHERE {valid_filter}
                                   """).format(imei_col=self._output_imei_column,
                                               tblname=sql.Identifier(tblname),
                                               valid_filter=self._valid_filter_query,
                                               include_amnesty_column=include_amnesty_column))
            num_written_records = 0
            for row_data in cursor:
                csv_writer.writerow(row_data._asdict())
                num_written_records += 1
            num_records = self._get_total_record_count(conn, tblname)

        return self._gen_metadata_for_list(filename,
                                           num_records=num_records,
                                           num_written_records=num_written_records), 'notification_lists', cp.duration

    def _write_delta_csv_notifications_list(self, operator_id):
        """Write delta CSV notifications list for a particular operator."""
        cursor_name = 'listgen_write_delta_csv_notifications_list_{0}'.format(operator_id)
        is_valid_query, imei_norm_with_check_digit_query = self._is_valid_and_check_digit_queries
        notifications_list_columns, include_amnesty_column = self._notification_list_columns

        with create_db_connection(self._config.db_config) as conn, \
                conn.cursor(name=cursor_name) as cursor, contextlib.ExitStack() as stack, CodeProfiler() as cp:
            allowed_delta_reasons = self._allowed_delta_reasons(conn, 'notifications_lists')
            fnames = {r: os.path.join(self._output_dir, '{0}_notifications_{1}_delta_{2:d}_{3:d}_{4}.csv'
                                      .format(self._date_str,
                                              operator_id,
                                              self._base_run_id,
                                              self._run_id,
                                              r))
                      for r in allowed_delta_reasons}
            files = {r: stack.enter_context(open(fn, 'w', encoding='utf8')) for r, fn in fnames.items()}
            csv_writers = {r: csv.DictWriter(f,
                                             fieldnames=notifications_list_columns,
                                             extrasaction='ignore') for r, f in files.items()}
            for csv_writer in csv_writers.values():
                csv_writer.writeheader()

            cursor.execute(sql.SQL("""SELECT {imei_col} AS imei,
                                             imsi,
                                             msisdn,
                                             TO_CHAR(block_date, 'YYYYMMDD') AS block_date,
                                             array_to_string(reasons, '|') AS reasons,
                                             delta_reason
                                             {include_amnesty_column}
                                        FROM (SELECT *
                                                FROM gen_delta_notifications_list(%s, %s),
                                                     LATERAL ({is_valid_query}) iv,
                                                     LATERAL ({imei_norm_with_check_digit_query}) cd) changes
                                       WHERE {valid_filter}
                                   """).format(is_valid_query=is_valid_query,
                                               imei_norm_with_check_digit_query=imei_norm_with_check_digit_query,
                                               valid_filter=self._valid_filter_query,
                                               imei_col=self._output_imei_column,
                                               include_amnesty_column=include_amnesty_column),
                           [operator_id, self._base_run_id])

            metrics = {r: defaultdict(int) for r in allowed_delta_reasons}
            for row_data in cursor:
                row_data_dict = row_data._asdict()
                delta_reason = row_data_dict.pop('delta_reason')
                csv_writer = csv_writers[delta_reason]
                csv_writer.writerow(row_data_dict)
                metrics[delta_reason]['num_records'] += 1

        return [self._gen_metadata_for_list(
            fn, **metrics[r]) for r, fn in fnames.items()], 'notifications_lists_delta', cp.duration

    def _write_full_csv_exceptions_list(self, operator_id):
        """Write full CSV, per-MNO exceptions list for a given MNO from the intermediate tables."""
        tblname = self._exceptions_lists_new_part_tblname(operator_id)
        filename = os.path.join(self._output_dir, '{0}_exceptions_{1}.csv'.format(self._date_str, operator_id))
        cursor_name = 'listgen_write_full_csv_exceptions_{0}'.format(operator_id)
        with create_db_connection(self._config.db_config) as conn, \
                conn.cursor(name=cursor_name) as cursor, open(filename, 'w') as csvfile, CodeProfiler() as cp:
            csv_writer = csv.DictWriter(csvfile, fieldnames=['imei', 'imsi', 'msisdn'], extrasaction='ignore')
            csv_writer.writeheader()
            cursor.execute(sql.SQL("""SELECT {imei_col} AS imei, imsi, msisdn
                                        FROM {tblname}
                                       WHERE {valid_filter}
                                         AND {restrict_pairings_filter}
                                         AND {barred_pairing_filter}
                                   """).format(imei_col=self._output_imei_column,
                                               tblname=sql.Identifier(tblname),
                                               valid_filter=self._valid_filter_query,
                                               restrict_pairings_filter=self._blacklisted_pairings_filter_query,
                                               barred_pairing_filter=self._barred_pairings_filter_query))
            num_written_records = 0
            for row_data in cursor:
                csv_writer.writerow(row_data._asdict())
                num_written_records += 1
            num_records = self._get_total_record_count(conn, tblname)

        return self._gen_metadata_for_list(filename,
                                           num_records=num_records,
                                           num_written_records=num_written_records), 'exception_lists', cp.duration

    def _write_delta_csv_exceptions_list(self, operator_id):
        """Write delta CSV exceptions list for a particular operator."""
        cursor_name = 'listgen_write_delta_csv_exceptions_list_{0}'.format(operator_id)
        is_valid_query, imei_norm_with_check_digit_query = self._is_valid_and_check_digit_queries
        with create_db_connection(self._config.db_config) as conn, \
                conn.cursor(name=cursor_name) as cursor, contextlib.ExitStack() as stack, CodeProfiler() as cp:
            allowed_delta_reasons = self._allowed_delta_reasons(conn, 'exceptions_lists')
            fnames = {r: os.path.join(self._output_dir, '{0}_exceptions_{1}_delta_{2:d}_{3:d}_{4}.csv'
                                      .format(self._date_str,
                                              operator_id,
                                              self._base_run_id,
                                              self._run_id,
                                              r))
                      for r in allowed_delta_reasons}
            files = {r: stack.enter_context(open(fn, 'w', encoding='utf8'))
                     for r, fn in fnames.items()}
            csv_writers = {r: csv.DictWriter(f,
                                             fieldnames=['imei', 'imsi', 'msisdn'],
                                             extrasaction='ignore') for r, f in files.items()}
            for csv_writer in csv_writers.values():
                csv_writer.writeheader()

            cursor.execute(sql.SQL("""SELECT {imei_col} AS imei, imsi, msisdn, delta_reason
                                        FROM (SELECT *
                                                FROM gen_delta_exceptions_list(%s, %s),
                                                     LATERAL ({is_valid_query}) iv,
                                                     LATERAL ({imei_norm_with_check_digit_query}) cd) changes
                                       WHERE {valid_filter}
                                   """).format(is_valid_query=is_valid_query,
                                               imei_norm_with_check_digit_query=imei_norm_with_check_digit_query,
                                               valid_filter=self._valid_filter_query,
                                               imei_col=self._output_imei_column),
                           [operator_id, self._base_run_id])

            metrics = {r: defaultdict(int) for r in allowed_delta_reasons}
            for row_data in cursor:
                row_data_dict = row_data._asdict()
                delta_reason = row_data_dict.pop('delta_reason')
                csv_writer = csv_writers[delta_reason]
                csv_writer.writerow(row_data_dict)
                metrics[delta_reason]['num_records'] += 1

        return [self._gen_metadata_for_list(
            fn, **metrics[r]) for r, fn in fnames.items()], 'exceptions_lists_delta', cp.duration

    def _gen_metadata_for_list(self, filename, **extra_data):
        """Function to generate a metadata dictionary for a list filename and any extra metadata."""
        file_size = os.stat(filename).st_size
        with open(filename, 'rb') as f:
            md5sum = compute_md5_hash(f)
        core_metadata = {
            'filename': os.path.abspath(filename),
            'md5sum': md5sum,
            'file_size_bytes': file_size
        }
        return {**core_metadata, **extra_data}

    def _allowed_delta_reasons(self, conn, tblname):
        """Utility function to generate the allowed delta reasons for a particular table."""
        with conn.cursor() as cursor:
            cursor.execute("""SELECT pg_get_expr(conbin, conrelid) AS consrc
                                FROM pg_constraint
                               WHERE conrelid = %s::regclass
                                 AND pg_get_expr(conbin, conrelid) LIKE '(delta_reason%%'""",
                           [tblname])
            result = cursor.fetchall()
            assert len(result) == 1
            constraint_src = result[0].consrc
            constraint_array_vals = re.sub(r'.*ARRAY\[(.*)\].*', r'\1', constraint_src)
            split_array_vals = constraint_array_vals.split(',')
            return [re.sub(r"\s*.*\'(.*)\'::text", r'\1', v) for v in split_array_vals]
