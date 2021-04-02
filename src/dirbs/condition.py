"""
DIRBS code representation of a classification 'condition'.

A condition comprises of one or more dimension algorithms with associated parameters.

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

import importlib
import hashlib
import datetime

from psycopg2 import sql

from dirbs.utils import compute_amnesty_flags
import dirbs.partition_utils as partition_utils
from dirbs.utils import create_db_connection, CodeProfiler


class Condition(object):
    """Class representing the configuration for an individual dimension."""

    def __init__(self, cond_config):
        """
        Constructor to initialize config for individual dimension.

        Arguments:
            cond_config: dimension config object to init
        """
        self.label = cond_config.label
        self.config = cond_config
        self.dimensions = []
        for d in self.config.dimensions:
            # Don't need to check for import failure, as config already tested
            dim_module = \
                importlib.import_module('dirbs.dimensions.' + d.module)
            dim_constructor = dim_module.__dict__.get('dimension')
            self.dimensions.append(dim_constructor(**d.params, condition_label=self.label, invert=d.invert))

    def intermediate_tbl_name(self, run_id):
        """
        Method to return the intermediate table name used by this condition for this run_id.

        Arguments:
            run_id: current run_id of the classification job which needs the name of this intermediate table

        Returns:
            intermediate table name with prefix 'classify_temp_'
        """
        hashed_label = hashlib.md5(self.label.encode('utf-8')).hexdigest()
        return 'classify_temp_{0}_{1}'.format(hashed_label, run_id)

    def intermediate_tbl_id(self, run_id):
        """
        Method to return a sql.Identifier version of the intermediate_tbl_name function.

        Arguments:
            run_id: current run_id of the classification job which needs the name of this intermediate table

        Returns:
            sql.Identifier version of the intermediate_tbl_name function
        """
        return sql.Identifier(self.intermediate_tbl_name(run_id))

    def queue_calc_imeis_jobs(self, executor, app_config, run_id, curr_date):
        """
        Method to queue jobs to calculate the IMEIs that are met by this condition.

        Arguments:
            executor: instance of the python executor class, to submit back the results
            app_config: dirbs app current configuration, to extract various configs required for the job
            run_id: run id of the current classification job
            curr_date: current date of the system
        """
        with create_db_connection(app_config.db_config) as conn, conn.cursor() as cursor:
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {intermediate_tbl} (
                                          imei_norm TEXT NOT NULL,
                                          virt_imei_shard SMALLINT NOT NULL
                                      )
                                      PARTITION BY RANGE (virt_imei_shard)""")
                           .format(intermediate_tbl=self.intermediate_tbl_id(run_id)))
            partition_utils.create_imei_shard_partitions(conn, tbl_name=self.intermediate_tbl_name(run_id),
                                                         unlogged=True)
            parallel_shards = partition_utils.num_physical_imei_shards(conn)

        # Done with connection -- temp tables should now be committed
        virt_imei_shard_ranges = partition_utils.virt_imei_shard_bounds(parallel_shards)
        for virt_imei_range_start, virt_imei_range_end in virt_imei_shard_ranges:
            yield executor.submit(self._calc_imeis_job,
                                  app_config,
                                  run_id,
                                  curr_date,
                                  virt_imei_range_start,
                                  virt_imei_range_end)

    def _calc_imeis_job(self, app_config, run_id, curr_date, virt_imei_range_start, virt_imei_range_end):
        """
        Function to calculate the IMEIs that are met by this condition (single job).

        Arguments:
            app_config: dirbs app current configuration, to extract various configs required for the job
            run_id: run_id of the currently running classification job
            curr_date: current date of the system to be used within the job
            virt_imei_range_start: start of the shard for the imeis to analyze
            virt_imei_range_end: end of the shard for the imeis to analyze

        Returns:
            tuple containing count of matched imeis and time duration of the job execution
        """
        with create_db_connection(app_config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            dims_sql = [d.sql(conn, app_config, virt_imei_range_start, virt_imei_range_end, curr_date=curr_date)
                        for d in self.dimensions]

            # Calculate the SQL for the intersection of all dimensions
            condition_sql = sql.SQL(' INTERSECT ').join(dims_sql)

            # If sticky, we need to UNION the sql with the currently selected IMEIs
            if self.config.sticky:
                condition_sql = sql.SQL("""SELECT imei_norm
                                             FROM classification_state
                                            WHERE cond_name = {cond_name}
                                              AND virt_imei_shard >= {virt_imei_range_start}
                                              AND virt_imei_shard < {virt_imei_range_end}
                                              AND end_date IS NULL
                                                  UNION ALL {cond_results_sql}
                                        """).format(cond_name=sql.Literal(self.label),
                                                    virt_imei_range_start=sql.Literal(virt_imei_range_start),
                                                    virt_imei_range_end=sql.Literal(virt_imei_range_end),
                                                    cond_results_sql=condition_sql)

            # Make sure we only get distinct IMEIs
            condition_sql = sql.SQL("""SELECT imei_norm, calc_virt_imei_shard(imei_norm) AS virt_imei_shard
                                         FROM ({0}) non_distinct
                                     GROUP BY imei_norm""").format(condition_sql)

            # Copy results to the temp table
            tbl_name = partition_utils.imei_shard_name(base_name=self.intermediate_tbl_name(run_id),
                                                       virt_imei_range_start=virt_imei_range_start,
                                                       virt_imei_range_end=virt_imei_range_end)
            cursor.execute(sql.SQL("""INSERT INTO {intermediate_tbl}(imei_norm, virt_imei_shard) {condition_sql}""")
                           .format(intermediate_tbl=sql.Identifier(tbl_name),
                                   condition_sql=condition_sql))

            matching_imeis_count = cursor.rowcount

        return matching_imeis_count, cp.duration

    def queue_update_classification_state_jobs(self, executor, app_config, run_id, curr_date):
        """
        Method to queue jobs to update the classification_state table after the IMEIs have been calculated.

        Arguments:
            executor: job executor instance to submit back the results to the queue
            app_config: current dirbs app config object to use configuration from
            run_id: run_id of the current running classification job
            curr_date: current date of the system
        """
        with create_db_connection(app_config.db_config) as conn:
            parallel_shards = partition_utils.num_physical_imei_shards(conn)
            virt_imei_shard_ranges = partition_utils.virt_imei_shard_bounds(parallel_shards)
            for virt_imei_range_start, virt_imei_range_end in virt_imei_shard_ranges:
                yield executor.submit(self._update_classification_state_job,
                                      app_config,
                                      run_id,
                                      curr_date,
                                      virt_imei_range_start,
                                      virt_imei_range_end)

    def _update_classification_state_job(self, app_config, run_id, curr_date, virt_imei_range_start,
                                         virt_imei_range_end):
        """
        Function to update the classificate_state table with IMEIs that are met by this condition (single job).

        Arguments:
            app_config: current dirbs app config object
            run_id: id of the job currently executing and accessing this method
            curr_date: current date of the system
            virt_imei_range_start: start of the shard for the imeis to analyze
            virt_imei_range_end: end of the shard for the imeis to analyze

        Returns:
            duration of the job
        """
        with create_db_connection(app_config.db_config) as conn, conn.cursor() as cursor, CodeProfiler() as cp:
            src_shard_name = partition_utils.imei_shard_name(base_name=self.intermediate_tbl_name(run_id),
                                                             virt_imei_range_start=virt_imei_range_start,
                                                             virt_imei_range_end=virt_imei_range_end)

            # Add index on imei_norm
            indices = [partition_utils.IndexMetadatum(idx_cols=['imei_norm'], is_unique=True)]
            partition_utils.add_indices(conn, tbl_name=src_shard_name, idx_metadata=indices)

            # Analyze table for better stats/plans
            cursor.execute(sql.SQL('ANALYZE {0}').format(sql.Identifier(src_shard_name)))

            # Calculate block date
            if curr_date is None:
                curr_date = datetime.date.today()

            in_amnesty_eval_period, in_amnesty_period = compute_amnesty_flags(app_config, curr_date)

            # If condition is blocking and is not eligible for amnesty, then compute block_date.
            # The block_date is set to NULL for amnesty_eligible condition within the eval period.
            amnesty_eligible = self.config.amnesty_eligible
            sticky_block_date = curr_date + datetime.timedelta(days=self.config.grace_period) \
                if self.config.blocking and not (amnesty_eligible and in_amnesty_eval_period) else None

            # If the condition's amnesty_eligible flag changed while in eval period, then make sure we update
            # the amnesty_granted column in the classification_state table for existing IMEIs meeting that condition.
            # These rows will be selected by the existing WHERE clause filters as the block_date would change
            # from being NULL to not-NULL or vice-versa.
            set_amnesty_granted_column = sql.SQL(', amnesty_granted = {0}').format(sql.Literal(amnesty_eligible)) \
                if in_amnesty_eval_period else sql.SQL('')

            # If in amnesty period, update the block_date for IMEIs that were previously classified
            # as amnesty eligible. This filter is to select amnesty_granted IMEIs with not-NULL block date.
            # This is to make sure if the amnesty_end_date was updated, we update the block_date too.
            amnesty_block_date_filter = sql.SQL('OR cs.amnesty_granted = TRUE') if in_amnesty_period else sql.SQL('')

            dest_shard_name = partition_utils.imei_shard_name(base_name='classification_state',
                                                              virt_imei_range_start=virt_imei_range_start,
                                                              virt_imei_range_end=virt_imei_range_end)

            # If a condition is blocking, insert new records into state table with not null blocking date or
            # set a not null blocking date for the existing ones having a null block_date.
            # Viceversa, if a condition is not blocking, insert new records into state table with Null block_date
            # or set a Null block_date for the existing ones having a not-null block_date.
            # Set the amnesty_granted column equal to condition's amnesty_eligible flag when in amnesty eval
            # period, otherwise always set it to False for new IMEIs meeting the condition.
            cursor.execute(sql.SQL("""INSERT INTO {dest_shard} AS cs(imei_norm,
                                                                     cond_name,
                                                                     run_id,
                                                                     start_date,
                                                                     end_date,
                                                                     block_date,
                                                                     amnesty_granted,
                                                                     virt_imei_shard)
                                           SELECT imei_norm,
                                                  %s,
                                                  %s,
                                                  %s,
                                                  NULL,
                                                  %s,
                                                  %s,
                                                  calc_virt_imei_shard(imei_norm)
                                             FROM {src_shard}
                                                  ON CONFLICT (imei_norm, cond_name)
                                            WHERE end_date IS NULL
                                                  DO UPDATE
                                                        SET block_date = CASE WHEN cs.amnesty_granted = TRUE
                                                                          AND NOT {in_eval_period}
                                                                         THEN {amnesty_end_date}
                                                                         ELSE {sticky_block_date}
                                                                          END
                                                            {set_amnesty_granted_column}
                                                      WHERE (cs.block_date IS NULL
                                                        AND excluded.block_date IS NOT NULL)
                                                         OR (cs.block_date IS NOT NULL
                                                        AND excluded.block_date IS NULL)
                                                            {amnesty_block_date_filter}""")  # noqa Q441
                           .format(src_shard=sql.Identifier(src_shard_name),
                                   dest_shard=sql.Identifier(dest_shard_name),
                                   in_eval_period=sql.Literal(in_amnesty_eval_period),
                                   set_amnesty_granted_column=set_amnesty_granted_column,
                                   amnesty_block_date_filter=amnesty_block_date_filter,
                                   amnesty_end_date=sql.Literal(app_config.amnesty_config.amnesty_period_end_date),
                                   sticky_block_date=sql.Literal(sticky_block_date)),
                           [self.label, run_id, curr_date, sticky_block_date,
                            (amnesty_eligible and in_amnesty_eval_period)])

            # Get rid of records that no longer exist in the matched IMEIs list
            cursor.execute(sql.SQL("""UPDATE {dest_shard} dst
                                         SET end_date = %s
                                       WHERE cond_name = %s
                                         AND end_date IS NULL
                                         AND NOT EXISTS (SELECT imei_norm
                                                           FROM {src_shard}
                                                          WHERE imei_norm = dst.imei_norm)""")
                           .format(src_shard=sql.Identifier(src_shard_name),
                                   dest_shard=sql.Identifier(dest_shard_name)),
                           [curr_date, self.label])

        return cp.duration
