"""
DIRBS DB schema migration script (v76 -> v77).

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

from psycopg2 import sql

import dirbs.schema_migrators
import dirbs.utils as utils
import dirbs.partition_utils as part_utils


class RepartitionTablesMigrator(dirbs.schema_migrators.AbstractMigrator):
    """Class use to upgrade to V77 of the schema.

    Implemented in Python simply for notification of progress, since this can't easily be done using pure SQL.
    """

    def partition_registration_list(self, conn, *, num_physical_shards):
        """Method to repartition registration_list for v47 upgrade."""
        with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_power_user'):
            # Create parent partition
            cursor.execute(
                """CREATE TABLE historic_registration_list_new (
                                   LIKE historic_registration_list INCLUDING DEFAULTS
                                                                   INCLUDING IDENTITY
                                                                   INCLUDING CONSTRAINTS
                                                                   INCLUDING STORAGE
                                                                   INCLUDING COMMENTS
                               )
                               PARTITION BY RANGE (virt_imei_shard)
                            """
            )
            part_utils._grant_perms_registration_list(conn, part_name='historic_registration_list_new')
            # Create child partitions
            part_utils.create_imei_shard_partitions(conn, tbl_name='historic_registration_list_new',
                                                    num_physical_shards=num_physical_shards,
                                                    perms_func=part_utils._grant_perms_registration_list,
                                                    fillfactor=80)
            # Insert data from original partition
            cursor.execute("""INSERT INTO historic_registration_list_new
                                   SELECT *
                                     FROM historic_registration_list""")

            # Add in indexes to each partition
            idx_metadata = [part_utils.IndexMetadatum(idx_cols=['imei_norm'],
                                                      is_unique=True,
                                                      partial_sql='WHERE end_date IS NULL')]
            part_utils.add_indices(conn, tbl_name='historic_registration_list_new', idx_metadata=idx_metadata)

            # Drop old view + table, rename tables, indexes and constraints
            cursor.execute('DROP VIEW registration_list')
            cursor.execute('DROP TABLE historic_registration_list CASCADE')
            part_utils.rename_table_and_indices(conn, old_tbl_name='historic_registration_list_new',
                                                new_tbl_name='historic_registration_list', idx_metadata=idx_metadata)
            cursor.execute("""CREATE OR REPLACE VIEW registration_list AS
                                   SELECT imei_norm, make, model, status, virt_imei_shard
                                     FROM historic_registration_list
                                    WHERE end_date IS NULL WITH CHECK OPTION""")
            cursor.execute("""GRANT SELECT ON registration_list
                                      TO dirbs_core_classify, dirbs_core_api, dirbs_core_import_registration_list""")

    def upgrade(self, db_conn):  # noqa: C901
        """Overrides AbstractMigrator upgrade method."""
        logger = logging.getLogger('dirbs.db')
        with db_conn.cursor() as cursor:
            cursor.execute("""CREATE FUNCTION calc_virt_imei_shard(imei TEXT) RETURNS SMALLINT
                              LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
                              AS $$
                              BEGIN
                                  RETURN SUBSTRING(COALESCE(imei, ''), 13, 2)::SMALLINT;
                              EXCEPTION WHEN OTHERS THEN
                                  RETURN 0;
                              END;
                              $$""")

            # By default, create 4 shards
            num_initial_shards = 4

            logger.info('Re-partitioning classification_state table...')
            cursor.execute('ALTER TABLE classification_state ADD COLUMN virt_imei_shard SMALLINT')
            cursor.execute('UPDATE classification_state SET virt_imei_shard = calc_virt_imei_shard(imei_norm)')
            cursor.execute('ALTER TABLE classification_state ALTER COLUMN virt_imei_shard SET NOT NULL')
            part_utils.repartition_classification_state(db_conn, num_physical_shards=num_initial_shards)
            logger.info('Re-partitioned classification_state table')

            logger.info('Re-partitioning registration_list table...')
            cursor.execute('ALTER TABLE historic_registration_list ADD COLUMN virt_imei_shard SMALLINT')
            cursor.execute('UPDATE historic_registration_list SET virt_imei_shard = calc_virt_imei_shard(imei_norm)')
            cursor.execute('ALTER TABLE historic_registration_list ALTER COLUMN virt_imei_shard SET NOT NULL')
            self.partition_registration_list(db_conn, num_physical_shards=num_initial_shards)
            logger.info('Re-partitioned registration_list table')

            logger.info('Re-partitioning pairing_list table...')
            cursor.execute('ALTER TABLE historic_pairing_list ADD COLUMN virt_imei_shard SMALLINT')
            cursor.execute('UPDATE historic_pairing_list SET virt_imei_shard = calc_virt_imei_shard(imei_norm)')
            cursor.execute('ALTER TABLE historic_pairing_list ALTER COLUMN virt_imei_shard SET NOT NULL')
            part_utils.repartition_pairing_list(db_conn, num_physical_shards=num_initial_shards)
            logger.info('Re-partitioned pairing_list table')

            logger.info('Re-partitioning blacklist table...')
            cursor.execute('ALTER TABLE blacklist ADD COLUMN virt_imei_shard SMALLINT')
            cursor.execute('UPDATE blacklist SET virt_imei_shard = calc_virt_imei_shard(imei_norm)')
            cursor.execute('ALTER TABLE blacklist ALTER COLUMN virt_imei_shard SET NOT NULL')
            part_utils.repartition_blacklist(db_conn, num_physical_shards=num_initial_shards)
            logger.info('Re-partitioned blacklist table')

            # Need to make sure owner of list tables is dirbs_core_listgen
            logger.info('Re-partitioning notifications_lists table...')
            # The original notifications_lists were not created with a single sequence for the IDs, so just do now
            with utils.db_role_setter(db_conn, role_name='dirbs_core_listgen'):
                cursor.execute(
                    """CREATE UNLOGGED TABLE notifications_lists_new (
                           row_id BIGSERIAL NOT NULL,
                           operator_id TEXT NOT NULL,
                           imei_norm TEXT NOT NULL,
                           imsi TEXT NOT NULL,
                           msisdn TEXT NOT NULL,
                           block_date DATE NOT NULL,
                           reasons TEXT[] NOT NULL,
                           amnesty_granted BOOLEAN DEFAULT FALSE NOT NULL,
                           start_run_id BIGINT NOT NULL,
                           end_run_id BIGINT,
                           delta_reason TEXT NOT NULL CHECK (delta_reason IN ('new', 'resolved', 'blacklisted',
                                                                              'no_longer_seen', 'changed')),
                           virt_imei_shard SMALLINT NOT NULL
                       ) PARTITION BY LIST (operator_id)
                    """
                )

            # Work out who the operators are
            partitions = utils.child_table_names(db_conn, 'notifications_lists')
            # Make sure that they are owned by dirbs_core_listgen (they can be owner by dirbs_core_power_user)
            # due to bad previous migration scripts
            with utils.db_role_setter(db_conn, role_name='dirbs_core_power_user'):
                for p in partitions:
                    cursor.execute(sql.SQL('ALTER TABLE {0} OWNER TO dirbs_core_listgen').format(sql.Identifier(p)))

            operators = [x.operator_id for x in utils.table_invariants_list(db_conn, partitions, ['operator_id'])]

            # Create operator child partitions
            for op_id in operators:
                tbl_name = part_utils.per_mno_lists_partition(operator_id=op_id, suffix='_new',
                                                              list_type='notifications')
                part_utils.create_per_mno_lists_partition(db_conn, operator_id=op_id,
                                                          parent_tbl_name='notifications_lists_new',
                                                          tbl_name=tbl_name,
                                                          num_physical_shards=1,
                                                          unlogged=True,
                                                          fillfactor=100)

            cursor.execute(
                """INSERT INTO notifications_lists_new(operator_id, imei_norm, imsi, msisdn, block_date,
                                                       reasons, start_run_id, end_run_id, delta_reason,
                                                       virt_imei_shard)
                        SELECT operator_id, imei_norm, imsi, msisdn, block_date,
                               reasons, start_run_id, end_run_id, delta_reason, calc_virt_imei_shard(imei_norm)
                          FROM notifications_lists
                """
            )
            # Drop old table, rename tables, indexes and constraints
            cursor.execute("""ALTER TABLE notifications_lists_new
                              RENAME CONSTRAINT notifications_lists_new_delta_reason_check
                              TO notifications_lists_delta_reason_check""")
            cursor.execute('DROP TABLE notifications_lists CASCADE')
            cursor.execute("""ALTER SEQUENCE notifications_lists_new_row_id_seq
                              RENAME TO notifications_lists_row_id_seq""")
            part_utils.rename_table_and_indices(db_conn, old_tbl_name='notifications_lists_new',
                                                new_tbl_name='notifications_lists')
            part_utils.repartition_notifications_lists(db_conn, num_physical_shards=num_initial_shards)
            logger.info('Re-partitioned notifications_lists table')

            logger.info('Re-partitioning exceptions_lists table...')
            # The original exceptions_lists were not created with a single sequence for the IDs, so just do now
            with utils.db_role_setter(db_conn, role_name='dirbs_core_listgen'):
                cursor.execute(
                    """CREATE UNLOGGED TABLE exceptions_lists_new (
                           row_id BIGSERIAL NOT NULL,
                           operator_id TEXT NOT NULL,
                           imei_norm TEXT NOT NULL,
                           imsi TEXT NOT NULL,
                           start_run_id BIGINT NOT NULL,
                           end_run_id BIGINT,
                           delta_reason TEXT NOT NULL CHECK (delta_reason IN ('added', 'removed')),
                           virt_imei_shard SMALLINT NOT NULL
                       ) PARTITION BY LIST (operator_id)
                    """
                )
            # Work out who the operators are
            partitions = utils.child_table_names(db_conn, 'exceptions_lists')
            # Make sure that they are owned by dirbs_core_listgen (they can be owner by dirbs_core_power_user)
            # due to bad previous migration scripts
            with utils.db_role_setter(db_conn, role_name='dirbs_core_power_user'):
                for p in partitions:
                    cursor.execute(sql.SQL('ALTER TABLE {0} OWNER TO dirbs_core_listgen').format(sql.Identifier(p)))
            operators = [x.operator_id for x in utils.table_invariants_list(db_conn, partitions, ['operator_id'])]

            # Create operator child partitions
            for op_id in operators:
                tbl_name = part_utils.per_mno_lists_partition(operator_id=op_id, suffix='_new', list_type='exceptions')
                part_utils.create_per_mno_lists_partition(db_conn, operator_id=op_id,
                                                          parent_tbl_name='exceptions_lists_new',
                                                          tbl_name=tbl_name,
                                                          num_physical_shards=1,
                                                          unlogged=True,
                                                          fillfactor=100)

            cursor.execute(
                """INSERT INTO exceptions_lists_new(operator_id, imei_norm, imsi, start_run_id,
                                                    end_run_id, delta_reason, virt_imei_shard)
                        SELECT operator_id, imei_norm, imsi, start_run_id, end_run_id, delta_reason,
                               calc_virt_imei_shard(imei_norm)
                          FROM exceptions_lists
                """
            )
            # Drop old table, rename tables, indexes and constraints
            cursor.execute("""ALTER TABLE exceptions_lists_new
                              RENAME CONSTRAINT exceptions_lists_new_delta_reason_check
                              TO exceptions_lists_delta_reason_check""")
            cursor.execute('DROP TABLE exceptions_lists CASCADE')
            cursor.execute('ALTER SEQUENCE exceptions_lists_new_row_id_seq RENAME TO exceptions_lists_row_id_seq')
            part_utils.rename_table_and_indices(db_conn, old_tbl_name='exceptions_lists_new',
                                                new_tbl_name='exceptions_lists')
            part_utils.repartition_exceptions_lists(db_conn, num_physical_shards=num_initial_shards)
            logger.info('Re-partitioned exceptions_lists table')

            logger.info('Re-partitioning seen_imeis (network_imeis) table')
            # First, just put everything in a temporary table so that we can call partutils
            with utils.db_role_setter(db_conn, role_name='dirbs_core_import_operator'):
                cursor.execute(
                    """CREATE UNLOGGED TABLE network_imeis (
                           first_seen DATE NOT NULL,
                           last_seen DATE NOT NULL,
                           seen_rat_bitmask INTEGER,
                           imei_norm TEXT NOT NULL,
                           virt_imei_shard SMALLINT NOT NULL
                       )
                    """
                )
            #
            # We disable index scans here as doing a merge append with index scans is much slower and involves
            # a lot of seeks which kills performance on non-SSD drives. Better to use an append plan and sort
            # the results by imei_norm
            #
            cursor.execute('SET enable_indexscan = false')
            cursor.execute(
                """INSERT INTO network_imeis
                        SELECT MIN(first_seen),
                               MAX(last_seen),
                               bit_or(seen_rat_bitmask),
                               imei_norm,
                               calc_virt_imei_shard(imei_norm)
                          FROM seen_imeis
                      GROUP BY imei_norm
                """
            )
            cursor.execute('SET enable_indexscan = true')
            part_utils.repartition_network_imeis(db_conn, num_physical_shards=num_initial_shards)
            cursor.execute('DROP TABLE seen_imeis CASCADE')
            logger.info('Re-partitioned seen_imeis (network_imeis) table')

            # First, just put all country-level triplets in a temporary table so that we can call partition_utils
            with utils.db_role_setter(db_conn, role_name='dirbs_core_import_operator'):
                cursor.execute(
                    """CREATE UNLOGGED TABLE monthly_network_triplets_country (
                           triplet_year SMALLINT NOT NULL,
                           triplet_month SMALLINT NOT NULL,
                           first_seen DATE NOT NULL,
                           last_seen DATE NOT NULL,
                           date_bitmask INTEGER NOT NULL,
                           triplet_hash UUID NOT NULL,
                           imei_norm TEXT,
                           imsi TEXT,
                           msisdn TEXT,
                           virt_imei_shard SMALLINT NOT NULL,
                           CHECK (last_seen >= first_seen),
                           CHECK (EXTRACT(month FROM last_seen) = triplet_month AND
                                  EXTRACT(year FROM last_seen) = triplet_year),
                           CHECK (EXTRACT(month FROM first_seen) = triplet_month AND
                                  EXTRACT(year FROM first_seen) = triplet_year)
                       ) PARTITION BY RANGE (triplet_year, triplet_month)
                    """
                )

            # Work out what partitions to create and create them
            partitions = utils.child_table_names(db_conn, 'seen_triplets')
            # Make sure that they are owned by dirbs_core_import_operator (they can be owner by dirbs_core_power_user)
            # due to bad previous migration scripts
            with utils.db_role_setter(db_conn, role_name='dirbs_core_power_user'):
                for p in partitions:
                    cursor.execute(sql.SQL('ALTER TABLE {0} OWNER TO dirbs_core_import_operator')
                                   .format(sql.Identifier(p)))

            year_month_tuples = {(x.triplet_year, x.triplet_month)
                                 for x in utils.table_invariants_list(db_conn, partitions, ['triplet_year',
                                                                                            'triplet_month'])}
            for year, month in year_month_tuples:
                part_utils.create_monthly_network_triplets_country_partition(db_conn, month=month,
                                                                             year=year, num_physical_shards=1)

            with utils.db_role_setter(db_conn, role_name='dirbs_core_import_operator'):
                cursor.execute(
                    """CREATE UNLOGGED TABLE monthly_network_triplets_per_mno (
                            LIKE monthly_network_triplets_country INCLUDING ALL,
                            operator_id TEXT NOT NULL
                       ) PARTITION BY LIST (operator_id)
                    """
                )

            # Work out what partitions to create and create them
            op_year_month_tuples = {(x.operator_id, x.triplet_year, x.triplet_month)
                                    for x in utils.table_invariants_list(db_conn, partitions, ['operator_id',
                                                                                               'triplet_year',
                                                                                               'triplet_month'])}
            # Create child partitions at per-MNO level
            for op, year, month in op_year_month_tuples:
                part_utils.create_monthly_network_triplets_per_mno_partition(db_conn, operator_id=op, month=month,
                                                                             year=year, num_physical_shards=1)

            # Create temporary monthly_network_triplets_per_mno table
            for year, month in year_month_tuples:
                logger.info('Generating temporary monthly_network_triplets_per_mno entries for {0:02d}/{1:d}...'
                            .format(month, year))
                cursor.execute(
                    """INSERT INTO monthly_network_triplets_per_mno
                            SELECT %(year)s,
                                   %(month)s,
                                   first_seen,
                                   last_seen,
                                   date_bitmask,
                                   triplet_hash,
                                   imei_norm,
                                   imsi,
                                   msisdn,
                                   calc_virt_imei_shard(imei_norm),
                                   operator_id
                              FROM seen_triplets
                             WHERE triplet_year = %(year)s
                               AND triplet_month = %(month)s
                    """,
                    {'year': year, 'month': month}
                )
                logger.info('Generated temporary monthly_network_triplets_per_mno entries for {0:02d}/{1:d}'
                            .format(month, year))

            # Create temporary monthly_network_triplets_country table. We need to do this monthly as we need
            # to aggregate by triplets on a monthly basis
            #
            # We disable index scans here as doing a merge append with index scans is much slower and involves
            # a lot of seeks which kills performance on non-SSD drives. Better to use an append plan and sort
            # the results by imei_norm
            #
            cursor.execute('SET enable_indexscan = false')
            for year, month in year_month_tuples:
                logger.info('Generating temporary monthly_network_triplets_country entries for {0:02d}/{1:d}...'
                            .format(month, year))
                cursor.execute(
                    """INSERT INTO monthly_network_triplets_country
                            SELECT %(year)s,
                                   %(month)s,
                                   MIN(first_seen),
                                   MAX(last_seen),
                                   bit_or(date_bitmask),
                                   triplet_hash,
                                   FIRST(imei_norm),
                                   FIRST(imsi),
                                   FIRST(msisdn),
                                   calc_virt_imei_shard(FIRST(imei_norm))
                              FROM seen_triplets
                             WHERE triplet_year = %(year)s
                               AND triplet_month = %(month)s
                          GROUP BY triplet_hash
                    """,
                    {'year': year, 'month': month}
                )
                logger.info('Generated temporary monthly_network_triplets_country entries for {0:02d}/{1:d}'
                            .format(month, year))
            cursor.execute('SET enable_indexscan = true')

            logger.info('Re-partitioning temporary monthly_network_triplets tables...')
            # Previously, the operator_data view was owned by dirbs_core_power_user but is now owned by the
            # dirbs_core_import_operator since it must be re-created
            with utils.db_role_setter(db_conn, role_name='dirbs_core_power_user'):
                cursor.execute('ALTER VIEW operator_data OWNER TO dirbs_core_import_operator')
            part_utils.repartition_monthly_network_triplets(db_conn, num_physical_shards=num_initial_shards)
            cursor.execute('DROP TABLE seen_triplets CASCADE')
            logger.info('Re-partitioned temporary monthly_network_triplets tables')

            # Replace list generation function to include virt_imei_shard
            cursor.execute("""
                DROP FUNCTION gen_blacklist(run_id BIGINT);
                DROP FUNCTION gen_notifications_list(op_id TEXT, run_id BIGINT);
                DROP FUNCTION gen_exceptions_list(op_id TEXT, run_id BIGINT);

                --
                -- Create function to generate a full blacklist for a given run_id. A value of -1 means get the latest
                -- list.
                --
                CREATE FUNCTION gen_blacklist(run_id BIGINT = -1)
                    RETURNS TABLE (
                        imei_norm       TEXT,
                        virt_imei_shard SMALLINT,
                        block_date      DATE,
                        reasons         TEXT[]
                    )
                    LANGUAGE plpgsql STRICT STABLE PARALLEL SAFE
                    AS $$
                DECLARE
                    query_run_id    BIGINT;
                BEGIN
                    --
                    -- If we don't specify a run_id, just set to the maximum run_id which will always return all rows
                    -- where end_run_id is NULL
                    --
                    IF run_id = -1 THEN
                        run_id := max_bigint();
                    END IF;

                    RETURN QUERY SELECT bl.imei_norm,
                                        bl.virt_imei_shard,
                                        bl.block_date,
                                        bl.reasons
                                   FROM blacklist bl
                                  WHERE bl.delta_reason != 'unblocked'
                                    AND run_id >= bl.start_run_id
                                    AND (run_id < bl.end_run_id OR bl.end_run_id IS NULL);
                END
                $$;

                --
                -- Create function to generate a full notifications_list for a given run_id and operator ID. A value
                -- of -1 means get the latest list.
                --
                CREATE FUNCTION gen_notifications_list(op_id TEXT, run_id BIGINT = -1)
                    RETURNS TABLE (
                        imei_norm       TEXT,
                        virt_imei_shard SMALLINT,
                        imsi            TEXT,
                        msisdn          TEXT,
                        block_date      DATE,
                        reasons         TEXT[],
                        amnesty_granted BOOLEAN
                    )
                    LANGUAGE plpgsql STRICT STABLE PARALLEL SAFE
                    AS $$
                BEGIN
                    --
                    -- If we don't specify a run_id, just set to the maximum run_id which will always return all rows
                    -- where end_run_id is NULL
                    --
                    IF run_id = -1 THEN
                        run_id := max_bigint();
                    END IF;

                    RETURN QUERY SELECT nl.imei_norm,
                                        nl.virt_imei_shard,
                                        nl.imsi,
                                        nl.msisdn,
                                        nl.block_date,
                                        nl.reasons,
                                        nl.amnesty_granted
                                   FROM notifications_lists nl
                                  WHERE nl.operator_id = op_id
                                    AND nl.delta_reason NOT IN ('resolved', 'blacklisted')
                                    AND run_id >= nl.start_run_id
                                    AND (run_id < nl.end_run_id OR nl.end_run_id IS NULL);
                END
                $$;

                --
                -- Create function to generate a full exceptions_list for a given run_id and operator ID. A value
                -- of -1 means get the latest list.
                --
                CREATE FUNCTION gen_exceptions_list(op_id TEXT, run_id BIGINT = -1)
                    RETURNS TABLE (
                        imei_norm       TEXT,
                        virt_imei_shard SMALLINT,
                        imsi            TEXT
                    )
                    LANGUAGE plpgsql STRICT STABLE PARALLEL SAFE
                    AS $$
                BEGIN
                    --
                    -- If we don't specify a run_id, just set to the maximum run_id which will always return all
                    -- rows where end_run_id is NULL
                    --
                    IF run_id = -1 THEN
                        run_id := max_bigint();
                    END IF;

                    RETURN QUERY SELECT el.imei_norm,
                                        el.virt_imei_shard,
                                        el.imsi
                                   FROM exceptions_lists el
                                  WHERE el.operator_id = op_id
                                    AND el.delta_reason != 'removed'
                                    AND run_id >= el.start_run_id
                                    AND (run_id < el.end_run_id OR el.end_run_id IS NULL);
                END
                $$;
            """)  # noqa: Q440, Q441

            # Update schema metadata table
            cursor.execute("""ALTER TABLE schema_metadata ADD COLUMN phys_shards SMALLINT NOT NULL
                              DEFAULT %s CHECK (phys_shards > 0 AND phys_shards <= 100)""", [num_initial_shards])
            cursor.execute('ALTER TABLE schema_metadata ALTER COLUMN phys_shards DROP DEFAULT')

            # Drop obsolete columns
            cursor.execute('ALTER TABLE schema_metadata DROP COLUMN potential_whitespace_imsis_msisdns')
            cursor.execute('ALTER TABLE report_monthly_stats DROP COLUMN num_whitespace_imsi_records')
            cursor.execute('ALTER TABLE report_monthly_stats DROP COLUMN num_whitespace_msisdn_records')


migrator = RepartitionTablesMigrator
