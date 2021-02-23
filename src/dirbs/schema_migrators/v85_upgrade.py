"""
DIRBS DB schema migration script (v84 -> v85).

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


class SchemaMigrator(dirbs.schema_migrators.AbstractMigrator):
    """Class use to upgrade to V84 of the schema."""

    def _repartition_pairing_list(self, conn, *, num_physical_shards):
        """Repartition pairing list to implement change in structure."""
        with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_power_user'):
            cursor.execute(
                """CREATE TABLE historic_pairing_list_new (
                       LIKE historic_pairing_list INCLUDING DEFAULTS
                                                  INCLUDING IDENTITY
                                                  INCLUDING CONSTRAINTS
                                                  INCLUDING STORAGE
                                                  INCLUDING COMMENTS
                   )
                   PARTITION BY RANGE (virt_imei_shard)
                """
            )
            cursor.execute("""ALTER TABLE historic_pairing_list_new ADD COLUMN msisdn TEXT NOT NULL""")

            # grant permissions
            part_utils._grant_perms_pairing_list(conn, part_name='historic_pairing_list_new')

            # create child partitions
            part_utils.create_imei_shard_partitions(conn, tbl_name='historic_pairing_list_new',
                                                    num_physical_shards=num_physical_shards,
                                                    perms_func=part_utils._grant_perms_pairing_list, fillfactor=80)

            # copy data from original partition
            cursor.execute("""INSERT INTO historic_pairing_list_new
                                   SELECT DISTINCT p.imei_norm, p.imsi, p.start_date, p.end_date, p.virt_imei_shard,
                                          m.msisdn
                                     FROM historic_pairing_list p
                               INNER JOIN monthly_network_triplets_country m ON p.imsi = m.imsi""")

            # add indexes
            idx_metadata = [part_utils.IndexMetadatum(idx_cols=['imei_norm', 'imsi', 'msisdn'],
                                                      is_unique=True,
                                                      partial_sql='WHERE end_date IS NULL')]
            part_utils.add_indices(conn, tbl_name='historic_pairing_list_new', idx_metadata=idx_metadata)

            # drop old views, tables, indexes and constraints
            cursor.execute('DROP VIEW pairing_list')
            cursor.execute('DROP TABLE historic_pairing_list CASCADE')
            part_utils.rename_table_and_indices(conn, old_tbl_name='historic_pairing_list_new',
                                                new_tbl_name='historic_pairing_list', idx_metadata=idx_metadata)

            # create new view and grant permissions
            cursor.execute("""CREATE VIEW pairing_list AS
                                   SELECT imei_norm, imsi, msisdn, virt_imei_shard
                                     FROM historic_pairing_list
                                    WHERE end_date IS NULL WITH CHECK OPTION""")
            cursor.execute("""GRANT SELECT ON pairing_list
                              TO dirbs_core_listgen, dirbs_core_report, dirbs_core_api, dirbs_core_import_pairing_list
                           """)

            # drop and recreate staging data insert trigger
            cursor.execute("""
                            DROP FUNCTION pairing_list_staging_data_insert_trigger_fn() CASCADE;

                            CREATE FUNCTION pairing_list_staging_data_insert_trigger_fn() RETURNS trigger
                                LANGUAGE plpgsql
                                AS $$
                            BEGIN
                                -- Clean/normalize data before inserting
                                NEW.imei_norm = normalize_imei(NULLIF(TRIM(NEW.imei), ''));
                                NEW.imsi = NULLIF(TRIM(new.imsi), '');
                                NEW.msisdn = NULLIF(TRIM(new.msisdn), '');
                                RETURN NEW;
                            END
                            $$;
            """)

    def _repartition_exceptions_lists(self, conn, *, num_physical_shards):
        """Repartition the exceptions lists to support msisdn."""
        with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_listgen'):
            cursor.execute(
                """CREATE TABLE exceptions_lists_new (
                       LIKE exceptions_lists INCLUDING DEFAULTS
                                             INCLUDING IDENTITY
                                             INCLUDING CONSTRAINTS
                                             INCLUDING STORAGE
                                             INCLUDING COMMENTS
                   )
                   PARTITION BY LIST (operator_id);

                   ALTER TABLE exceptions_lists_new ADD COLUMN msisdn TEXT NOT NULL;
                """
            )

            part_utils._grant_perms_list(conn, part_name='exceptions_lists_new')  # grant relevant permissions
            imei_shard_names = utils.child_table_names(conn, 'exceptions_lists')  # determine the child table names
            operators = [o.operator_id
                         for o in utils.table_invariants_list(conn,
                                                              imei_shard_names,
                                                              ['operator_id'])]  # workout who the operators are

            # create child partitions for new list (operator at top level, then IMEI sharded)
            for op_id in operators:
                tbl_name = part_utils.per_mno_lists_partition(operator_id=op_id, suffix='_new', list_type='exceptions')
                part_utils.create_per_mno_lists_partition(conn, parent_tbl_name='exceptions_lists_new',
                                                          tbl_name=tbl_name, operator_id=op_id,
                                                          num_physical_shards=num_physical_shards)

            # insert data into the new parent partition
            cursor.execute("""INSERT INTO exceptions_lists_new
                                   SELECT e.row_id, e.operator_id, e.imei_norm, e.imsi, e.start_run_id, e.end_run_id,
                                          e.delta_reason, e.virt_imei_shard, p.msisdn
                                     FROM exceptions_lists e
                               INNER JOIN historic_pairing_list p ON e.imsi = p.imsi""")

            # add indexes in each partitions
            part_utils.add_indices(conn, tbl_name='exceptions_lists_new',
                                   idx_metadata=part_utils.exceptions_lists_indices())

            # drop old table, after assigning sequence to new table
            cursor.execute('ALTER SEQUENCE exceptions_lists_row_id_seq OWNED BY exceptions_lists_new.row_id')
            cursor.execute('DROP TABLE exceptions_lists CASCADE')

            # rename table, indexes and constraints
            part_utils.rename_table_and_indices(conn, old_tbl_name='exceptions_lists_new',
                                                new_tbl_name='exceptions_lists',
                                                idx_metadata=part_utils.exceptions_lists_indices())

            # recreating gen_exceptionlist function
            with utils.db_role_setter(conn, role_name='dirbs_core_power_user'):
                cursor.execute("""
                                DROP FUNCTION gen_exceptions_list(op_id TEXT, run_id BIGINT);

                                --
                                -- Recreate function to generate a full exceptions_list for a given
                                -- run_id and operator.
                                -- A value of -1 means get the latest list.
                                --
                                CREATE FUNCTION gen_exceptions_list(op_id TEXT, run_id BIGINT = -1)
                                    RETURNS TABLE (
                                        imei_norm       TEXT,
                                        virt_imei_shard SMALLINT,
                                        imsi            TEXT,
                                        msisdn          TEXT
                                    )
                                    LANGUAGE plpgsql STRICT STABLE PARALLEL SAFE
                                    AS $$
                                BEGIN
                                    --
                                    -- If we don't specify a run_id, just set to the maximum run_id which will always
                                    -- return all rows where end_run_id is NULL
                                    --
                                    IF run_id = -1 THEN
                                        run_id := max_bigint();
                                    END IF;

                                    RETURN QUERY SELECT el.imei_norm,
                                                        el.virt_imei_shard,
                                                        el.imsi,
                                                        el.msisdn
                                                   FROM exceptions_lists el
                                                  WHERE el.operator_id = op_id
                                                    AND el.delta_reason != 'removed'
                                                    AND run_id >= el.start_run_id
                                                    AND (run_id < el.end_run_id OR el.end_run_id IS NULL);
                                END
                                $$;

                                DROP FUNCTION gen_delta_exceptions_list(op_id TEXT, base_run_id BIGINT, run_id BIGINT);

                                --
                                -- Create function to generate a per-MNO delta exceptions list for a run_id, operator
                                -- id and optional base_run_id.
                                --
                                -- If not base_run_id is supplied, this function will use the maximum run_id found in
                                -- the DB that it less than than the supplied run_id
                                --
                                CREATE FUNCTION gen_delta_exceptions_list(op_id TEXT,
                                                                          base_run_id BIGINT,
                                                                          run_id BIGINT = -1)
                                    RETURNS TABLE (
                                        imei_norm       TEXT,
                                        imsi            TEXT,
                                        msisdn          TEXT,
                                        delta_reason    TEXT
                                    )
                                    LANGUAGE plpgsql STRICT STABLE PARALLEL SAFE
                                    AS $$
                                BEGIN
                                    --
                                    -- If we don't specify a run_id, just set to the maximum run_id
                                    --
                                    IF run_id = -1 THEN
                                        run_id := max_bigint();
                                    END IF;

                                    IF run_id < base_run_id THEN
                                      RAISE EXCEPTION 'Parameter base_run_id % greater than run_id %',
                                                      base_run_id, run_id;
                                    END IF;

                                    RETURN QUERY SELECT *
                                                   FROM (SELECT el.imei_norm,
                                                                el.imsi,
                                                                el.msisdn,
                                                                overall_delta_reason(el.delta_reason
                                                                        ORDER BY start_run_id DESC) AS delta_reason
                                                           FROM exceptions_lists el
                                                          WHERE operator_id = op_id
                                                            AND start_run_id > base_run_id
                                                            AND start_run_id <= run_id
                                                       GROUP BY el.imei_norm, el.imsi, el.msisdn) x
                                                  WHERE x.delta_reason IS NOT NULL;
                                END
                                $$;
                                """)  # noqa: Q440, Q441

    def _permit_listgen_on_barred_lists(self, logger, conn):
        """Method to grant permisions to dirbs_core_listgen on barred list, barred tac list."""
        with conn.cursor() as cursor:
            logger.debug('Granting SELECT permission on barred_list To dirbs_core_listgen...')
            cursor.execute('GRANT SELECT ON barred_list TO dirbs_core_listgen')
            logger.debug('Granting SELECT permission on barred_tac_list To dirbs_core_listgen...')
            cursor.execute('GRANT SELECT ON barred_tac_list TO dirbs_core_listgen')

    def _migrate_monitoring_list(self, logger, conn):
        """Method to migrate monitoring list."""
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("""CREATE TABLE historic_monitoring_list (
                                          imei_norm text NOT NULL,
                                          start_date TIMESTAMP NOT NULL,
                                          end_date TIMESTAMP,
                                          virt_imei_shard SMALLINT NOT NULL
                                    )
                                    PARTITION BY RANGE (virt_imei_shard)"""))

            num_shards = part_utils.num_physical_imei_shards(conn)

            logger.debug('Granting permissions to monitoring_list partitions...')
            part_utils._grant_perms_monitoring_list(conn, part_name='historic_monitoring_list')

            logger.debug('Creating monitoring_list child partitions...')
            part_utils.create_imei_shard_partitions(conn, tbl_name='historic_monitoring_list',
                                                    num_physical_shards=num_shards,
                                                    perms_func=part_utils._grant_perms_monitoring_list,
                                                    fillfactor=80)

            # Add indexes to each partition
            idx_metadata = [part_utils.IndexMetadatum(idx_cols=['imei_norm'],
                                                      is_unique=True,
                                                      partial_sql='WHERE end_date IS NULL')]
            part_utils.add_indices(conn, tbl_name='historic_monitoring_list', idx_metadata=idx_metadata)

            # creating view to historic_monitoring_list
            cursor.execute("""CREATE OR REPLACE VIEW monitoring_list AS
                                   SELECT imei_norm, virt_imei_shard
                                     FROM historic_monitoring_list
                                    WHERE end_date IS NULL WITH CHECK OPTION""")
            cursor.execute("""GRANT SELECT ON monitoring_list
                                      TO dirbs_core_classify, dirbs_core_api, dirbs_core_import_monitoring_list""")

            # creating insert trigger function
            cursor.execute("""CREATE FUNCTION monitoring_list_staging_data_insert_trigger_fn() RETURNS TRIGGER
                                              LANGUAGE plpgsql
                                              AS $$
                                          BEGIN
                                              NEW.imei_norm = normalize_imei(NULLIF(TRIM(NEW.imei), ''));
                                              RETURN NEW;
                                          END
                                          $$;

                                          ALTER FUNCTION monitoring_list_staging_data_insert_trigger_fn()
                                            OWNER TO dirbs_core_power_user;
                                        """)
            logger.debug('Granting create permission to dirbs_core_import_monitoring_list...')
            cursor.execute('GRANT CREATE ON SCHEMA core TO dirbs_core_import_monitoring_list')

    def _migrate_device_association_list(self, logger, conn):
        """Method to migrate barred imeis list."""
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("""CREATE TABLE historic_device_association_list (
                                          imei_norm text NOT NULL,
                                          uid text NOT NULL,
                                          start_date TIMESTAMP NOT NULL,
                                          end_date TIMESTAMP,
                                          virt_imei_shard SMALLINT NOT NULL
                                    )
                                    PARTITION BY RANGE (virt_imei_shard);"""))

            num_shards = part_utils.num_physical_imei_shards(conn)
            logger.debug('Granting permissions to barred_list partitions...')
            part_utils._grant_perms_barred_list(conn, part_name='historic_device_association_list')
            logger.debug('Creating barred_list child partitions...')
            part_utils.create_imei_shard_partitions(conn, tbl_name='historic_device_association_list',
                                                    num_physical_shards=num_shards,
                                                    perms_func=part_utils._grant_perms_association_list,
                                                    fillfactor=80)

            # Add indexes to each partition
            idx_metadata = [part_utils.IndexMetadatum(idx_cols=['uid', 'imei_norm'],
                                                      is_unique=True,
                                                      partial_sql='WHERE end_date IS NULL')]
            part_utils.add_indices(conn, tbl_name='historic_device_association_list', idx_metadata=idx_metadata)

            # Creating view to historic_barred_list
            cursor.execute("""CREATE OR REPLACE VIEW device_association_list AS
                                   SELECT uid, imei_norm, virt_imei_shard
                                     FROM historic_device_association_list
                                    WHERE end_date IS NULL WITH CHECK OPTION""")  # noqa: Q440
            cursor.execute("""GRANT SELECT ON device_association_list
                                      TO dirbs_core_classify, dirbs_core_api,
                                      dirbs_core_import_device_association_list""")

            # Creating insert trigger function
            cursor.execute("""CREATE FUNCTION device_association_list_staging_data_insert_trigger_fn() RETURNS TRIGGER
                                  LANGUAGE plpgsql
                                  AS $$
                              BEGIN
                                  NEW.uid = NULLIF(TRIM(NEW.uid), '');
                                  NEW.imei_norm = normalize_imei(NULLIF(TRIM(NEW.imei), ''));
                                  RETURN NEW;
                              END
                              $$;

                              ALTER FUNCTION device_association_list_staging_data_insert_trigger_fn()
                                OWNER TO dirbs_core_power_user;
                            """)
            logger.debug('Granting create permission to dirbs_core_import_device_association_list...')
            cursor.execute('GRANT CREATE ON SCHEMA core TO dirbs_core_import_device_association_list')

    def upgrade(self, conn):
        """Overrides AbstractMigrator upgrade method."""
        num_initial_shards = 4

        logger = logging.getLogger('dirbs.db')
        logger.info('Creating historic_device_association_list table...')
        self._migrate_device_association_list(logger, conn)
        logger.info('Association List creation successful.')
        logger.info('Creating historic_monitoring_list table...')
        self._migrate_monitoring_list(logger, conn)
        logger.info('Monitoring List creation successful.')
        self._permit_listgen_on_barred_lists(logger, conn)
        logger.info('Re-partitioning pairing list table...')
        logger.warning('Pairing entries with empty or NULL MSISDN in Operator Data will be skipped...')
        self._repartition_pairing_list(conn, num_physical_shards=num_initial_shards)
        logger.info('Pairing list re-partition successful.')
        logger.info('Re-partitioning exceptions_lists table...')
        self._repartition_exceptions_lists(conn, num_physical_shards=num_initial_shards)
        logger.info('Exceptions Lists repartition successful.')


migrator = SchemaMigrator
