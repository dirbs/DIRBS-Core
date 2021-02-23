"""
DIRBS DB schema migration script (v83 -> v84).

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
import dirbs.partition_utils as part_utils


class SchemaMigrator(dirbs.schema_migrators.AbstractMigrator):
    """Class use to upgrade to V84 of the schema."""

    def _define_unique_bitcount_func(self, logger, conn):
        """Helper method to define unique_bitcount sql function."""
        logger.debug('Defining unique_bitcount() function...')
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("""CREATE FUNCTION unique_bitcount(n INTEGER) RETURNS INTEGER
                                          LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
                                          AS $$
                                          BEGIN
                                              IF (SELECT bitcount(n)) > 0
                                              THEN
                                                  RETURN 1;
                                              ELSE
                                                  RETURN 0;
                                              END IF;
                                          END;
                                    $$;
                                    """))
        logger.debug('unique_bitcount() function definition successful')

    def _create_index_on_device_id(self, logger, conn):
        """Method to create index on device_id in registration_list."""
        logger.info('Creating index on device_id in historic_registration_list...')
        idx_metadata = [part_utils.IndexMetadatum(idx_cols=['device_id'],
                                                  is_unique=False,
                                                  partial_sql='WHERE end_date IS NULL')]
        part_utils.add_indices(conn, tbl_name='historic_registration_list', idx_metadata=idx_metadata)

    def _migrate_barred_list(self, logger, conn):
        """Method to migrate barred imeis list."""
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("""CREATE TABLE historic_barred_list (
                                          imei_norm text NOT NULL,
                                          start_date TIMESTAMP NOT NULL,
                                          end_date TIMESTAMP,
                                          virt_imei_shard SMALLINT NOT NULL
                                     )
                                     PARTITION BY RANGE (virt_imei_shard)"""))

            num_shards = part_utils.num_physical_imei_shards(conn)
            logger.debug('Granting permissions to barred_list partitions...')
            part_utils._grant_perms_barred_list(conn, part_name='historic_barred_list')
            logger.debug('Creating barred_list child partitions...')
            part_utils.create_imei_shard_partitions(conn, tbl_name='historic_barred_list',
                                                    num_physical_shards=num_shards,
                                                    perms_func=part_utils._grant_perms_barred_list,
                                                    fillfactor=80)

            # Add indexes to each partition
            idx_metadata = [part_utils.IndexMetadatum(idx_cols=['imei_norm'],
                                                      is_unique=True,
                                                      partial_sql='WHERE end_date IS NULL')]
            part_utils.add_indices(conn, tbl_name='historic_barred_list', idx_metadata=idx_metadata)

            # Creating view to historic_barred_list
            cursor.execute("""CREATE OR REPLACE VIEW barred_list AS
                                   SELECT imei_norm, virt_imei_shard
                                     FROM historic_barred_list
                                    WHERE end_date IS NULL WITH CHECK OPTION""")
            cursor.execute("""GRANT SELECT ON barred_list
                                      TO dirbs_core_classify, dirbs_core_api, dirbs_core_import_barred_list""")

            # Creating insert trigger function
            cursor.execute("""CREATE FUNCTION barred_list_staging_data_insert_trigger_fn() RETURNS TRIGGER
                                  LANGUAGE plpgsql
                                  AS $$
                              BEGIN
                                  NEW.imei_norm = normalize_imei(NULLIF(TRIM(NEW.imei), ''));
                                  RETURN NEW;
                              END
                              $$;

                              ALTER FUNCTION barred_list_staging_data_insert_trigger_fn()
                                OWNER TO dirbs_core_power_user;
                            """)
            logger.debug('Granting create permission to dirbs_core_import_barred_list...')
            cursor.execute('GRANT CREATE ON SCHEMA core TO dirbs_core_import_barred_list')

    def _migrate_barred_tac_list(self, logger, conn):
        """Method to migrate barred tac list."""
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("""CREATE TABLE historic_barred_tac_list (
                                          tac character varying(8) NOT NULL,
                                          start_date TIMESTAMP NOT NULL,
                                          end_date TIMESTAMP DEFAULT NULL
                                          );
                                          CREATE UNIQUE INDEX
                                                     ON historic_barred_tac_list
                                                  USING btree (tac)
                                                  WHERE (end_date IS NULL)"""))
            logger.debug('Created historic_barred_tac_list with indexing.')

            # Creating view to historic table
            cursor.execute("""CREATE VIEW barred_tac_list AS
                                   SELECT tac
                                     FROM historic_barred_tac_list
                                    WHERE end_date IS NULL WITH CHECK OPTION""")
            logger.debug('Created view on historic_barred_tac_list')

            # Granting permissions
            cursor.execute("""GRANT SELECT ON barred_tac_list
                                      TO dirbs_core_classify, dirbs_core_api, dirbs_core_import_barred_tac_list""")
            cursor.execute('GRANT CREATE ON SCHEMA core TO dirbs_core_import_barred_tac_list')
            logger.debug('Granted permissions on barred_tac_list')

    def _migrate_subscriber_registration_list(self, logger, conn):
        """Method to migrate/create subscriber registration list table."""
        with conn.cursor() as cursor:
            # create subscriber registration
            cursor.execute(sql.SQL("""CREATE TABLE historic_subscribers_registration_list (
                                          uid CHARACTER VARYING(20) NOT NULL,
                                          imsi TEXT NOT NULL,
                                          start_date TIMESTAMP NOT NULL,
                                          end_date TIMESTAMP DEFAULT NULL
                                          );
                                          CREATE UNIQUE INDEX
                                                ON historic_subscribers_registration_list
                                             USING btree (uid, imsi)
                                             WHERE (end_date IS NULL)"""))
            logger.debug('Created historic_subscriber_registration_list table')

            # creating view to historic table
            cursor.execute(sql.SQL("""CREATE VIEW subscribers_registration_list AS
                                           SELECT uid, imsi
                                             FROM historic_subscribers_registration_list
                                            WHERE end_date IS NULL WITH CHECK OPTION"""))  # noqa: Q440
            logger.debug('Created view on historic_subscriber_registration_list')

            # granting permissions
            cursor.execute("""GRANT SELECT ON subscribers_registration_list
                                      TO dirbs_core_classify, dirbs_core_import_subscribers_registration_list""")

            logger.debug('Granting create permission to dirbs_core_import_subscriber_registration_list...')
            cursor.execute('GRANT CREATE ON SCHEMA core TO dirbs_core_import_subscribers_registration_list')

    def upgrade(self, conn):
        """Overrides AbstractMigrator upgrade method."""
        logger = logging.getLogger('dirbs.db')
        logger.info('Creating historic barred_list table...')
        self._migrate_barred_list(logger, conn)
        logger.info('Barred List creation successful')
        logger.info('Creating historic_barred_tac_list table...')
        self._migrate_barred_tac_list(logger, conn)
        logger.info('Created historic_barred_tac_list table')
        logger.info('Creating historic_subscriber_registration_list table...')
        self._migrate_subscriber_registration_list(logger, conn)
        logger.info('Created historic_subscriber_registration_list')
        self._define_unique_bitcount_func(logger, conn)
        self._create_index_on_device_id(logger, conn)


migrator = SchemaMigrator
