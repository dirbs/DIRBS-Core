"""
DIRBS Whitelist DB schema migration script (v1).

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


class WhitelistSchemaMigrator(dirbs.schema_migrators.AbstractMigrator):
    """Class used to migrate whitelist schemas into the database."""

    def upgrade(self, conn):
        """Overrides AbstractMigrator upgrade method."""
        logger = logging.getLogger('dirbs.db')
        logger.info('Creating historic_whitelist table...')
        with conn.cursor() as cur:
            # create historic table for whitelist
            cur.execute(sql.SQL("""CREATE TABLE historic_whitelist (
                                       imei_norm text NOT NULL,
                                       associated BOOLEAN DEFAULT FALSE,
                                       eir_id text DEFAULT NULL,
                                       start_date TIMESTAMP NOT NULL,
                                       end_date TIMESTAMP DEFAULT NULL,
                                       virt_imei_shard SMALLINT NOT NULL
                                )
                                PARTITION BY RANGE (virt_imei_shard)"""))

            num_shards = part_utils.num_physical_imei_shards(conn)
            logger.debug('Creating Whitelist child partitions...')
            part_utils.create_imei_shard_partitions(conn, tbl_name='historic_whitelist',
                                                    num_physical_shards=num_shards,
                                                    fillfactor=80)

            # Add indices to each partition
            idx_metadata = [part_utils.IndexMetadatum(idx_cols=['imei_norm'],
                                                      is_unique=True,
                                                      partial_sql='WHERE end_date IS NULL')]
            part_utils.add_indices(conn, tbl_name='historic_whitelist', idx_metadata=idx_metadata)

            # creating views to historic_whitelist
            cur.execute("""CREATE VIEW whitelist AS
                                SELECT imei_norm, associated, eir_id, virt_imei_shard
                                  FROM historic_whitelist
                                 WHERE end_date IS NULL WITH CHECK OPTION""")

            # create view for imeis that are not associated yet
            cur.execute("""CREATE VIEW available_whitelist AS
                                SELECT imei_norm, virt_imei_shard
                                  FROM historic_whitelist
                                 WHERE associated IS FALSE
                                   AND end_date IS NULL WITH CHECK OPTION""")

            # create insert & update trigger on historic_registration_list to update whitelist
            # on update and insert
            cur.execute("""CREATE OR REPLACE FUNCTION insert_whitelist() RETURNS TRIGGER AS
                           $BODY$
                           BEGIN
                               IF new.status = 'whitelist' OR new.status IS NULL THEN
                                INSERT INTO
                                    historic_whitelist (imei_norm, start_date, end_date, virt_imei_shard)
                                    VALUES (new.imei_norm, new.start_date, new.end_date, new.virt_imei_shard);
                               END IF;

                                      RETURN new;
                           END;
                           $BODY$
                           LANGUAGE plpgsql;

                           -- update function
                           CREATE OR REPLACE FUNCTION update_whitelist() RETURNS TRIGGER AS
                           $BODY$
                           BEGIN
                               UPDATE historic_whitelist
                                 SET end_date = new.end_date
                                WHERE imei_norm = new.imei_norm
                                  AND new.end_date IS NOT NULL;

                                  RETURN new;
                           END;
                           $BODY$
                           LANGUAGE plpgsql;

                           -- triggers
                           CREATE TRIGGER wl_insert_trigger AFTER INSERT ON historic_registration_list
                                                                        FOR EACH ROW
                                                                  EXECUTE PROCEDURE insert_whitelist();

                           CREATE TRIGGER wl_update_trigger AFTER UPDATE ON historic_registration_list
                                                                        FOR EACH ROW
                                                                  EXECUTE PROCEDURE update_whitelist();

                           ALTER TYPE job_command_type RENAME TO job_command_type_old;

                           --
                           -- Create type for command
                           --
                           CREATE TYPE job_command_type AS ENUM (
                               'dirbs-catalog',
                               'dirbs-classify',
                               'dirbs-db',
                               'dirbs-import',
                               'dirbs-listgen',
                               'dirbs-prune',
                               'dirbs-report',
                               'dirbs-whitelist'
                           );

                           ALTER TABLE job_metadata ALTER COLUMN command TYPE job_command_type
                              USING command::TEXT::job_command_type;

                           DROP TYPE job_command_type_old;

                           --
                           -- Whitelist notification triggers
                           --
                           CREATE FUNCTION notify_insert_distributor() RETURNS TRIGGER AS
                           $BODY$
                           BEGIN
                               IF new.associated IS FALSE AND new.eir_id IS NULL THEN
                                PERFORM pg_notify('distributor_updates', row_to_json(NEW)::text);
                               END IF;
                               RETURN new;
                           END;
                           $BODY$
                           LANGUAGE plpgsql VOLATILE COST 100;

                           CREATE FUNCTION notify_remove_distributor() RETURNS TRIGGER AS
                           $BODY$
                           BEGIN
                                IF new.end_date IS NOT NULL THEN
                                 PERFORM pg_notify('distributor_updates', row_to_json(NEW)::text);
                                END IF;
                                RETURN new;
                           END;
                           $BODY$
                           LANGUAGE plpgsql VOLATILE COST 100;

                           CREATE TRIGGER notify_insert_trigger AFTER INSERT ON historic_whitelist
                                                                             FOR EACH ROW
                                                                       EXECUTE PROCEDURE notify_insert_distributor();

                           CREATE TRIGGER notify_remove_trigger AFTER UPDATE ON historic_whitelist
                                                                             FOR EACH ROW
                                                                       EXECUTE PROCEDURE notify_remove_distributor();

                           GRANT SELECT ON historic_whitelist TO dirbs_core_import_registration_list;
                           GRANT UPDATE ON historic_whitelist TO dirbs_core_import_registration_list;
                           GRANT INSERT ON historic_whitelist TO dirbs_core_import_registration_list;
                           GRANT INSERT ON historic_whitelist TO dirbs_core_white_list;
                           GRANT UPDATE ON historic_whitelist TO dirbs_core_white_list;
                           GRANT SELECT ON historic_whitelist TO dirbs_core_white_list;
                           GRANT DELETE ON historic_whitelist TO dirbs_core_white_list;
                        """)  # noqa: Q440, Q449, Q441, Q447


migrator = WhitelistSchemaMigrator
