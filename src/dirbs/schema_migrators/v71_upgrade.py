"""
DIRBS DB schema migration script (v70 -> v71).

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

from psycopg2 import sql

import dirbs.schema_migrators
from dirbs.metadata import most_recent_job_start_time_by_command


class HistoricGSMAImportTablesMigrator(dirbs.schema_migrators.AbstractMigrator):
    """Class use to upgrade to V71 of the schema.

    Implement in Python since it requires adding as start_date the last successful job of the importer
    and is harder to do using pure SQL.
    """

    def upgrade(self, db_conn):
        """Overrides AbstractMigrator upgrade method."""
        logger = logging.getLogger('dirbs.db')
        with db_conn.cursor() as cursor:
            logger.info('Creating GSMA historic table...')
            cursor.execute(sql.SQL("""CREATE TABLE historic_gsma_data (
                                          tac character varying(8) NOT NULL,
                                          manufacturer character varying(128),
                                          bands character varying(4096),
                                          allocation_date date,
                                          model_name character varying(1024),
                                          device_type TEXT,
                                          optional_fields jsonb,
                                          rat_bitmask INTEGER,
                                          start_date TIMESTAMP NOT NULL,
                                          end_date TIMESTAMP DEFAULT NULL
                                      );
                                      CREATE UNIQUE INDEX
                                                 ON historic_gsma_data
                                              USING btree (tac)
                                              WHERE (end_date IS NULL)"""))
            logger.info('Created historic table')

            logger.info('Migrating GSMA table to historic GSMA table...')
            gsma_job_start_time = most_recent_job_start_time_by_command(db_conn, 'dirbs-import',
                                                                        subcommand='gsma_tac',
                                                                        successful_only=True)
            if not gsma_job_start_time:
                gsma_job_start_time = datetime.datetime.now()
            cursor.execute(sql.SQL("""INSERT INTO historic_gsma_data(tac,
                                                                     manufacturer,
                                                                     bands,
                                                                     allocation_date,
                                                                     model_name,
                                                                     device_type,
                                                                     optional_fields,
                                                                     rat_bitmask,
                                                                     start_date,
                                                                     end_date)
                                           SELECT tac, manufacturer, bands, allocation_date, model_name, device_type,
                                                  optional_fields, rat_bitmask, %s, NULL
                                             FROM gsma_data;"""), [gsma_job_start_time])
            logger.info('Migrated import tables to historic table')

            logger.info('Dropping old GSMA import table...')
            cursor.execute(sql.SQL("""DROP TABLE gsma_data;"""))
            logger.info('Dropped old GSMA import table')

            logger.info('Creating GSMA materialized view to keep a compatibility with the previous importers ...')
            cursor.execute(sql.SQL("""CREATE MATERIALIZED VIEW gsma_data AS
                                          SELECT tac, manufacturer, bands, allocation_date, model_name,
                                                 device_type, optional_fields, rat_bitmask
                                            FROM historic_gsma_data
                                           WHERE end_date IS NULL WITH DATA;

                                      CREATE UNIQUE INDEX
                                                 ON gsma_data
                                              USING btree (tac)
                                   """))  # noqa Q441
            cursor.execute(sql.SQL("""ALTER MATERIALIZED VIEW gsma_data OWNER TO dirbs_core_import_gsma"""))
            logger.info('Created GSMA view to keep a compatibility with the previous importers...')

            logger.info('Granting privileges on GSMA view and historic table...')
            cursor.execute(sql.SQL("""GRANT SELECT ON historic_gsma_data TO
                                          dirbs_core_import_operator, dirbs_core_classify, dirbs_core_api,
                                          dirbs_core_report;
                                      GRANT SELECT ON gsma_data TO
                                          dirbs_core_import_operator, dirbs_core_classify, dirbs_core_api,
                                          dirbs_core_report, dirbs_core_import_gsma;
                                      GRANT SELECT, INSERT, UPDATE ON historic_gsma_data TO
                                          dirbs_core_import_gsma;"""))
            logger.info('Granted privileges')


migrator = HistoricGSMAImportTablesMigrator
