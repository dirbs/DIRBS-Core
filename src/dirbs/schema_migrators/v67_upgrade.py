"""
DIRBS DB schema migration script (v66 -> v67).

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


class HistoricImportTablesMigrator(dirbs.schema_migrators.AbstractMigrator):
    """Class use to upgrade to V67 of the schema.

    Implement in Python since it requires adding as start_date the last successful job of the importer
    and is harder to do using pure SQL.
    """

    def upgrade(self, db_conn):
        """Overrides AbstractMigrator upgrade method."""
        logger = logging.getLogger('dirbs.db')
        with db_conn.cursor() as cursor:
            logger.info('Creating historic tables...')
            cursor.execute(sql.SQL("""CREATE TABLE historic_stolen_list (
                                          imei_norm text NOT NULL,
                                          reporting_date DATE DEFAULT NULL,
                                          start_date TIMESTAMP NOT NULL,
                                          end_date TIMESTAMP DEFAULT NULL
                                      );
                                      CREATE UNIQUE INDEX
                                                 ON historic_stolen_list
                                              USING btree (imei_norm)
                                              WHERE (end_date IS NULL);

                                      CREATE TABLE historic_pairing_list (
                                          imei_norm text NOT NULL,
                                          imsi text NOT NULL,
                                          start_date TIMESTAMP NOT NULL,
                                          end_date TIMESTAMP DEFAULT NULL
                                      );
                                      CREATE UNIQUE INDEX
                                                ON historic_pairing_list
                                             USING btree (imei_norm, imsi)
                                             WHERE (end_date IS NULL);

                                      CREATE TABLE historic_golden_list (
                                          hashed_imei_norm UUID NOT NULL,
                                          start_date TIMESTAMP NOT NULL,
                                          end_date TIMESTAMP DEFAULT NULL
                                      );
                                      CREATE UNIQUE INDEX
                                                 ON historic_golden_list
                                              USING btree (hashed_imei_norm)
                                              WHERE (end_date IS NULL);

                                      CREATE TABLE historic_registration_list (
                                          imei_norm text NOT NULL,
                                          start_date TIMESTAMP NOT NULL,
                                          end_date TIMESTAMP DEFAULT NULL
                                      );
                                      CREATE UNIQUE INDEX
                                                 ON historic_registration_list
                                              USING btree (imei_norm)
                                              WHERE (end_date IS NULL);"""))
            logger.info('Created historic tables')

            logger.info('Start migrating import tables to historic tables...')
            logger.info('Migrating stolen_list table to historic_stolen_list table...')
            stolen_job_start_time = most_recent_job_start_time_by_command(db_conn, 'dirbs-import',
                                                                          subcommand='stolen_list',
                                                                          successful_only=True)
            if not stolen_job_start_time:
                stolen_job_start_time = datetime.datetime.now()
            cursor.execute(sql.SQL("""INSERT INTO historic_stolen_list(imei_norm, reporting_date, start_date, end_date)
                                           SELECT imei_norm, reporting_date, %s, NULL
                                             FROM stolen_list;"""), [stolen_job_start_time])

            logger.info('Migrating pairing_list table to historic_pairing_list table...')
            pairing_job_start_time = most_recent_job_start_time_by_command(db_conn, 'dirbs-import',
                                                                           subcommand='pairing_list',
                                                                           successful_only=True)
            if not pairing_job_start_time:
                pairing_job_start_time = datetime.datetime.now()
            cursor.execute(sql.SQL("""INSERT INTO historic_pairing_list(imei_norm, imsi, start_date, end_date)
                                           SELECT imei_norm, imsi, %s, NULL
                                             FROM pairing_list;"""), [pairing_job_start_time])

            logger.info('Migrating registration_list table to historic_registration_list table...')
            registration_job_start_time = most_recent_job_start_time_by_command(db_conn, 'dirbs-import',
                                                                                subcommand='registration_list',
                                                                                successful_only=True)
            if not registration_job_start_time:
                registration_job_start_time = datetime.datetime.now()
            cursor.execute(sql.SQL("""INSERT INTO historic_registration_list(imei_norm, start_date, end_date)
                                           SELECT imei_norm, %s, NULL
                                             FROM registration_list;"""), [registration_job_start_time])

            logger.info('Migrating golden_list table to historic_golden_list table...')
            golden_job_start_time = most_recent_job_start_time_by_command(db_conn, 'dirbs-import',
                                                                          subcommand='golden_list',
                                                                          successful_only=True)
            if not golden_job_start_time:
                golden_job_start_time = datetime.datetime.now()
            cursor.execute(sql.SQL("""INSERT INTO historic_golden_list(hashed_imei_norm, start_date, end_date)
                                           SELECT hashed_imei_norm, %s, NULL
                                             FROM golden_list;"""), [golden_job_start_time])
            logger.info('Migrated all the import tables to historic tables')

            logger.info('Dropping old import tables...')
            cursor.execute(sql.SQL("""DROP TABLE pairing_list;
                                      DROP TABLE stolen_list;
                                      DROP TABLE golden_list;
                                      DROP TABLE registration_list;"""))
            logger.info('Dropped old import tables')

            logger.info('Creating views to keep a compatibility with the previous importers ...')
            cursor.execute(sql.SQL("""CREATE VIEW pairing_list AS
                                          SELECT imei_norm, imsi
                                            FROM historic_pairing_list
                                           WHERE end_date IS NULL WITH CHECK OPTION;

                                      CREATE VIEW stolen_list AS
                                          SELECT imei_norm, reporting_date
                                            FROM historic_stolen_list
                                           WHERE end_date IS NULL WITH CHECK OPTION;

                                      CREATE VIEW golden_list AS
                                          SELECT hashed_imei_norm
                                            FROM historic_golden_list
                                           WHERE end_date IS NULL WITH CHECK OPTION;

                                      CREATE VIEW registration_list AS
                                          SELECT imei_norm
                                            FROM historic_registration_list
                                           WHERE end_date IS NULL WITH CHECK OPTION;"""))
            logger.info('Created views')

            logger.info('Granting privileges on views and historic tables...')
            cursor.execute(sql.SQL("""GRANT SELECT ON historic_pairing_list TO
                                          dirbs_core_listgen,
                                          dirbs_core_report,
                                          dirbs_core_api;
                                      GRANT SELECT ON pairing_list TO
                                          dirbs_core_listgen,
                                          dirbs_core_report,
                                          dirbs_core_api,
                                          dirbs_core_import_pairing_list;
                                      GRANT SELECT, INSERT, UPDATE ON historic_pairing_list TO
                                          dirbs_core_import_pairing_list;
                                      GRANT SELECT ON historic_stolen_list TO dirbs_core_classify;
                                      GRANT SELECT ON stolen_list TO
                                          dirbs_core_classify,
                                          dirbs_core_import_stolen_list;
                                      GRANT SELECT, INSERT, UPDATE ON historic_stolen_list TO
                                          dirbs_core_import_stolen_list;
                                      GRANT SELECT ON historic_golden_list TO dirbs_core_listgen;
                                      GRANT SELECT ON golden_list TO
                                          dirbs_core_listgen,
                                          dirbs_core_import_golden_list;
                                      GRANT SELECT, INSERT, UPDATE ON historic_golden_list TO
                                          dirbs_core_import_golden_list;
                                      GRANT SELECT ON historic_registration_list TO
                                          dirbs_core_classify,
                                          dirbs_core_api;
                                      GRANT SELECT ON registration_list TO
                                          dirbs_core_classify,
                                          dirbs_core_api,
                                          dirbs_core_import_registration_list;
                                      GRANT SELECT, INSERT, UPDATE ON historic_registration_list TO
                                          dirbs_core_import_registration_list;"""))
            logger.info('Granted privileges')


migrator = HistoricImportTablesMigrator
