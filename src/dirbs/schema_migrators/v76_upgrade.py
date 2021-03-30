"""
DIRBS DB schema migration script (v75 -> v76).

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
from calendar import monthrange
from datetime import datetime

from psycopg2 import sql

import dirbs.schema_migrators
import dirbs.utils as utils


class HllMigrator(dirbs.schema_migrators.AbstractMigrator):
    """Class use to upgrade to V76 of the schema.

    Implement in Python since it requires partitioning the table and is harder to do using pure SQL.
    Before migration, user need to run as superuser:
        CREATE SCHEMA hll;
        GRANT USAGE ON SCHEMA hll TO dirbs_core_base;
        CREATE EXTENSION hll SCHEMA hll;
    """

    def upgrade(self, db_conn):
        """Overrides AbstractMigrator upgrade method."""
        logger = logging.getLogger('dirbs.db')
        with db_conn.cursor() as cursor:
            # Set search_path to include hll
            cursor.execute('SET search_path = core, hll;')
            cursor.execute("""
                DO $$
                DECLARE
                    database_name TEXT;
                BEGIN
                    SELECT current_database() INTO database_name;
                    -- Set the search path of this database to "core"
                    EXECUTE 'ALTER DATABASE ' || quote_ident(database_name) || ' SET search_path TO core, hll';
                END $$;""")

            logger.info('Creating daily_per_mno_hll_sketches table...')
            with utils.db_role_setter(db_conn, role_name='dirbs_core_import_operator'):
                cursor.execute("""CREATE TABLE daily_per_mno_hll_sketches (
                                      PRIMARY KEY (data_date, operator_id),
                                      data_date         DATE  NOT NULL,
                                      operator_id       TEXT  NOT NULL,
                                      creation_date     DATE  NOT NULL,
                                      triplet_hll       HLL   NOT NULL,
                                      imei_hll          HLL   NOT NULL,
                                      imsi_hll          HLL   NOT NULL,
                                      msisdn_hll        HLL   NOT NULL,
                                      imei_imsis_hll    HLL   NOT NULL,
                                      imei_msisdns_hll  HLL   NOT NULL,
                                      imsi_msisdns_hll  HLL   NOT NULL
                                  )
                                """)
                cursor.execute('GRANT SELECT ON daily_per_mno_hll_sketches TO dirbs_core_report')
                logger.info('Created daily_per_mno_hll_sketches table')

                logger.info('Populating daily_per_mno_hll_sketches from seen_triplets...')
                child_table_names_list = utils.child_table_names(db_conn, 'seen_triplets')

                # Make sure that seen_triplets partitions are owned by dirbs_core_import_operator (they are supposed
                # to be). Previously migration scripts failed to set ownership correctly when tables were re-written
                # and they were incorrectly owned by dirbs_core_power_user.
                with utils.db_role_setter(db_conn, role_name='dirbs_core_power_user'):
                    for p in child_table_names_list:
                        cursor.execute(sql.SQL('ALTER TABLE {0} OWNER TO dirbs_core_import_operator')
                                       .format(sql.Identifier(p)))

                for partition_name in child_table_names_list:
                    logger.info('Populating daily_per_mno_hll_sketches from partition {0}...'.format(partition_name))
                    cursor.execute(sql.SQL('SELECT triplet_year, triplet_month FROM {0} LIMIT 1')
                                   .format(sql.Identifier(partition_name)))
                    res = cursor.fetchone()
                    if res is None:
                        # Table is empty
                        continue

                    year = res.triplet_year
                    month = res.triplet_month
                    days_in_month = monthrange(year, month)[1]
                    triplet_sql_list = []
                    imei_sql_list = []
                    imsi_sql_list = []
                    msisdn_sql_list = []
                    imei_imsis_sql_list = []
                    imei_msisdns_sql_list = []
                    imsi_msisdns_sql_list = []
                    final_select_sql_list = []

                    hll_partition_name = 'hll_{0}'.format(partition_name)
                    cursor.execute(sql.SQL("""CREATE TABLE {0} (PRIMARY KEY (data_date, operator_id),
                                                      LIKE daily_per_mno_hll_sketches)
                                                      INHERITS (daily_per_mno_hll_sketches);
                                                      ALTER TABLE {0} OWNER TO dirbs_core_import_operator
                                           """).format(sql.Identifier(hll_partition_name)))

                    aggregated_data_temp_table = 'temp_{0}'.format(hll_partition_name)
                    base_query = sql.SQL("""CREATE TEMP TABLE {aggregated_data_temp_table_id} AS
                                                SELECT {select_sql}
                                                  FROM {partition_tbl_id}""")

                    for day in range(1, days_in_month + 1):
                        day_literal = sql.Literal(day)
                        triplet_sql_list.append(sql.SQL("""hll_add_agg(hll_hash_text(triplet_hash::TEXT))
                                                           FILTER(WHERE (date_bitmask
                                                                         & (1 << ({day_literal} - 1))) <> 0
                                                              AND imei_norm IS NOT NULL
                                                              AND imsi IS NOT NULL
                                                              AND msisdn IS NOT NULL) AS triplet_day{day_literal}""")
                                                .format(day_literal=day_literal))

                        imei_sql_list.append(sql.SQL("""hll_add_agg(hll_hash_text(imei_norm))
                                                            FILTER(WHERE (date_bitmask
                                                                          & (1 << ({day_literal} - 1))) <> 0
                                                                     AND imei_norm IS NOT NULL)
                                                            AS imei_day{day_literal}""")
                                             .format(day_literal=day_literal))

                        imsi_sql_list.append(sql.SQL("""hll_add_agg(hll_hash_text(imsi))
                                                        FILTER(WHERE (date_bitmask & (1 << ({day_literal} - 1))) <> 0
                                                                 AND imsi IS NOT NULL) AS imsi_day{day_literal}""")
                                             .format(day_literal=day_literal))

                        msisdn_sql_list.append(sql.SQL("""hll_add_agg(hll_hash_text(msisdn))
                                                              FILTER(WHERE (date_bitmask
                                                                            & (1 << ({day_literal} - 1))) <> 0
                                                                       AND msisdn IS NOT NULL)
                                                              AS msisdn_day{day_literal}""")
                                               .format(day_literal=day_literal))

                        imei_imsis_sql_list.append(sql.SQL("""hll_add_agg(hll_hash_text(imei_norm||'$'||imsi))
                                                                  FILTER(WHERE (date_bitmask
                                                                                & (1 << ({day_literal} - 1))) <> 0
                                                                           AND imei_norm IS NOT NULL
                                                                           AND imsi IS NOT NULL)
                                                                  AS imei_imsis_day{day_literal}""")
                                                   .format(day_literal=day_literal))

                        imei_msisdns_sql_list.append(sql.SQL("""hll_add_agg(hll_hash_text(imei_norm||'$'||msisdn))
                                                                    FILTER(WHERE (date_bitmask
                                                                                  & (1 << ({day_literal} - 1))) <> 0
                                                                             AND  imei_norm IS NOT NULL
                                                                             AND  msisdn IS NOT NULL
                                                                           ) AS imei_msisdns_day{day_literal}""")
                                                     .format(day_literal=day_literal))

                        imsi_msisdns_sql_list.append(sql.SQL("""hll_add_agg(hll_hash_text(imsi||'$'||msisdn))
                                                                    FILTER(WHERE (date_bitmask
                                                                                  & (1 << ({day_literal} - 1))) <> 0
                                                                             AND  imsi IS NOT NULL
                                                                             AND  msisdn IS NOT NULL)
                                                                             AS imsi_msisdns_day{day_literal}""")
                                                     .format(day_literal=day_literal))

                    for sql_list in [triplet_sql_list, imei_sql_list, imsi_sql_list, msisdn_sql_list,
                                     imei_imsis_sql_list, imei_msisdns_sql_list, imsi_msisdns_sql_list]:
                        final_select_sql_list.extend(sql_list)

                    final_query = base_query \
                        .format(aggregated_data_temp_table_id=sql.Identifier(aggregated_data_temp_table),
                                select_sql=sql.SQL(', ').join(final_select_sql_list),
                                partition_tbl_id=sql.Identifier(partition_name))

                    cursor.execute(final_query)

                    for day in range(1, days_in_month + 1):
                        str_split = partition_name.split('_')
                        op = str_split[2]
                        job_start_time = datetime.now()
                        day_literal = sql.Literal(day)

                        cursor.execute(sql.SQL("""INSERT INTO {0} (data_date, operator_id, creation_date, triplet_hll,
                                                                   imei_hll, imsi_hll, msisdn_hll, imei_imsis_hll,
                                                                   imei_msisdns_hll, imsi_msisdns_hll)
                                                       SELECT make_date(%s, %s, {day_literal}) AS data_date,
                                                              %s AS operator_id, %s AS creation_date,
                                                              CASE
                                                                  WHEN triplet_day{day_literal} IS NULL
                                                                  THEN hll_empty()
                                                                  ELSE triplet_day{day_literal}
                                                              END AS triplet_hll,
                                                              CASE
                                                                  WHEN imei_day{day_literal} IS NULL THEN hll_empty()
                                                                  ELSE imei_day{day_literal}
                                                              END AS imei_hll,
                                                              CASE
                                                                  WHEN imsi_day{day_literal} IS NULL THEN hll_empty()
                                                                  ELSE imsi_day{day_literal}
                                                              END AS imsi_hll,
                                                              CASE
                                                                  WHEN msisdn_day{day_literal} IS NULL THEN hll_empty()
                                                                  ELSE msisdn_day{day_literal}
                                                              END AS msisdn_hll,
                                                              CASE
                                                                  WHEN imei_imsis_day{day_literal} IS NULL
                                                                  THEN hll_empty()
                                                                  ELSE imei_imsis_day{day_literal}
                                                              END AS imei_imsis_hll,
                                                              CASE
                                                                  WHEN imei_msisdns_day{day_literal} IS NULL
                                                                  THEN hll_empty()
                                                                  ELSE imei_msisdns_day{day_literal}
                                                              END AS imei_msisdns_hll,
                                                              CASE
                                                                  WHEN imsi_msisdns_day{day_literal} IS NULL
                                                                  THEN hll_empty()
                                                                  ELSE imsi_msisdns_day{day_literal}
                                                              END AS imsi_msisdns_hll

                                                         FROM {1}""")
                                       .format(sql.Identifier(hll_partition_name),
                                               sql.Identifier(aggregated_data_temp_table),
                                               day_literal=day_literal),
                                       [year, month, op, job_start_time])

                    logger.info('Populated daily_per_mno_hll_sketches from partition {0}'.format(partition_name))

            logger.info('Populated daily_per_mno_hll_sketches from seen_triplets')


migrator = HllMigrator
