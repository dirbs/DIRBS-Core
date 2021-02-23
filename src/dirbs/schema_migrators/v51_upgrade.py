"""
DIRBS DB schema migration script (v50 -> v51).

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


class SeenIMEIsLastSeenDateMigrator(dirbs.schema_migrators.AbstractMigrator):
    """Class use to upgrade to V51 of the schema (seen_imeis).

    Implement in Python since it requires adding last_seen date to all seen_triplets partitions
    and is harder to do using pure SQL.
    """

    def upgrade(self, db_conn):
        """Overrides AbstractMigrator upgrade method."""
        logger = logging.getLogger('dirbs.db')
        with db_conn.cursor() as cursor:
            # _create_seen_imeis_partition creates a table LIKE seen_imeis that needs to have the new col as well
            child_table_names_list = utils.child_table_names(db_conn, 'seen_imeis')
            logger.info('Adding last_seen date to all partitions of seen_imeis table: '
                        '{0}'.format(', '.join(child_table_names_list)))

            cursor.execute("""ALTER TABLE seen_imeis ADD COLUMN last_seen date DEFAULT NULL""")

            for c in child_table_names_list:
                logger.info('Adding last_seen date value to table {0}...'.format(c))

                cursor.execute(sql.SQL("""SELECT operator_id FROM {0} LIMIT 1""")
                               .format(sql.Identifier(c)))

                res = cursor.fetchone()
                if res:
                    operator_id = res.operator_id
                    logger.info('Setting last_seen value to max seen_date in partition {0}...'.format(c))

                    cursor.execute(sql.SQL("""UPDATE {0} s1
                                                 SET last_seen = s2.max_seen
                                                FROM
                                                     (SELECT imei_norm, MAX(last_seen) AS max_seen
                                                        FROM seen_triplets_no_null_imeis
                                                       WHERE operator_id = %s
                                                    GROUP BY imei_norm)s2
                                               WHERE s1.imei_norm = s2.imei_norm""")
                                   .format(sql.Identifier(c)), [operator_id])

                    logger.info('Set last_seen value to max last_seen date')

                else:
                    logger.info('Skipped setting last_seen values in partition {0} as is empty'.format(c))

                logger.info('Setting any last_seen values that are NULL to the first_seen value in partition {0}'
                            '...'.format(c))

                cursor.execute(sql.SQL("""UPDATE {0}
                                             SET last_seen = first_seen
                                           WHERE last_seen IS NULL""").format(sql.Identifier(c)))

                logger.info('Set any last_seen values that are NULL to the first_seen value')
                logger.info('Added last_seen date value to table {0}'.format(c))

            cursor.execute("""ALTER TABLE seen_imeis ALTER COLUMN last_seen DROP DEFAULT""")
            cursor.execute("""ALTER TABLE seen_imeis ALTER COLUMN last_seen SET NOT NULL""")
            logger.info('Added last_seen date to all partitions of seen_imeis table')


migrator = SeenIMEIsLastSeenDateMigrator
