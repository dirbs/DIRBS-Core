"""
DIRBS DB schema migration script (v77 -> v78).

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


class RemoveTripletsChildTblMigrator(dirbs.schema_migrators.AbstractMigrator):
    """Class use to upgrade to V78 of the schema.

    Implement in Python since it requires iterating among all daily_per_mno_hll_sketches partitions
    and is harder to do using pure SQL.
    """

    def upgrade(self, db_conn):
        """Overrides AbstractMigrator upgrade method."""
        logger = logging.getLogger('dirbs.db')
        with db_conn.cursor() as cursor, utils.db_role_setter(db_conn, role_name='dirbs_core_import_operator'):
            child_table_names_list = utils.child_table_names(db_conn, 'daily_per_mno_hll_sketches')
            for partition_name in child_table_names_list:
                logger.info('Copying partition {0} into daily_per_mno_hll_sketches table...'
                            .format(partition_name))
                cursor.execute(sql.SQL("""INSERT INTO daily_per_mno_hll_sketches
                                               SELECT *
                                                 FROM {partition_name_id}""")
                               .format(partition_name_id=sql.Identifier(partition_name)))
                logger.info('Copied partition {0} into daily_per_mno_hll_sketches table'
                            .format(partition_name))

                logger.info('Dropping daily_per_mno_hll_sketches partition {0}...'.format(partition_name))
                cursor.execute(sql.SQL("""DROP TABLE {partition_name_id}""")
                               .format(partition_name_id=sql.Identifier(partition_name)))
                logger.info('Dropped partition {0}'.format(partition_name))


migrator = RemoveTripletsChildTblMigrator
