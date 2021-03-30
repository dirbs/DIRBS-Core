"""
DIRBS DB schema migration script (v78 -> v79).

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

import dirbs.schema_migrators
import dirbs.partition_utils as partition_utils


class StolenListPartitionMigrator(dirbs.schema_migrators.AbstractMigrator):
    """Class use to upgrade to V79 of the schema.

    Implement in Python since it requires repartitioning the stolen_list
    """

    def upgrade(self, db_conn):
        """Overrides AbstractMigrator upgrade method."""
        with db_conn.cursor() as cursor:
            logger = logging.getLogger('dirbs.db')
            logger.info('Re-partitioning stolen_list table...')
            cursor.execute('ALTER TABLE historic_stolen_list ADD COLUMN virt_imei_shard SMALLINT')
            cursor.execute('UPDATE historic_stolen_list SET virt_imei_shard = calc_virt_imei_shard(imei_norm)')
            cursor.execute('ALTER TABLE historic_stolen_list ALTER COLUMN virt_imei_shard SET NOT NULL')
            num_shards = partition_utils.num_physical_imei_shards(db_conn)
            partition_utils.repartition_stolen_list(db_conn, num_physical_shards=num_shards)
            logger.info('Re-partitioned stolen_list table')

            # Now that we can create tables during classification, we need to allow dirbs_core_classify to
            # create tables
            cursor.execute('GRANT CREATE ON SCHEMA core TO dirbs_core_classify')


migrator = StolenListPartitionMigrator
