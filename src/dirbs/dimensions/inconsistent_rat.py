"""
DIRBS dimension function for IMEIs connecting to RAT greater than device capability based on GSMA TAC database.

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

from psycopg2 import sql

import dirbs.partition_utils as partition_utils
from .base import Dimension


class InconsistentRAT(Dimension):
    """Implementation of the InconsistentRAT classification dimension."""

    def _matching_imeis_sql(self, conn, app_config, virt_imei_range_start, virt_imei_range_end, curr_date=None):
        """
        Overrides Dimension._matching_imeis_sql.

        :param conn: database connection
        :param app_config: dirbs config obj
        :param virt_imei_range_start: virtual imei shard range start
        :param virt_imei_range_end: virtual imei shard range end
        :param curr_date: user defined current date
        :return: SQL
        """
        """ Compute the RAT bitmask on a per model level by OR'ing all the TAC bitmasks with
        the same model name. Check individually that device was seen on 2G/3G/4G RAT and if model is
        capable of that particular RAT.
        The first condition in the where clause ANDs the device_rat_bitmask with 48 (bits 4, 5 set) to
        get the operator_rank bits corresponding to 2G RATs. If this value is greater than 0, then
        device was observed on 2G RATs. The model_rat_bitmask is AND with 64 (bit 6 set) to
        get gsma_rank corresponding to 2G RAT. If this value is zero, then device does not have 3G capability.
        The second condition in the where clause ANDs the device_rat_bitmask with 960 (bits 6, 7, 8, 9 set) to
        get the operator_rank bits corresponding to 3G RATs. If this value is greater than 0, then
        device was observed on 3G RATs. The model_rat_bitmask is AND with 512 (bit 9 set) to
        get gsma_rank corresponding to 3G RAT. If this value is zero, then device does not have 3G capability.
        The third condition in the where clause ANDs the device_rat_bitmask with 7168 (bits 10, 11, 12 set) to
        get the operator_rank bits corresponding to 4G RATs. If this value is greater than 0, then
        device was observed on 4G RATs. The model_rat_bitmask is AND with 4096 (bit 12 set) to
        get gsma_rank corresponding to 4G RAT. If this value is zero, then device does not have 4G capability.
        If device was seen on a RAT that device is not capable of then it is flagged for having inconsistent RAT.
        IMEIs associated with TACs having NULL manufacturer or model name are excluded from classification.
        """
        network_imeis_shard = partition_utils.imei_shard_name(base_name='network_imeis',
                                                              virt_imei_range_start=virt_imei_range_start,
                                                              virt_imei_range_end=virt_imei_range_end)

        return sql.SQL(
            """SELECT imei_norm
                 FROM (SELECT imei_norm,
                              SUBSTRING(imei_norm FROM 1 FOR 8) AS tac,
                              seen_rat_bitmask AS device_rat_bitmask
                         FROM {network_imeis_shard}
                        WHERE seen_rat_bitmask IS NOT NULL) imei_rat
                 JOIN (SELECT gsma_tacs.tac,
                              gsma_per_model_rat_bitmask.model_rat_bitmask
                         FROM (SELECT model_name,
                                      manufacturer,
                                      bit_or(rat_bitmask) AS model_rat_bitmask
                                 FROM gsma_data
                                WHERE model_name IS NOT NULL
                                  AND manufacturer IS NOT NULL
                             GROUP BY model_name, manufacturer) gsma_per_model_rat_bitmask
                                 JOIN gsma_data gsma_tacs
                                USING (model_name, manufacturer)) gsma_per_tac_bitmask
                           ON imei_rat.tac = gsma_per_tac_bitmask.tac
                WHERE ((device_rat_bitmask & 48) > 0 AND (model_rat_bitmask & 64) = 0)
                   OR ((device_rat_bitmask & 960) > 0 AND (model_rat_bitmask & 512) = 0)
                   OR ((device_rat_bitmask & 7168) > 0 AND (model_rat_bitmask & 4096) = 0)""").format(  # noqa: Q447
            network_imeis_shard=sql.Identifier(network_imeis_shard)).as_string(conn)


dimension = InconsistentRAT
