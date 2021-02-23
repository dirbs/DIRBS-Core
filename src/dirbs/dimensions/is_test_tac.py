"""
This function is used for determining whether an IMEI's TAC component belongs to the ranges associated with Test TACs.

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


class IsTestTAC(Dimension):
    """Implementation of the IsTestTAC classification dimension."""

    def _matching_imeis_sql(self, conn, app_config, virt_imei_range_start, virt_imei_range_end, curr_date=None):
        """
        Overrides Dimension._matching_imeis_sql.

        First six digits of the Test IMEI features:
        - first 2 digits are '00';
        - exclude IMEIs with characters;
        - the third and fourth digits can be either:
              '10' followed by two digits both between 1 and 17
           OR '44', '86' or '91'

        e.g. first six digits of the Test IMEI :
        001 001-
        001 017

        00 44
        00 86
        00 91

        :param conn: database connection
        :param app_config: dirbs config obj
        :param virt_imei_range_start: virtual imei shard range start
        :param virt_imei_range_end: virtual imei shard range end
        :param curr_date: user defined current date
        :return: SQL
        """
        """Overrides Dimension._matching_imeis_sql."""
        network_imeis_shard = partition_utils.imei_shard_name(base_name='network_imeis',
                                                              virt_imei_range_start=virt_imei_range_start,
                                                              virt_imei_range_end=virt_imei_range_end)

        return sql.SQL(
            """SELECT imei_norm
                 FROM {network_imeis_shard}
                WHERE SUBSTRING(imei_norm, 1, 2) = '00'
                  AND imei_norm ~ '^[0-9]{{8}}'
                  AND (
                          (
                             SUBSTRING(imei_norm, 3, 2) = '10'
                                   AND
                             SUBSTRING(imei_norm, 5, 2)::INT BETWEEN 1 AND 17
                          )
                          OR SUBSTRING(imei_norm, 3, 2) IN ('44', '86', '91')
                      )
            """).format(network_imeis_shard=sql.Identifier(network_imeis_shard)).as_string(conn)  # noqa: Q447


dimension = IsTestTAC
