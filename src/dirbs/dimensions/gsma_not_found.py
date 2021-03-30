"""
DIRBS dimension function for a TAC not found in the GSMA TAC DB.

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

import re

from psycopg2 import sql

from dirbs.utils import compute_analysis_end_date
import dirbs.partition_utils as partition_utils
from .base import Dimension


class GSMANotFound(Dimension):
    """Implementation of the GSMANotFound classification dimension."""

    def __init__(self, *, per_rbi_delays=None, ignore_rbi_delays=False, **kwargs):
        """
        Constructor.

        :param per_rbi_delays: user defined rbi delays values
        :param ignore_rbi_delays: flag to ignore rbi delay
        :param kwargs: kwargs
        """
        super().__init__(**kwargs)

        if not isinstance(ignore_rbi_delays, bool):
            raise ValueError("\'ignore_rbi_delays\' parameter should be boolean.")

        # Default values for RBI delays based on GSMA DB allocation-appearance lag analysis
        default_rbi_delays = {
            '00': 32,
            '01': 40,
            '35': 20,
            '86': 19,
            '91': 20,
            '99': 69
        }

        if per_rbi_delays is None:
            per_rbi_delays = {}
        elif ignore_rbi_delays:
            raise ValueError("\'ignore_rbi_delays\' parameter cannot be set to True when \'per_rbi_delays\' "
                             'parameter is also defined!')

        # Validate that the keys are all 2 - digit strings
        invalid_keys = [k for k in per_rbi_delays.keys() if not re.search(r'^\d{2}$', k)]
        if len(invalid_keys) > 0:
            raise ValueError("Invalid entry in \'per_rbi_delays\' parameter. RBI value must be a 2-digit string.")

        # Validate that the values are all integers
        invalid_values = [v for v in per_rbi_delays.values() if not isinstance(v, int) or v < 0]
        if len(invalid_values) > 0:
            raise ValueError("Invalid entry in \'per_rbi_delays\' parameter. RBI delay value must be an integer.")

        self.final_rbi_delays = {**default_rbi_delays, **per_rbi_delays} if not ignore_rbi_delays else {}

    def _matching_imeis_sql(self, conn, app_config, virt_imei_range_start, virt_imei_range_end, curr_date=None):
        """
        Overrides Dimension._matching_imeis_sql.

        :param conn: database connection
        :param app_config: dirbs config obj
        :param virt_imei_range_start: virtual imei shard range start
        :param virt_imei_range_end: virtual imei shard range end
        :param curr_date: user defined current date for analysis
        :return: SQL
        """
        analysis_end_date = compute_analysis_end_date(conn, curr_date)
        rbi_list = [rbi for rbi in self.final_rbi_delays.keys()]
        delay_list = [self.final_rbi_delays[rbi] for rbi in rbi_list]

        # HACK: This is used by _write_country_gsma_not_found_report in cli/report.py which instantiates
        # a dimension without understanding about paralllel queries. Therefore, we passed in 1 and 100
        # to cover the entire range of IMEIs and expect it to read from the network_imeis table.
        if virt_imei_range_start == 1 and virt_imei_range_end == 100:
            network_imeis_shard = 'network_imeis'
        else:
            network_imeis_shard = partition_utils.imei_shard_name(base_name='network_imeis',
                                                                  virt_imei_range_start=virt_imei_range_start,
                                                                  virt_imei_range_end=virt_imei_range_end)

        # The first CTE 'not_in_gsma' calculates the first date the IMEI was observed on the network for
        # all IMEIs that have a TAC that is not present in the GSMA database. The min_first_seen date
        # is computed using the minimum of first_seen date among all operators the IMEI was observed on.
        # The second CTE 'rbi_delays' is the list of RBIs and corresponding delays that were configured.
        # The results of the two CTEs are joined using RBI as the key.
        # Finally, those IMEIs whose min_seen_date + RBI delay is less than curr_date are classified
        # as gsma_not_found and rest are excluded.
        # Note 1: The less than check implies that even after adding delay, these IMEIs would still not
        # have been allocated by the classification date and hence are not valid IMEIs.
        # Note 2: The delay is added on a per-IMEI basis rather than per-TAC due to potential for someone
        # squatting on an unallocated TAC in the past.
        return sql.SQL(
            """SELECT imei_norm
                 FROM (WITH not_in_gsma AS (SELECT imei_norm,
                                                   first_seen AS min_first_seen,
                                                   LEFT(imei_norm, 2) AS rbi
                                              FROM {network_imeis_shard}
                                             WHERE NOT EXISTS (SELECT 1
                                                                 FROM gsma_data
                                                                WHERE tac = LEFT(imei_norm, 8))),
                             rbi_delays AS (SELECT rbi,
                                                   delay
                                              FROM UNNEST({rbi_list}::TEXT[], {delay_list}::INT[]) AS tbl(rbi, delay))
                     SELECT imei_norm
                       FROM not_in_gsma
                  LEFT JOIN rbi_delays
                      USING (rbi)
                      WHERE min_first_seen + COALESCE(delay, 0) < {analysis_end_date}) invalid_imeis
            """).format(network_imeis_shard=sql.Identifier(network_imeis_shard),  # noqa: Q447, Q449
                        rbi_list=sql.Literal(rbi_list),
                        delay_list=sql.Literal(delay_list),
                        analysis_end_date=sql.Literal(analysis_end_date)).as_string(conn)


dimension = GSMANotFound
