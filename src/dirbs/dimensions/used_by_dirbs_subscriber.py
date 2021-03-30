"""
DIRBS dimension function for a IMEIs used by DIRBS subscribers.

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
import datetime

from psycopg2 import sql

from dirbs.utils import compute_analysis_end_date
from .base import Dimension


class UsedByDirbsSubscriber(Dimension):
    """Implementation of the UsedByDirbsSubscriber classification dimension."""

    def __init__(self, *, lookback_days, **kwargs):
        """
        Constructor.

        :param lookback_days: number of days to look back for analysis
        :param kwargs: kwargs
        """
        super().__init__(**kwargs)

        try:
            self._lookback_days = int(lookback_days)
        except (TypeError, ValueError):
            raise ValueError("\'lookback_days\' parameter must be an integer, "
                             "got \'{0}\' instead...".format(lookback_days))

    @property
    def algorithm_name(self):
        """Overrides Dimension.algorithm_name."""
        return 'Used by DIRBS subscriber'

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
        analysis_end_date = compute_analysis_end_date(conn, curr_date)
        analysis_start_date = analysis_end_date - datetime.timedelta(days=self._lookback_days)
        self._log_analysis_window(analysis_start_date, analysis_end_date)
        operators = app_config.region_config.operators
        mcc_mnc_pairs = [op.mcc_mnc_pairs for op in operators]
        flat_mcc_mnc_pairs = [val for sublist in mcc_mnc_pairs for val in sublist]
        mcc_mnc_pairs_list = list({'{0}{1}%'.format(x['mcc'], x['mnc']) for x in flat_mcc_mnc_pairs})

        return sql.SQL(
            """SELECT imei_norm
                 FROM monthly_network_triplets_country
                WHERE imei_norm IS NOT NULL
                  AND last_seen >= {analysis_start_date}
                  AND first_seen < {analysis_end_date}
                  AND virt_imei_shard >= {virt_imei_range_start}
                  AND virt_imei_shard < {virt_imei_range_end}
                  AND starts_with_prefix(imsi, {mcc_mnc_list})
            """).format(analysis_start_date=sql.Literal(analysis_start_date),
                        analysis_end_date=sql.Literal(analysis_end_date),
                        virt_imei_range_start=sql.Literal(virt_imei_range_start),
                        virt_imei_range_end=sql.Literal(virt_imei_range_end),
                        mcc_mnc_list=sql.Literal(mcc_mnc_pairs_list)).as_string(conn)


dimension = UsedByDirbsSubscriber
