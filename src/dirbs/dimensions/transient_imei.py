"""
DIRBS dimension function for transient IMEIs.

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

from dateutil import relativedelta
from psycopg2 import sql

from dirbs.utils import compute_analysis_end_date
from .base import Dimension


class TransientIMEI(Dimension):
    """Implementation of the TransientIMEI classification dimension."""

    def __init__(self, *, period=None, num_msisdns=None, **kwargs):
        """Constructor.

        Arguments:
            period: analysis period in days
            sequence_check: if enabled, will check for possible sequnce in entries
            operator_check: if enabled, will check if entries belong to same operator
        """
        super().__init__(**kwargs)

        if period is None:
            raise ValueError('period value can not be NULL in transient imei dimension. Check config...')

        if num_msisdns is None:
            raise ValueError('num_msisdns value can not be NULL in transient imei dimension. Check config...')

        try:
            self._period = int(period)
        except (TypeError, ValueError):
            raise ValueError("\'period\' parameter must be a int, got \'{0}\' instead in transient imei dimension..."
                             .format(period))

        try:
            self.num_msisdns = int(num_msisdns)
        except (TypeError, ValueError):
            raise ValueError("\'num_msisdn\' parameter must be a int, got \'{0}\' instead in "
                             'transient imei dimension...'.format(num_msisdns))

        if self._period is not None and self._period <= 0:
            raise ValueError("\'period\' in transient imei dimension requires positive value. Check config...")

        if self.num_msisdns is not None and self.num_msisdns <= 0:
            raise ValueError("\'num_msisdn\' parameter in transient imei dimension requires positive value. "
                             'Check config...')

    @property
    def algorithm_name(self):
        """Overrides Dimension.algorithm_name."""
        return 'Transient IMEI'

    def _calc_analysis_window(self, conn, curr_date=None):
        """
        Method used to calculate the analysis window (as a tuple) given a curr date.

        Arguments:
            conn: DIRBS Postgresql connection
            curr_date: current date of the analysis
        """
        analysis_end_date = compute_analysis_end_date(conn, curr_date)
        analysis_start_date = analysis_end_date - relativedelta.relativedelta(days=self._period)
        self._log_analysis_window(analysis_start_date, analysis_end_date)
        return analysis_start_date, analysis_end_date

    def _matching_imeis_sql(self, conn, app_config, virt_imei_range_start, virt_imei_range_end, curr_date=None):
        """
        Overrides Dimension._matching_imeis_sql.

        Arguments:
            conn: DIRBS PostgreSQL connection object
            app_config: DIRBS parsed configuration object
            virt_imei_range_start: IMEI shard start range to search
            virt_imei_range_end: IMEI shard end range to search
            curr_date: current date to use to analyze
        Returns:
            Dimension SQL Query
        """
        analysis_start_date, analysis_end_date = self._calc_analysis_window(conn, curr_date)

        query = sql.SQL("""SELECT imei_norm
                             FROM (SELECT imei_norm, SUM(bit) AS msisdn_count
                                     FROM (SELECT imei_norm, msisdn, operator_id,
                                                  get_bitmask_within_window(date_bitmask,
                                                                            first_seen,
                                                                            last_seen,
                                                                            {analysis_start_date},
                                                                            {analysis_start_dom},
                                                                            {analysis_end_date},
                                                                            {analysis_end_dom}) AS date_bitmask
                                             FROM monthly_network_triplets_per_mno
                                            WHERE imei_norm IS NOT NULL
                                              AND last_seen >= {analysis_start_date}
                                              AND first_seen < {analysis_end_date}
                                              AND virt_imei_shard >= {virt_imei_range_start}
                                              AND virt_imei_shard < {virt_imei_range_end}
                                              AND is_valid_msisdn(msisdn)) mn
                               CROSS JOIN generate_series(0, 30) AS i
                               CROSS JOIN LATERAL get_bit(mn.date_bitmask::bit(31), i) AS bit
                                 GROUP BY imei_norm) AS imeis_to_msisdns
                            WHERE msisdn_count/{period} >= {num_of_msisdns}
                              AND have_arithmetic_progression(imei_norm, {analysis_start_date}, {analysis_end_date})
                              AND have_same_operator_id(imei_norm, {analysis_start_date}, {analysis_end_date}) = 1
                             """).format(
            analysis_start_date=sql.Literal(analysis_start_date),
            analysis_start_dom=sql.Literal(analysis_start_date.day),
            analysis_end_date=sql.Literal(analysis_end_date),
            analysis_end_dom=sql.Literal(analysis_end_date.day),
            virt_imei_range_start=sql.Literal(virt_imei_range_start),
            virt_imei_range_end=sql.Literal(virt_imei_range_end),
            period=sql.Literal(self._period),
            num_of_msisdns=sql.Literal(self.num_msisdns)
        )

        return query.as_string(conn)


dimension = TransientIMEI
