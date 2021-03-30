"""
DIRBS dimension function for average duplicate threshold within a time period.

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

from .duplicate_abstract_base import DuplicateAbstractBase


class DuplicateAverageThreshold(DuplicateAbstractBase):
    """Implementation of the DuplicateAverageThreshold classification dimension."""

    def __init__(self, *, threshold, period_days=None, period_months=None, min_seen_days, use_msisdn=False, **kwargs):
        """
        Constructor.

        :param threshold: threshold for duplicate entries
        :param period_days: analysis period in days (default None)
        :param period_months: analysis period in months (default None)
        :param min_seen_days: minimum days a triplets has been seen
        :param use_msisdn: flag to use MSISDN for analysis instead of IMSI (default false)
        :param kwargs:
        """
        super().__init__(period_days=period_days, period_months=period_months, use_msisdn=use_msisdn, **kwargs)

        try:
            self._threshold = float(threshold)
        except (TypeError, ValueError):
            raise ValueError("\'threshold\' parameter must be a float, got \'{0}\' instead...".format(threshold))

        try:
            self._min_seen_days = int(min_seen_days)
        except (TypeError, ValueError):
            raise ValueError("\'min_seen_days\' parameter must be an integer, "
                             "got \'{0}\' instead...".format(min_seen_days))

        if self._period_days is not None and self._min_seen_days > self._period_days:
            # TODO: Handle this for period_months as well -- difficult to do this without knowing curr_date
            raise ValueError('min_seen_days is greater than period_days in duplicate_daily_avg_config')

    @property
    def algorithm_name(self):
        """Overrides DuplicateAbstractBase.algorithm_name."""
        return 'Duplicate daily average'

    def _matching_imeis_sql(self, conn, app_config, virt_imei_range_start, virt_imei_range_end, curr_date=None):
        """
        Overrides Dimension._matching_imeis_sql.

        :param conn: database connection
        :param app_config: dirbs config obj
        :param virt_imei_range_start: imei shard range start
        :param virt_imei_range_end: imei shard range end
        :param curr_date: user defined current date for analysis
        :return: SQL
        """
        analysis_start_date, analysis_end_date = self._calc_analysis_window(conn, curr_date)

        """Calculate the average number of IMSIs an IMEI is seen with on a daily basis.
        If an IMEI-IMSI pair is seen across multiple operators on a day, pair is only counted once for that day.
        The averaging is done over the number of days that the IMEI was observed on within the lookback window.
        To not penalize IMEIs that have limited data, the min_seen_days parameter value is used to skip IMEIs that have
        number of days they were observed on less than the specified value.
        """
        if self._use_msisdn:
            # if use_msisdn is True than use MSISDN instead IMSI for analysis
            return sql.SQL(
                """SELECT imei_norm
                     FROM (SELECT imei_norm,
                                  bitcount(bit_or(combined_date_bitmask)) AS days_seen,
                                  SUM(bitcount(combined_date_bitmask)) AS msisdn_per_imei
                             FROM (SELECT imei_norm,
                                          msisdn,
                                          triplet_year,
                                          triplet_month,
                                          bit_or(get_bitmask_within_window(date_bitmask,
                                                                           first_seen,
                                                                           last_seen,
                                                                           {analysis_start_date},
                                                                           {analysis_start_dom},
                                                                           {analysis_end_date},
                                                                           {analysis_end_dom})
                                                ) AS combined_date_bitmask
                                     FROM monthly_network_triplets_country
                                    WHERE imei_norm IS NOT NULL
                                      AND last_seen >= {analysis_start_date}
                                      AND first_seen < {analysis_end_date}
                                      AND virt_imei_shard >= {virt_imei_range_start}
                                      AND virt_imei_shard < {virt_imei_range_end}
                                      AND is_valid_msisdn(msisdn)
                                 GROUP BY imei_norm, msisdn, triplet_year,
                                          triplet_month) all_seen_triplets
                         GROUP BY imei_norm, triplet_month, triplet_year) triplet_monthly_days
                 GROUP BY imei_norm
                          HAVING SUM(days_seen) >= {min_seen_days_threshold}
                                 AND (SUM(msisdn_per_imei)/SUM(days_seen)) >= {threshold}
                """).format(analysis_start_date=sql.Literal(analysis_start_date),  # noqa: Q447
                            analysis_start_dom=sql.Literal(analysis_start_date.day),
                            analysis_end_date=sql.Literal(analysis_end_date),
                            analysis_end_dom=sql.Literal(analysis_end_date.day),
                            virt_imei_range_start=sql.Literal(virt_imei_range_start),
                            virt_imei_range_end=sql.Literal(virt_imei_range_end),
                            min_seen_days_threshold=sql.Literal(self._min_seen_days),
                            threshold=sql.Literal(self._threshold)).as_string(conn)
        return sql.SQL(
            """SELECT imei_norm
                 FROM (SELECT imei_norm,
                              bitcount(bit_or(combined_date_bitmask)) AS days_seen,
                              SUM(bitcount(combined_date_bitmask)) AS imsis_per_imei
                         FROM (SELECT imei_norm,
                                      imsi,
                                      triplet_year,
                                      triplet_month,
                                      bit_or(get_bitmask_within_window(date_bitmask,
                                                                       first_seen,
                                                                       last_seen,
                                                                       {analysis_start_date},
                                                                       {analysis_start_dom},
                                                                       {analysis_end_date},
                                                                       {analysis_end_dom})
                                            ) AS combined_date_bitmask
                                 FROM monthly_network_triplets_country
                                WHERE imei_norm IS NOT NULL
                                  AND last_seen >= {analysis_start_date}
                                  AND first_seen < {analysis_end_date}
                                  AND virt_imei_shard >= {virt_imei_range_start}
                                  AND virt_imei_shard < {virt_imei_range_end}
                                  AND is_valid_imsi(imsi)
                             GROUP BY imei_norm, imsi, triplet_year,
                                      triplet_month) all_seen_triplets
                     GROUP BY imei_norm, triplet_month, triplet_year) triplet_monthly_days
             GROUP BY imei_norm
                      HAVING SUM(days_seen) >= {min_seen_days_threshold}
                             AND (SUM(imsis_per_imei)/SUM(days_seen)) >= {threshold}
            """).format(analysis_start_date=sql.Literal(analysis_start_date),  # noqa: Q447
                        analysis_start_dom=sql.Literal(analysis_start_date.day),
                        analysis_end_date=sql.Literal(analysis_end_date),
                        analysis_end_dom=sql.Literal(analysis_end_date.day),
                        virt_imei_range_start=sql.Literal(virt_imei_range_start),
                        virt_imei_range_end=sql.Literal(virt_imei_range_end),
                        min_seen_days_threshold=sql.Literal(self._min_seen_days),
                        threshold=sql.Literal(self._threshold)).as_string(conn)


dimension = DuplicateAverageThreshold
