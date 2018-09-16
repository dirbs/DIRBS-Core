"""
DIRBS dimension function for duplicate threshold within a time period.

Copyright (c) 2018 Qualcomm Technologies, Inc.

 All rights reserved.



 Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
 limitations in the disclaimer below) provided that the following conditions are met:


 * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 disclaimer.

 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 disclaimer in the documentation and/or other materials provided with the distribution.

 * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
 products derived from this software without specific prior written permission.

 NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
 THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 POSSIBILITY OF SUCH DAMAGE.

"""

from psycopg2 import sql

from .duplicate_abstract_base import DuplicateAbstractBase


class DuplicateThreshold(DuplicateAbstractBase):
    """Implementation of the DuplicateThreshold classification dimension."""

    def __init__(self, *, threshold, period_days=None, period_months=None, **kwargs):
        """Constructor."""
        super().__init__(period_days=period_days, period_months=period_months, **kwargs)
        try:
            self._threshold = int(threshold)
        except (TypeError, ValueError):
            raise ValueError('\'threshold\' parameter must be an integer, got \'{0}\' instead...'.format(threshold))

    @property
    def algorithm_name(self):
        """Overrides Dimension.algorithm_name."""
        return 'Duplicate threshold'

    def _matching_imeis_sql(self, conn, app_config, virt_imei_range_start, virt_imei_range_end, curr_date=None):
        """Overrides Dimension._matching_imeis_sql."""
        analysis_start_date, analysis_end_date = self._calc_analysis_window(conn, curr_date)
        return sql.SQL(
            """SELECT imei_norm
                 FROM (SELECT DISTINCT imei_norm, imsi
                         FROM monthly_network_triplets_country
                        WHERE imei_norm IS NOT NULL
                          AND last_seen >= {analysis_start_date}
                          AND first_seen < {analysis_end_date}
                          AND virt_imei_shard >= {virt_imei_range_start}
                          AND virt_imei_shard < {virt_imei_range_end}
                          AND is_valid_imsi(imsi)) all_seen_imei_imsis
             GROUP BY imei_norm HAVING COUNT(*) >= {threshold}
            """).format(analysis_start_date=sql.Literal(analysis_start_date),
                        analysis_end_date=sql.Literal(analysis_end_date),
                        virt_imei_range_start=sql.Literal(virt_imei_range_start),
                        virt_imei_range_end=sql.Literal(virt_imei_range_end),
                        threshold=sql.Literal(self._threshold)).as_string(conn)


dimension = DuplicateThreshold
