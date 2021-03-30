"""
DIRBS base dimension class containing common code used by all duplicate algorithms.

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

from dirbs.utils import compute_analysis_end_date
from .base import Dimension


class DuplicateAbstractBase(Dimension):
    """Abstract base class that all duplicate dimensions should inherit from."""

    def __init__(self, *, period_days, period_months, use_msisdn, **kwargs):
        """
        Constructor.

        :param period_days: period in terms of days
        :param period_months: period in terms of months
        :param use_msisdn: flag to use MSISDN for analysis rather then IMSI
        :param kwargs: kwargs
        """
        super().__init__(**kwargs)

        if period_days is not None and period_months is not None:
            raise ValueError('Both period_days and period_months in duplicate dimension are non-NULL. Check config...')

        if period_days is None and period_months is None:
            raise ValueError('Both period_days and period_months in duplicate dimension are NULL. Check config...')

        if not isinstance(use_msisdn, bool):
            raise ValueError('use_msisdn should be a boolean value (True/False). Check config...')

        self._period_days = int(period_days) if period_days is not None else None
        self._period_months = int(period_months) if period_months is not None else None
        self._use_msisdn = True if use_msisdn else False

        if self._period_months is not None and self._period_months < 0:
            raise ValueError('Negative value for period_months passed to duplicate dimension. Check config...')

        if self._period_days is not None and self._period_days < 0:
            raise ValueError('Negative value for period_days passed to duplicate dimension. Check config...')

    def _calc_analysis_window(self, conn, curr_date=None):
        """
        Method used to calculate the analysis window (as a tuple) given a curr date.

        :param conn: database connection
        :param curr_date: user defined current date (default None)
        :return: dates range for analysis
        """
        analysis_end_date = compute_analysis_end_date(conn, curr_date)
        if self._period_months is not None:
            analysis_start_date = analysis_end_date - relativedelta.relativedelta(months=self._period_months)
        else:
            analysis_start_date = analysis_end_date - relativedelta.relativedelta(days=self._period_days)

        self._log_analysis_window(analysis_start_date, analysis_end_date)
        return analysis_start_date, analysis_end_date
