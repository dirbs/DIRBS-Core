"""
DIRBS base dimension functionality shared by all dimensions.

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

import abc
import logging

from psycopg2 import sql

import dirbs.partition_utils as partition_utils
from dirbs.utils import log_analysis_window


class Dimension(object):
    """Abstract base class representing the interface for an individual classification dimension."""

    def __init__(self, invert=False, condition_label=None):
        """
        Constructor.

        :param invert: to invert the condition effect
        :param condition_label: label of the condition (default None)
        """
        self.invert = invert
        self.condition_label = condition_label

    __metaclass__ = abc.ABCMeta

    @property
    def algorithm_name(self):
        """Dimension algorithm name."""
        raise NotImplementedError('Should be implemented')

    def sql(self, conn, app_config, virt_imei_range_start, virt_imei_range_end, curr_date=None):
        """
        Interface for a dimension to return the SQL fragment associated with it.

        :param conn: database connection
        :param app_config: dirbs config obj
        :param virt_imei_range_start: virtual imei shard range start
        :param virt_imei_range_end: virtual imei shard range end
        :param curr_date: current date by user (default None)
        :return: SQL
        """
        base_sql = self._matching_imeis_sql(conn, app_config, virt_imei_range_start, virt_imei_range_end,
                                            curr_date)
        if type(base_sql) == bytes:
            base_sql = str(base_sql, conn.encoding)

        # Dimensions should convert their query fragments to strings before returning
        assert type(base_sql) == str

        if self.invert:
            network_imeis_shard = partition_utils.imei_shard_name(base_name='network_imeis',
                                                                  virt_imei_range_start=virt_imei_range_start,
                                                                  virt_imei_range_end=virt_imei_range_end)

            dim_sql = sql.SQL("""SELECT imei_norm
                                   FROM {network_imeis_shard}
                                  WHERE NOT EXISTS(SELECT imei_norm
                                                     FROM ({base_dim_sql}) base
                                                    WHERE imei_norm = {network_imeis_shard}.imei_norm)
                              """).format(network_imeis_shard=sql.Identifier(network_imeis_shard),  # noqa: Q449
                                          base_dim_sql=sql.SQL(base_sql))
        else:
            dim_sql = sql.SQL(base_sql)
        return dim_sql

    @abc.abstractmethod
    def _matching_imeis_sql(self, conn, app_config, virt_imei_range_start, virt_imei_range_end, curr_date=None):
        """
        Interface for classifying IMEIs based on a dimension.

        Returns a string version of the SQL, with no unbound parameters

        :param conn: database connection
        :param app_config: dirbs config obj
        :param virt_imei_range_start: virtual imei shard range start
        :param virt_imei_range_end: virtual imei shard range end
        :param curr_date: current date by user
        """
        pass

    def _log_analysis_window(self, analysis_start_date, analysis_end_date, start_message=None):
        """
        Helper function to print out window on used for analysis using interval notation.

        :param analysis_start_date: start date for analysis
        :param analysis_end_date: end date for analysis
        :param start_message: start analysis message
        """
        logger = logging.getLogger('dirbs.classify')
        cond_label_info = 'in condition "{0}" '.format(self.condition_label) if self.condition_label else ''
        if not start_message:
            start_message = '{0} dimension {1}using analysis window'.format(self.algorithm_name, cond_label_info)
        log_analysis_window(logger, analysis_start_date, analysis_end_date, start_message=start_message,
                            start_date_inclusive=True, end_date_inclusive=False)
