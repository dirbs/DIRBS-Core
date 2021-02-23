"""
DIRBS dimension function for a IMEIs on the device association list.

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

from dirbs.utils import filter_imei_list_sql_by_device_type
import dirbs.partition_utils as partition_utils
from .base import Dimension


class NotOnAssociationList(Dimension):
    """Implementation of not_on_association_list classification dimension."""

    def _matching_imeis_sql(self, conn, app_config, virt_imei_range_start, virt_imei_range_end, curr_date=None):
        """
        Overrirdes Dimension._matching_imeis_sql.

        :param conn: database connection
        :param app_config: dirbs config obj
        :param virt_imei_range_start: start of imei shard range
        :param virt_imei_range_end: end of imei shard range
        :param curr_date: user defined current date
        :return: SQL
        """
        network_imeis_shard = partition_utils.imei_shard_name(base_name='network_imeis',
                                                              virt_imei_range_start=virt_imei_range_start,
                                                              virt_imei_range_end=virt_imei_range_end)
        association_list_shard = partition_utils.imei_shard_name(base_name='historic_device_association_list',
                                                                 virt_imei_range_start=virt_imei_range_start,
                                                                 virt_imei_range_end=virt_imei_range_end)

        classification_query = sql.SQL("""SELECT imei_norm
                                            FROM {network_imeis_shard}
                                           WHERE NOT EXISTS(SELECT imei_norm
                                                              FROM {association_shard}
                                                             WHERE imei_norm = {network_imeis_shard}.imei_norm
                                                               AND end_date IS NULL)""").format(  # noqa: Q449
            network_imeis_shard=sql.Identifier(network_imeis_shard),
            association_shard=sql.Identifier(association_list_shard))

        classification_query = classification_query.as_string(conn)

        if len(app_config.region_config.exempted_device_types) > 0:
            classification_query = filter_imei_list_sql_by_device_type(conn,
                                                                       app_config.region_config.exempted_device_types,
                                                                       classification_query)
        return classification_query


dimension = NotOnAssociationList
