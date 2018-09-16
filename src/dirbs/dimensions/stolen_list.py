"""
DIRBS dimension function for a IMEIs on the local stolen list.

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

import dirbs.partition_utils as partition_utils
from .base import Dimension


class StolenList(Dimension):
    """Implementation of the StolenList classification dimension."""

    def _matching_imeis_sql(self, conn, app_config, virt_imei_range_start, virt_imei_range_end, curr_date=None):
        """Overrides Dimension._matching_imeis_sql."""
        stolen_list_shard = partition_utils.imei_shard_name(base_name='historic_stolen_list',
                                                            virt_imei_range_start=virt_imei_range_start,
                                                            virt_imei_range_end=virt_imei_range_end)

        return sql.SQL("""SELECT imei_norm
                            FROM {stolen_list_shard}
                           WHERE status IS NULL
                              OR status = 'blacklist'
                             AND end_date IS NULL
                       """).format(stolen_list_shard=sql.Identifier(stolen_list_shard)).as_string(conn)


dimension = StolenList
