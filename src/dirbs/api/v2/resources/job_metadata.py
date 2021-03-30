"""
DIRBS REST-ful job_metadata API module.

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

from typing import List

from flask import jsonify
from psycopg2 import sql

from dirbs.api.common.db import get_db_connection
from dirbs.api.v2.schemas.job_metadata import JobKeys, JobMetadata


def get_metadata(command: List[str] = None, subcommand: List[str] = None,
                 run_id: List[int] = None, status: List[str] = None,
                 order: str = 'ASC', offset: int = 0, limit: int = 10):
    """Job Metadata API method handler.

    Arguments:
        command: List of specific command names
        subcommand: List of specific sub-command names
        run_id: List of run ids of the jobs
        status: List of acceptable status of jobs
        order: Ascending or Descending order by start_time of the job (default ASC)
        offset: Offset of the results to fetch from (default from start)
        limit: number of results per page (default 10)
    Returns:
        PostgreSQL records callables
    """
    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        # Build the query with params retrieved from request
        filters_sql = []

        for field, label in [(status, 'status'), (command, 'command'), (subcommand, 'subcommand')]:
            if len(field) > 0:
                mogrified_sql = cursor.mogrify(sql.SQL("""{0}::TEXT IN %s""").
                                               format(sql.Identifier(label)), [tuple(field)])
                filters_sql.append(sql.SQL(str(mogrified_sql, db_conn.encoding)))

        if len(run_id) > 0:
            mogrified_sql = cursor.mogrify(sql.SQL("""{0} IN (SELECT UNNEST(%s::BIGINT[]))""")
                                           .format(sql.Identifier('run_id')), [(run_id)])
            filters_sql.append(sql.SQL(str(mogrified_sql, db_conn.encoding)))

        base_sql = sql.SQL("""SELECT *, COUNT(*) OVER() AS total_count FROM job_metadata""")

        final_sql = base_sql

        if len(filters_sql) > 0:
            final_sql = sql.SQL('{0} WHERE {1}').format(base_sql, sql.SQL(' AND ').join(filters_sql))

        final_sql = sql.SQL('{final_sql} ORDER BY start_time {order_type} OFFSET {offset} LIMIT {limit}').format(
            final_sql=final_sql,
            order_type=sql.SQL(order),
            offset=sql.SQL(str(offset)),
            limit=sql.SQL(str(limit))
        )

        cursor.execute(final_sql)
        return cursor.fetchall()


def job_metadata_api(order, offset, limit, command=None, subcommand=None, run_id=None, status=None, show_details=True):
    """
    Defines handler method for job-metadata GET API (version 2.0).

    :param command: command name (default None)
    :param subcommand: sub-command name (default None)
    :param run_id: job run id (default None)
    :param status: job execution status (default None)
    :param show_details: show full job details (default True)
    :param order: sorting order (Ascending/Descending, default None)
    :param offset: offset of data (default None)
    :param limit: limit of the data (default None)
    :return: json
    """
    result = get_metadata(command, subcommand, run_id, status, order, offset, limit)
    result_size = result[0][-1] if result else 0
    keys = {'current_key': offset, 'next_key': offset + limit if result else 0, 'result_size': result_size}
    if not show_details:
        response = {
            '_keys': JobKeys().dump(dict(keys)).data,
            'jobs': [JobMetadata(exclude=('extra_metadata',)).dump(rec._asdict()).data for rec in result]
        }
    else:
        response = {
            '_keys': JobKeys().dump(dict(keys)).data,
            'jobs': [JobMetadata().dump(rec._asdict()).data for rec in result]
        }
    return jsonify(response)
