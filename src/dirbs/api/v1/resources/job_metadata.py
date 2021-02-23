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
from dirbs.api.v1.schemas.job_metadata import JobMetadata


def job_metadata_api(command: List[str] = None, subcommand: List[str] = None, run_id: List[int] = None,
                     status: List[str] = None, max_results: int = 10, show_details: bool = True) -> jsonify:
    """
    Job metadata API endpoint.

    Arguments:
        command: list of commands in str format (default None)
        subcommand: list of sub-commands in str format (default None)
        run_id: list of run ids of the jobs (default None)
        status: list of job statuses (default None)
        max_results: show this many results (default 10)
        show_details: bool to show complete details of jobs (default True)
    Returns:
        JSON response
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

        base_sql = sql.SQL("""SELECT * FROM job_metadata""")

        final_sql = base_sql

        if len(filters_sql) > 0:
            final_sql = sql.SQL('{0} WHERE {1}').format(base_sql, sql.SQL(' AND ').join(filters_sql))

        final_sql = sql.SQL('{0} ORDER BY start_time DESC LIMIT %s').format(final_sql)

        cursor.execute(final_sql, [max_results])

        if not show_details:
            resp = [JobMetadata(exclude=('extra_metadata',)).dump(rec._asdict()).data for rec in cursor]
        else:
            resp = [JobMetadata().dump(rec._asdict()).data for rec in cursor]

        return jsonify(resp)
