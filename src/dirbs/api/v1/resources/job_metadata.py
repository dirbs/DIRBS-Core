"""
DIRBS REST-ful job_metadata API module.

SPDX-License-Identifier: BSD-4-Clause-Clear

Copyright (c) 2018-2019 Qualcomm Technologies, Inc.

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
limitations in the disclaimer below) provided that the following conditions are met:

    - Redistributions of source code must retain the above copyright notice, this list of conditions and the following
      disclaimer.
    - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
      following disclaimer in the documentation and/or other materials provided with the distribution.
    - All advertising materials mentioning features or use of this software, or any deployment of this software,
      or documentation accompanying any distribution of this software, must display the trademark/logo as per the
      details provided here: https://www.qualcomm.com/documents/dirbs-logo-and-brand-guidelines
    - Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or
      promote products derived from this software without specific prior written permission.



SPDX-License-Identifier: ZLIB-ACKNOWLEDGEMENT

Copyright (c) 2018-2019 Qualcomm Technologies, Inc.

This software is provided 'as-is', without any express or implied warranty. In no event will the authors be held liable
for any damages arising from the use of this software. Permission is granted to anyone to use this software for any
purpose, including commercial applications, and to alter it and redistribute it freely, subject to the following
restrictions:

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
from flask import jsonify
from psycopg2 import sql

from dirbs.api.common.db import get_db_connection
from dirbs.api.v1.schemas.job_metadata import JobMetadata


def job_metadata_api(command=None, subcommand=None, run_id=None, status=None, max_results=10, show_details=True):
    """
    Job metadata API endpoint.

    :param command: job command name (default none)
    :param subcommand: job sub-command name (default none)
    :param run_id: job run id (default none)
    :param status: job execution status (default none)
    :param max_results: max output results (default 10)
    :param show_details: bool (default True)
    :return: json response
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
