"""
DIRBS REST-ful job_metadata API module.

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
from enum import Enum

from flask import jsonify
from psycopg2 import sql
from marshmallow import Schema, fields, validate

from dirbs.api.common.db import get_db_connection


def api(command=None, subcommand=None, run_id=None, status=None, max_results=10, show_details=True):
    """Job metadata API endpoint."""
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


class JobMetadata(Schema):
    """Define schema for the metadata associated with a DIRBS job."""

    command = fields.String(required=True)
    subcommand = fields.String(required=True)
    command_line = fields.String(required=True)
    db_user = fields.String(required=True)
    start_time = fields.DateTime(required=True)
    end_time = fields.DateTime(required=True)
    exception_info = fields.String(required=True)
    run_id = fields.Integer(required=True)
    status = fields.String(required=True)
    extra_metadata = fields.Dict(required=False)


class JobStatus(Enum):
    """Enum for the supported data import file types."""

    RUNNING = 'running'
    SUCCESS = 'success'
    ERROR = 'error'


class JobCommandType(Enum):
    """Enum for the supported job command types."""

    CATALOG = 'dirbs-catalog'
    CLASSIFY = 'dirbs-classify'
    DB = 'dirbs-db'
    IMPORT = 'dirbs-import'
    LISTGEN = 'dirbs-listgen'
    PRUNE = 'dirbs-prune'
    REPORT = 'dirbs-report'


class JobMetadataArgs(Schema):
    """Input arguments for the job metadata API."""

    command = fields.List(fields.String(validate=validate.OneOf([f.value for f in JobCommandType])),
                          required=False, missing=[], description='Filter results to include only '
                                                                  'jobs belonging to specified command(s)')
    subcommand = fields.List(fields.String(), required=False, missing=[],
                             description='Filter results to include only jobs belonging to specified subcommand(s)')
    run_id = fields.List(fields.Integer(validate=validate.Range(min=1)), required=False, missing=[],
                         description='Filter results to only include job with the specified run_id(s)')
    max_results = fields.Integer(required=False, validate=validate.Range(min=1), missing=10,
                                 description='Number of jobs to return sorted by run_id in descending order')
    status = fields.List(fields.String(validate=validate.OneOf([f.value for f in JobStatus])),
                         required=False, missing=[], description='Filter results to only include jobs '
                                                                 'having the specified status')
    show_details = fields.Boolean(required=False, missing=True, description='Whether or not to include '
                                                                            '\'extra_metadata\' field in the results')

    @property
    def fields_dict(self):
        """Convert declared fields to dictionary."""
        return self._declared_fields
