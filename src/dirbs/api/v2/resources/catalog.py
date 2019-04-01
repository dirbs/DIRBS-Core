"""
DIRBS REST-ful data_catalog API module.

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
import operator

from psycopg2 import sql
from flask import jsonify

from dirbs.api.common.db import get_db_connection
from dirbs.api.common.pagination import Pagination
from dirbs.api.common.catalog import _build_sql_query_filters
from dirbs.api.v2.schemas.catalog import CatalogFile, Keys


def catalog_api(**kwargs):
    """
    Defines handler for Catalog API (version 2.0) GET method.
    :param kwargs: input args
    :return: json
    """
    sorting_order = kwargs.get('order')
    offset_key = kwargs.get('offset')
    per_page_limit = kwargs.get('limit')

    # Build filters to be applied to the SQL query
    filters, filter_params = _build_sql_query_filters(**kwargs)

    query = sql.SQL("""SELECT array_agg(status ORDER BY run_id DESC)::TEXT[] AS status_list, dc.*
                                     FROM (SELECT file_id,
                                                  filename,
                                                  file_type,
                                                  compressed_size_bytes,
                                                  modified_time,
                                                  is_valid_zip,
                                                  is_valid_format,
                                                  md5,
                                                  extra_attributes,
                                                  first_seen,
                                                  last_seen,
                                                  uncompressed_size_bytes,
                                                  num_records
                                             FROM data_catalog
                                                  {filters}
                                         ORDER BY last_seen DESC, file_id DESC
                                            LIMIT ALL) dc
                                LEFT JOIN (SELECT run_id, status, extra_metadata
                                             FROM job_metadata
                                            WHERE command = 'dirbs-import') jm
                                           ON md5 = (extra_metadata->>'input_file_md5')::uuid
                                 GROUP BY file_id,
                                          filename,
                                          file_type,
                                          compressed_size_bytes,
                                          modified_time,
                                          is_valid_zip,
                                          is_valid_format,
                                          md5,
                                          extra_attributes,
                                          first_seen,
                                          last_seen,
                                          uncompressed_size_bytes,
                                          num_records
                                 ORDER BY last_seen DESC, file_id DESC""")  # noqa Q444

    where_clause = sql.SQL('')
    if len(filters) > 0:
        where_clause = sql.SQL('WHERE {0}').format(sql.SQL(' AND ').join(filters))

    with get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute(cursor.mogrify(query.format(filters=where_clause), filter_params))
        resp = [CatalogFile().dump(rec._asdict()).data for rec in cursor]

        if sorting_order is not None or (offset_key is not None and per_page_limit is not None):
            paginated_data = Pagination.paginate(resp, offset_key, per_page_limit)

            if sorting_order == 'Ascending':
                paginated_data.get('data').sort(key=operator.itemgetter('file_id'))
                response = {
                    '_keys': Keys().dump(dict(paginated_data.get('keys'))).data,
                    'files': [file_data for file_data in paginated_data.get('data')]
                }

                return jsonify(response)

            elif sorting_order == 'Descending':
                paginated_data.get('data').sort(key=operator.itemgetter('file_id'), reverse=True)
                response = {
                    '_keys': Keys().dump(dict(paginated_data.get('keys'))).data,
                    'files': [file_data for file_data in paginated_data.get('data')]
                }

                return jsonify(response)

            response = {
                '_keys': Keys().dump(dict(paginated_data.get('keys'))).data,
                'files': [file_data for file_data in paginated_data.get('data')]
            }
            return jsonify(response)

        keys = {'offset': '', 'limit': '', 'previous_key': '', 'next_key': '', 'result_size': len(resp)}
        response = {
            '_keys': Keys().dump(dict(keys)).data,
            'files': resp
        }
        return jsonify(response)
