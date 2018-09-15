"""
DIRBS REST-ful IMEI API module.

Copyright (c) 2018 Qualcomm Technologies, Inc.

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted (subject to the limitations in the disclaimer below) provided that the
following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this list of conditions
   and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions
   and the following disclaimer in the documentation and/or other materials provided with the distribution.
 * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse
   or promote products derived from this software without specific prior written permission.

NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED
BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
import re

from flask import jsonify, current_app, abort
from marshmallow import Schema, fields
from psycopg2 import sql

from dirbs.api.common.db import get_db_connection
from dirbs.utils import filter_imei_list_sql_by_device_type, registration_list_status_filter_sql


def api(imei, include_seen_with=False, include_paired_with=False):
    """IMEI API common functionality."""
    if len(imei) > 16:
        abort(400, 'Bad IMEI format (too long)')

    if re.match(r'^\d{14}', imei):
        imei_norm = imei[:14]
    else:
        imei_norm = imei.upper()

    tac = imei_norm[:8]
    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        cursor.execute('SELECT NOT EXISTS (SELECT * FROM gsma_data WHERE tac = %s) AS not_in_gsma', [tac])
        rt_gsma_not_found = cursor.fetchone()[0]

        conditions = current_app.config['DIRBS_CONFIG'].conditions
        condition_results = {c.label: {'blocking': c.blocking, 'result': False} for c in conditions}
        cursor.execute("""SELECT cond_name
                            FROM classification_state
                           WHERE imei_norm = %(imei_norm)s
                             AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)
                             AND end_date IS NULL""",
                       {'imei_norm': imei_norm})
        for res in cursor:
            # Handle conditions no longer in the config
            if res.cond_name in condition_results:
                condition_results[res.cond_name]['result'] = True

        resp = {
            'imei_norm': imei_norm,
            'classification_state': {
                'blocking_conditions': {k: v['result'] for k, v in condition_results.items() if v['blocking']},
                'informative_conditions': {k: v['result'] for k, v in condition_results.items() if not v['blocking']}
            },
            'realtime_checks': {
                'invalid_imei': False if re.match(r'^\d{14}$', imei_norm) else True,
                'gsma_not_found': rt_gsma_not_found
            }
        }

        # add a real-time check for the registration list
        cursor.execute(sql.SQL("""SELECT EXISTS(SELECT 1
                                                  FROM registration_list
                                                 WHERE imei_norm = %(imei_norm)s
                                                   AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)
                                                   AND {wl_status_filter}) AS in_registration_list""")
                       .format(wl_status_filter=registration_list_status_filter_sql()), {'imei_norm': imei_norm})
        in_registration_list = cursor.fetchone().in_registration_list
        exempted_device_types = current_app.config['DIRBS_CONFIG'].region_config.exempted_device_types
        if not in_registration_list and len(exempted_device_types) > 0:
            imei_sql = str(cursor.mogrify("""SELECT %s::TEXT AS imei_norm""", [imei_norm]), db_conn.encoding)

            sql_query = filter_imei_list_sql_by_device_type(db_conn,
                                                            exempted_device_types,
                                                            imei_sql)
            cursor.execute(sql_query)
            # The IMEI is returned if it does not belong to an exempted device type.
            # As the IMEI was not in registration list and is not exempted,
            # the in_registration_list value would be set to False.
            in_registration_list = cursor.fetchone() is None
        resp['realtime_checks']['in_registration_list'] = in_registration_list

        # add a real-time check for if IMEI was ever observed on the network
        cursor.execute(
            """SELECT EXISTS(SELECT 1
                               FROM network_imeis
                              WHERE imei_norm = %(imei_norm)s
                                AND virt_imei_shard =
                                        calc_virt_imei_shard(%(imei_norm)s)) AS ever_observed_on_network
            """,
            {'imei_norm': imei_norm}
        )
        ever_observed_on_network = cursor.fetchone().ever_observed_on_network
        resp['realtime_checks']['ever_observed_on_network'] = ever_observed_on_network

        if include_seen_with:
            cursor.execute("""SELECT DISTINCT imsi, msisdn
                                FROM monthly_network_triplets_country_no_null_imeis
                               WHERE imei_norm = %(imei_norm)s
                                 AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)""",
                           {'imei_norm': imei_norm})
            resp['seen_with'] = [{'imsi': x.imsi, 'msisdn': x.msisdn} for x in cursor]

        cursor.execute("""SELECT EXISTS(SELECT 1
                                          FROM pairing_list
                                         WHERE imei_norm = %(imei_norm)s
                                           AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s))""",
                       {'imei_norm': imei_norm})
        resp['is_paired'] = [x.exists for x in cursor][0]

        if include_paired_with:
            cursor.execute("""SELECT imsi
                                FROM pairing_list
                               WHERE imei_norm = %(imei_norm)s
                                 AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)""",
                           {'imei_norm': imei_norm})
            resp['paired_with'] = [x.imsi for x in cursor]

        return jsonify(IMEI().dump(resp).data)


class ClassificationState(Schema):
    """Define schema for status of configured conditions."""

    blocking_conditions = fields.Dict()
    informative_conditions = fields.Dict()


class RealtimeChecks(Schema):
    """Define schema for realtime checks associated with the IMEI."""

    invalid_imei = fields.Boolean()
    gsma_not_found = fields.Boolean()
    in_registration_list = fields.Boolean()
    ever_observed_on_network = fields.Boolean()


class SeenWith(Schema):
    """Define schema for list of IMSI-MSISDN pairs seen with the IMEI."""

    imsi = fields.String()
    msisdn = fields.String()


class IMEI(Schema):
    """Define schema for IMEI API."""

    imei_norm = fields.String(required=True)
    seen_with = fields.List(fields.Nested(SeenWith), required=False)
    classification_state = fields.Nested(ClassificationState, required=True)
    realtime_checks = fields.Nested(RealtimeChecks)
    is_paired = fields.Boolean(required=True)
    paired_with = fields.List(fields.String(), required=False)


class IMEIArgs(Schema):
    """Input arguments for the IMEI API."""

    include_seen_with = fields.Boolean(required=False, missing=False,
                                       description='Whether or not to include \'seen_with\' field in the response')
    include_paired_with = fields.Boolean(required=False, missing=False,
                                         description='Whether or not to include \'paired_with\' field in the response')

    @property
    def fields_dict(self):
        """Convert declared fields to dictionary."""
        return self._declared_fields
