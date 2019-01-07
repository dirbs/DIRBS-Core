"""
DIRBS REST-ful IMEI API module.

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
import re
import operator

from flask import jsonify, current_app, abort
from psycopg2 import sql

from dirbs.api.common.db import get_db_connection
from dirbs.api.common.imei import validate_imei, get_conditions, ever_observed_on_network, is_paired
from dirbs.api.v2.schemas.imei import IMEIInfo, IMEI, IMEISubscribers, IMEIPairings
from dirbs.api.common.pagination import Pagination


def registration_list_status(cursor, imei_norm):
    """Method to get RegistrationList status of an IMEI from Registration List."""
    cursor.execute(sql.SQL("""SELECT status,
                                          CASE
                                            WHEN status = 'whitelist' THEN FALSE
                                            WHEN status IS NULL THEN FALSE
                                            ELSE TRUE
                                          END
                                          AS provisional_only
                                FROM registration_list
                               WHERE imei_norm = %(imei_norm)s
                                 AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)"""),
                   {'imei_norm': imei_norm})
    result = cursor.fetchone()

    if result is not None:
        return dict({
            'status': result.status,
            'provisional_only': result.provisional_only
        })

    return dict({
        'status': None,
        'provisional_only': None
    })


def stolen_list_status(cursor, imei_norm):
    """Method to get StolenList status of an IMEI from Stolen List."""
    cursor.execute(sql.SQL("""SELECT status,
                                          CASE
                                            WHEN status = 'blacklist' THEN FALSE
                                            WHEN status IS NULL THEN FALSE
                                            ELSE TRUE
                                          END
                                          AS provisional_only
                                FROM stolen_list
                               WHERE imei_norm = %(imei_norm)s
                                 AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)"""),
                   {'imei_norm': imei_norm})
    result = cursor.fetchone()
    if result is not None:
        return dict({
            'status': result.status,
            'provisional_only': result.provisional_only
        })
    else:
        return dict({
            'status': None,
            'provisional_only': None
        })


def block_date(cursor, imei_norm):
    """Method to get block date of an IMEI from Notification and Blacklist."""
    # check if it is in blacklist
    cursor.execute("""SELECT block_date, delta_reason
                        FROM blacklist
                       WHERE imei_norm = %(imei_norm)s
                         AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)
                    ORDER BY start_run_id DESC""",
                   {'imei_norm': imei_norm})
    blacklist_block_date = cursor.fetchone()
    if blacklist_block_date is not None and blacklist_block_date.delta_reason != 'unblocked':
        return blacklist_block_date.block_date
    else:
        # check if it is in notification list
        cursor.execute("""SELECT block_date, delta_reason
                            FROM notifications_lists
                           WHERE imei_norm = %(imei_norm)s
                             AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)
                        ORDER BY start_run_id DESC""",
                       {'imei_norm': imei_norm})
        notification_block_date = cursor.fetchone()
        if notification_block_date is not None and notification_block_date.delta_reason != 'resolved':
            return notification_block_date.block_date
        return None


def is_exempted_device(cursor, imei_norm):
    """Method to check if an IMEI device has been exempted."""
    exempted_device_types = current_app.config['DIRBS_CONFIG'].region_config.exempted_device_types

    if len(exempted_device_types) > 0:
        cursor.execute("""SELECT device_type
                            FROM gsma_data
                           WHERE tac = '{tac}'""".format(tac=imei_norm[:8]))
        result = cursor.fetchone()
        if result is not None:
            return result.device_type in exempted_device_types
        return False
    return False


def imei_info(imei):
    """IMEI-Info API method handler."""
    imei_norm = validate_imei(imei)

    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        cursor.execute("""SELECT imei_norm, make, model, status, model_number, brand_name, device_type,
                                 radio_interface
                            FROM registration_list
                           WHERE imei_norm = %(imei_norm)s
                             AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)""",
                       {'imei_norm': imei_norm})
        rec = cursor.fetchone()
        if rec is not None:
            return jsonify(IMEIInfo().dump(rec._asdict()).data)
        return {}


def imei(imei):
    """IMEI API handler."""
    imei_norm = validate_imei(imei)

    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        condition_results = get_conditions(cursor, imei_norm)

        response = {
            'imei_norm': imei_norm,
            'block_date': block_date(cursor, imei_norm),
            'classification_state': {
                'blocking_conditions': [
                    dict({
                        'condition_name': key,
                        'condition_met': value['result']
                    }) for key, value in condition_results.items() if value['blocking']
                ],
                'informative_conditions': [
                    dict({
                        'condition_name': key,
                        'condition_met': value['result']
                    }) for key, value in condition_results.items() if not value['blocking']
                ]
            },
            'realtime_checks': {
                'ever_observed_on_network': ever_observed_on_network(cursor, imei_norm),
                'invalid_imei': False if re.match(r'^\d{14}$', imei_norm) else True,
                'is_paired': is_paired(cursor, imei_norm),
                'is_exempted_device': is_exempted_device(cursor, imei_norm)
            },
            'registration_status': registration_list_status(cursor, imei_norm),
            'stolen_status': stolen_list_status(cursor, imei_norm)
        }

        return jsonify(IMEI().dump(response).data)


def imei_subscribers(imei, **kwargs):
    """IMEI-Subscribers API handler."""
    imei_norm = validate_imei(imei)
    offset = kwargs.get('offset')
    limit = kwargs.get('limit')
    order = kwargs.get('order')

    if offset is None:
        offset = 1

    if limit is None:
        limit = 10

    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        cursor.execute("""SELECT DISTINCT imsi, msisdn, last_seen
                            FROM monthly_network_triplets_country_no_null_imeis
                           WHERE imei_norm = %(imei_norm)s
                             AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)""",
                       {'imei_norm': imei_norm})
        if cursor is not None:
            subscribers = [{'imsi': x.imsi, 'msisdn': x.msisdn, 'last_seen': x.last_seen} for x in cursor]
            paginated_data = Pagination.paginate(subscribers, offset, limit)

            if order == 'Ascending':
                paginated_data.get('data').sort(key=operator.itemgetter('last_seen'))
                return jsonify(IMEISubscribers().dump(dict(imei_norm=imei_norm,
                                                           subscribers=paginated_data.get('data'),
                                                           _keys=paginated_data.get('keys'))).data)
            elif order == 'Descending':
                paginated_data.get('data').sort(key=operator.itemgetter('last_seen'), reverse=True)
                return jsonify(IMEISubscribers().dump(dict(imei_norm=imei_norm,
                                                           subscribers=paginated_data.get('data'),
                                                           _keys=paginated_data.get('keys'))).data)
            return jsonify(IMEISubscribers().dump(dict(imei_norm=imei_norm,
                                                       subscribers=paginated_data.get('data'),
                                                       _keys=paginated_data.get('keys'))).data)

        keys = {'offset': offset, 'limit': limit, 'current_key': offset, 'next_key': '', 'result_size': 0}
        return jsonify(IMEISubscribers().dump(dict(imei_norm=imei_norm, subscribers=None, _keys=keys)))


def imei_pairings(imei, **kwargs):
    """IMEI-Pairings API handler."""
    imei_norm = validate_imei(imei)
    offset = kwargs.get('offset')
    limit = kwargs.get('limit')
    order = kwargs.get('order')

    if offset is None:
        offset = 1

    if limit is None:
        limit = 10

    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        cursor.execute("""SELECT pairing_list.imsi, network_triplets.last_seen
                            FROM pairing_list
                       LEFT JOIN monthly_network_triplets_country_no_null_imeis AS network_triplets
                                  ON network_triplets.imsi = pairing_list.imsi
                             AND network_triplets.imei_norm = pairing_list.imei_norm
                           WHERE pairing_list.imei_norm = '{imei_norm}'"""
                       .format(imei_norm=imei_norm))
        if cursor is not None:
            pairings = [{'imsi': x.imsi, 'last_seen': x.last_seen} for x in cursor]
            paginated_data = Pagination.paginate(pairings, offset, limit)

            if order == 'Ascending':
                paginated_data.get('data').sort(key=operator.itemgetter('last_seen'))
                return jsonify(IMEIPairings().dump(dict(imei_norm=imei_norm,
                                                        pairs=paginated_data.get('data'),
                                                        _keys=paginated_data.get('keys'))).data)
            elif order == 'Descending':
                paginated_data.get('data').sort(key=operator.itemgetter('last_seen'), reverse=True)
                return jsonify(IMEIPairings().dump(dict(imei_norm=imei_norm,
                                                        pairs=paginated_data.get('data'),
                                                        _keys=paginated_data.get('keys'))).data)

            return jsonify(IMEIPairings().dump(dict(imei_norm=imei_norm,
                                                    pairs=paginated_data.get('data'),
                                                    _keys=paginated_data.get('keys'))).data)

        keys = {'offset': offset, 'limit': limit, 'current_key': offset, 'next_key': '', 'result_size': 0}
        return jsonify(IMEIPairings().dump(dict(imei_norm=imei_norm, pairs=None, _keys=keys)))


def imei_batch(**kwargs):
    """IMEI API POST method handler for IMEI-Batch request."""
    if bool(kwargs):
        imeis = kwargs.get('imeis')
        data = []
        with get_db_connection() as db_conn, db_conn.cursor() as cursor:
            for imei in imeis:
                imei_norm = validate_imei(imei)
                condition_results = get_conditions(cursor, imei_norm)

                response = {
                    'imei_norm': imei_norm,
                    'block_date': block_date(cursor, imei_norm),
                    'classification_state': {
                        'blocking_conditions': [
                            dict({
                                'condition_name': key,
                                'condition_met': value['result']
                            }) for key, value in condition_results.items() if value['blocking']
                        ],
                        'informative_conditions': [
                            dict({
                                'condition_name': key,
                                'condition_met': value['result']
                            }) for key, value in condition_results.items() if not value['blocking']
                        ]
                    },
                    'realtime_checks': {
                        'ever_observed_on_network': ever_observed_on_network(cursor, imei_norm),
                        'invalid_imei': False if re.match(r'^\d{14}$', imei_norm) else True,
                        'is_paired': is_paired(cursor, imei_norm),
                        'is_exempted_device': is_exempted_device(cursor, imei_norm)
                    },
                    'registration_status': registration_list_status(cursor, imei_norm),
                    'stolen_status': stolen_list_status(cursor, imei_norm)
                }

                data.append(IMEI().dump(response).data)
            return jsonify({'results': data})
    abort(400, 'Bad Input format (args cannot be empty)')
