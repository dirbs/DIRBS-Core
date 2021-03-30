"""
DIRBS REST-ful IMEI API module.

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
import re

from flask import jsonify, current_app
from psycopg2 import sql

from dirbs.api.common.db import get_db_connection
from dirbs.api.common.imei import validate_imei, get_conditions, is_paired, is_in_registration_list
from dirbs.api.v2.schemas.imei import IMEIInfo, IMEI, IMEISubscribers, IMEIPairings


def registration_list_status(cursor, imei_norm: str) -> dict:
    """
    Method to get RegistrationList status of an IMEI from Registration List.

    Arguments:
        cursor: PostgreSQL database cursor
        imei_norm: Normalized IMEI
    Returns:
        Response dictionary
    """
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


def stolen_list_status(cursor, imei_norm: str) -> dict:
    """
    Method to get StolenList status of an IMEI from Stolen List.

    Arguments:
        cursor: PostgreSQL database cursor
        imei_norm: Normalized IMEI
    Returns:
        Response dictionary
    """
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


def block_date(cursor, imei_norm: str) -> str:
    """
    Method to get block date of an IMEI from Notification and Blacklist.

    Arguments:
        cursor: PostgreSQL database connection cursor object
        imei_norm: Normalized IMEI
    Returns:
        Block Date of the IMEI if exists otherwise None
    """
    # check if it is in blacklist
    cursor.execute("""SELECT MIN(block_date) AS block_date
                        FROM classification_state
                       WHERE imei_norm = %(imei_norm)s
                         AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)
                         AND end_date IS NULL""",
                   {'imei_norm': imei_norm})
    imei_block_date = cursor.fetchone()
    return imei_block_date.block_date


def first_seen(cursor, imei_norm: str) -> str:
    """
    Method to extract min first_seen of an IMEI.

    Arguments:
        cursor: PostgreSQL database connection cursor object
        imei_norm: Normalized IMEI
    Returns:
        first_seen date of IMEI if exists otherwise None
    """
    cursor.execute("""SELECT MIN(first_seen) AS first_seen
                        FROM network_imeis
                       WHERE imei_norm = %(imei_norm)s
                         AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)""",
                   {'imei_norm': imei_norm})
    return cursor.fetchone().first_seen


def is_exempted_device(cursor, imei_norm: str) -> bool:
    """
    Method to check if an IMEI device has been exempted.

    Arguments:
        cursor: PostgreSQL database cursor
        imei_norm: Normalized IMEI
    Returns:
        Boolean value (True/False)
    """
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


def imei_info_api(imei: str) -> jsonify:
    """
    IMEI-Info API method handler.

    Arguments:
        imei: IMEI value to extract information
    Returns:
        JSON response if exists otherwise
    """
    imei_norm = validate_imei(imei)

    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        cursor.execute("""SELECT imei_norm, make, model, status, model_number, brand_name, device_type,
                                 radio_interface
                            FROM registration_list
                           WHERE imei_norm = %(imei_norm)s
                             AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)""",
                       {'imei_norm': imei_norm})
        info_rec = cursor.fetchone()

        cursor.execute("""SELECT imei_norm
                            FROM registration_list
                           WHERE device_id = (SELECT device_id
                                                FROM registration_list
                                               WHERE imei_norm = %(imei_norm)s
                                                 AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s))
                             AND imei_norm NOT IN (%(imei_norm)s)""",
                       {'imei_norm': imei_norm})

        if info_rec is not None:
            response = info_rec._asdict()
            response['associated_imeis'] = [rec.imei_norm for rec in cursor] \
                if cursor is not None else []
            return jsonify(IMEIInfo().dump(response).data)
        return {}


def imei_api(imei: str, include_registration_status: bool = False, include_stolen_status: bool = False) -> jsonify:
    """
    IMEI API handler.

    Arguments:
        imei: value of the IMEI
        include_registration_status: boolean weather to include reg status or not (default False)
        include_stolen_status: boolean weather to include stolen status or not (default False)
    Returns:
        JSON response
    """
    imei_norm = validate_imei(imei)
    tac = imei_norm[:8]

    tac = imei_norm[:8]
    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        cursor.execute('SELECT NOT EXISTS (SELECT * FROM gsma_data WHERE tac = %s) AS not_in_gsma', [tac])
        rt_gsma_not_found = cursor.fetchone()[0]
        first_seen_date = first_seen(cursor, imei_norm)
        condition_results = get_conditions(cursor, imei_norm)
        response = {
            'imei_norm': imei_norm,
            'block_date': block_date(cursor, imei_norm),
            'first_seen': first_seen_date,
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
                'ever_observed_on_network': True if first_seen_date else False,
                'invalid_imei': False if re.match(r'^\d{14}$', imei_norm) else True,
                'is_paired': is_paired(cursor, imei_norm),
                'is_exempted_device': is_exempted_device(cursor, imei_norm),
                'in_registration_list': is_in_registration_list(db_conn, cursor, imei_norm),
                'gsma_not_found': rt_gsma_not_found
            }
        }

        if include_registration_status:
            response['registration_status'] = registration_list_status(cursor, imei_norm)
        if include_stolen_status:
            response['stolen_status'] = stolen_list_status(cursor, imei_norm)

        return jsonify(IMEI().dump(response).data)


def imei_subscribers_api(imei: str, **kwargs: dict) -> jsonify:
    """
    IMEI-Subscribers API handler.

    Arguments:
        imei: IMEI value
        kwargs: required arguments dictionary
    Returns:
        JSON response
    """
    imei_norm = validate_imei(imei)
    offset = kwargs.get('offset')
    limit = kwargs.get('limit')
    order = kwargs.get('order')

    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        query = """SELECT DISTINCT imsi, msisdn, last_seen, COUNT(*) OVER() AS total_count
                     FROM monthly_network_triplets_country_no_null_imeis
                    WHERE imei_norm = %(imei_norm)s
                      AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)
                 ORDER BY last_seen {order_type}
                 OFFSET {data_offset}
                 LIMIT {data_limit}""".format(order_type=order, data_offset=offset, data_limit=limit)  # noqa Q447

        cursor.execute(query, {'imei_norm': imei_norm})
        if cursor is not None:
            subscribers = [{'imsi': x.imsi,
                            'msisdn': x.msisdn,
                            'last_seen': x.last_seen,
                            'total_count': x.total_count} for x in cursor]
            keys = {'current_key': offset,
                    'next_key': offset + limit if subscribers else '',
                    'result_size': subscribers[0].get('total_count') if subscribers else 0}
            return jsonify(IMEISubscribers().dump(dict(imei_norm=imei_norm,
                                                       subscribers=subscribers,
                                                       _keys=keys)).data)

        keys = {'current_key': offset, 'next_key': '', 'result_size': 0}
        return jsonify(IMEISubscribers().dump(dict(imei_norm=imei_norm, subscribers=None, _keys=keys)))


def imei_pairings_api(imei: str, **kwargs: dict) -> jsonify:
    """
    IMEI-Pairings API handler.

    Arguments:
        imei: IMEI value
        kwargs: required arguments dictionary
    Returns:
        JSON response
    """
    imei_norm = validate_imei(imei)
    offset = kwargs.get('offset')
    limit = kwargs.get('limit')
    order = kwargs.get('order')

    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        query = """SELECT pairing_list.imsi, network_triplets.last_seen, COUNT(*) OVER() AS total_count
                     FROM pairing_list
                LEFT JOIN monthly_network_triplets_country_no_null_imeis AS network_triplets
                           ON network_triplets.imsi = pairing_list.imsi
                      AND network_triplets.imei_norm = pairing_list.imei_norm
                    WHERE pairing_list.imei_norm = %(imei_norm)s
                      AND pairing_list.virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)
                ORDER BY last_seen {order_type} 
                OFFSET {data_offset} 
                LIMIT {data_limit}""".format(order_type=order, data_offset=offset, data_limit=limit)  # noqa Q447

        cursor.execute(query, {'imei_norm': imei_norm})
        if cursor is not None:
            pairings = [{'imsi': x.imsi, 'last_seen': x.last_seen, 'total_count': x.total_count} for x in cursor]
            keys = {'current_key': offset,
                    'next_key': offset + limit if pairings else '',
                    'result_size': pairings[0].get('total_count') if pairings else 0}
            return jsonify(IMEIPairings().dump(dict(imei_norm=imei_norm,
                                                    pairs=pairings,
                                                    _keys=keys)).data)

        keys = {'current_key': offset, 'next_key': '', 'result_size': 0}
        return jsonify(IMEIPairings().dump(dict(imei_norm=imei_norm, pairs=None, _keys=keys)))


def imei_batch_api(**kwargs: dict) -> jsonify:
    """
    IMEI API POST method handler for IMEI-Batch request.

    Arguments:
        kwargs: required arguments (list of IMEIs)
    Returns:
        JSON response
    """
    imeis = kwargs.get('imeis')
    include_registration_status = kwargs.get('include_registration_status')
    include_stolen_status = kwargs.get('include_stolen_status')

    data = []
    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        for imei in imeis:
            imei_norm = validate_imei(imei)
            tac = imei_norm[:8]
            condition_results = get_conditions(cursor, imei_norm)
            first_seen_date = first_seen(cursor, imei_norm)
            cursor.execute('SELECT NOT EXISTS (SELECT * FROM gsma_data WHERE tac = %s) AS not_in_gsma', [tac])
            rt_gsma_not_found = cursor.fetchone()[0]

            response = {
                'imei_norm': imei_norm,
                'block_date': block_date(cursor, imei_norm),
                'first_seen': first_seen_date,
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
                    'ever_observed_on_network': True if first_seen_date else False,
                    'invalid_imei': False if re.match(r'^\d{14}$', imei_norm) else True,
                    'is_paired': is_paired(cursor, imei_norm),
                    'is_exempted_device': is_exempted_device(cursor, imei_norm),
                    'gsma_not_found': rt_gsma_not_found,
                    'in_registration_list': is_in_registration_list(db_conn, cursor, imei_norm)
                }
            }

            if include_registration_status:
                response['registration_status'] = registration_list_status(cursor, imei_norm)
            if include_stolen_status:
                response['stolen_status'] = stolen_list_status(cursor, imei_norm)

            data.append(IMEI().dump(response).data)
        return jsonify({'results': data})
