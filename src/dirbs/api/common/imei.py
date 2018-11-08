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
from enum import Enum

from flask import jsonify, current_app, abort
from marshmallow import Schema, fields, validate, post_dump
from psycopg2 import sql

from dirbs.api.common.db import get_db_connection
from dirbs.api.common.pagination import Pagination
from dirbs.utils import filter_imei_list_sql_by_device_type, registration_list_status_filter_sql


# noinspection PyProtectedMember
def api(imei, include_seen_with=False, include_paired_with=False):
    """IMEI API common functionality."""
    imei_norm = ImeiApi._validate_imei(imei)

    tac = imei_norm[:8]
    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        cursor.execute('SELECT NOT EXISTS (SELECT * FROM gsma_data WHERE tac = %s) AS not_in_gsma', [tac])
        rt_gsma_not_found = cursor.fetchone()[0]

        condition_results = ImeiApi._get_conditions(cursor, imei_norm)

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
        resp['realtime_checks']['in_registration_list'] = ImeiApi._is_in_registration_list(db_conn, cursor, imei_norm)

        # add a real-time check for if IMEI was ever observed on the network
        resp['realtime_checks']['ever_observed_on_network'] = ImeiApi._ever_observed_on_network(cursor, imei_norm)

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


class ImeiApi:
    """Defines methods for IMEI API (version 2.0)."""

    @staticmethod
    def _validate_imei(imei):
        """Method for validating imei format."""
        if len(imei) > 16:
            abort(400, 'Bad IMEI format (too long)')

        if re.match(r'^\d{14}', imei):
            imei_norm = imei[:14]
        else:
            imei_norm = imei.upper()

        return imei_norm

    @staticmethod
    def _get_conditions(cursor, imei_norm):
        """Method for reading conditions from config & DB."""
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
        return condition_results

    @staticmethod
    def _ever_observed_on_network(cursor, imei_norm):
        """Method to check if an IMEI is ever observed on the network."""
        cursor.execute(
            """SELECT EXISTS(SELECT 1
                               FROM network_imeis
                              WHERE imei_norm = %(imei_norm)s
                                AND virt_imei_shard =
                                        calc_virt_imei_shard(%(imei_norm)s)) AS ever_observed_on_network
            """,
            {'imei_norm': imei_norm}
        )
        return cursor.fetchone().ever_observed_on_network

    @staticmethod
    def _is_paired(cursor, imei_norm):
        """Method to check if an IMEI is paired."""
        cursor.execute("""SELECT EXISTS(SELECT 1
                                          FROM pairing_list
                                         WHERE imei_norm = %(imei_norm)s
                                           AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s))""",
                       {'imei_norm': imei_norm})
        return cursor.fetchone()[0]

    @staticmethod
    def _is_in_registration_list(db_conn, cursor, imei_norm):
        """Method to check if an IMEI exists in the Registration List."""
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
        return in_registration_list

    @staticmethod
    def _registration_list_status(cursor, imei_norm):
        """Method to get Registration List status of an IMEI from database."""
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

    @staticmethod
    def _stolen_list_status(cursor, imei_norm):
        """Method to check if an IMEI exists in the stolen list."""
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

    @staticmethod
    def _is_exempted_device(cursor, imei_norm):
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

    @staticmethod
    def _get_block_date(cursor, imei_norm):
        """Method to check and return block date of IMEI from lists."""
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

    @staticmethod
    def _get_subscribers(cursor, imei_norm):
        """Method to get IMSI-MSISDN pairs seen on the network with imei_norm."""
        cursor.execute("""SELECT DISTINCT imsi, msisdn, last_seen
                            FROM monthly_network_triplets_country_no_null_imeis
                           WHERE imei_norm = %(imei_norm)s
                             AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)""",
                       {'imei_norm': imei_norm})
        if cursor is not None:
            return [{'imsi': x.imsi, 'msisdn': x.msisdn, 'last_seen': x.last_seen} for x in cursor]
        return []

    def get_info(self, imei):
        """IMEI API imei/<imei>/info handler."""
        imei_norm = self._validate_imei(imei)

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

    def get(self, imei):
        """IMEI API handler for IMEI."""
        imei_norm = self._validate_imei(imei)

        with get_db_connection() as db_conn, db_conn.cursor() as cursor:
            condition_results = self._get_conditions(cursor, imei_norm)

            response = {
                'imei_norm': imei_norm,
                'block_date': self._get_block_date(cursor, imei_norm),
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
                    'ever_observed_on_network': self._ever_observed_on_network(cursor, imei_norm),
                    'invalid_imei': False if re.match(r'^\d{14}$', imei_norm) else True,
                    'is_paired': self._is_paired(cursor, imei_norm),
                    'is_exempted_device': self._is_exempted_device(cursor, imei_norm)
                },
                'registration_status': self._registration_list_status(cursor, imei_norm),
                'stolen_status': self._stolen_list_status(cursor, imei_norm)
            }

            return jsonify(IMEIV2().dump(response).data)

    def get_subscribers(self, imei, **kwargs):
        """Handler method for IMEI-Subscribers API (version 2.0)."""
        imei_norm = self._validate_imei(imei)
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

    def get_pairings(self, imei, **kwargs):
        """Handler method for IMEI-Pairings API (version 2.0)."""
        imei_norm = self._validate_imei(imei)
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

    def get_batch(self, **kwargs):
        """IMEI API POST method handler for IMEI-Batch request."""
        if bool(kwargs):
            imeis = kwargs.get('imeis')
            data = []
            with get_db_connection() as db_conn, db_conn.cursor() as cursor:
                for imei in imeis:
                    imei_norm = self._validate_imei(imei)
                    condition_results = self._get_conditions(cursor, imei_norm)

                    response = {
                        'imei_norm': imei_norm,
                        'block_date': self._get_block_date(cursor, imei_norm),
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
                            'ever_observed_on_network': self._ever_observed_on_network(cursor, imei_norm),
                            'invalid_imei': False if re.match(r'^\d{14}$', imei_norm) else True,
                            'is_paired': self._is_paired(cursor, imei_norm),
                            'is_exempted_device': self._is_exempted_device(cursor, imei_norm)
                        },
                        'registration_status': self._registration_list_status(cursor, imei_norm),
                        'stolen_status': self._stolen_list_status(cursor, imei_norm)
                    }

                    data.append(IMEIV2().dump(response).data)
                return jsonify({'results': data})
        abort(400, 'Bad Input format (args cannot be empty)')


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


class StolenStatus(Schema):
    """Defines schema for checking Stolen Status of the IMEI from LSMS for API (version 2.0)."""

    status = fields.String()
    provisional_only = fields.Boolean()


class RegistrationStatus(Schema):
    """Defines schema for checking Registration Status of the IMEI from DRS for API (version 2.0)."""

    status = fields.String()
    provisional_only = fields.Boolean()


class RealtimeChecksV2(Schema):
    """Defines schema for realtime checks associated with IMEI for API (version 2.0)."""

    ever_observed_on_network = fields.Boolean()
    invalid_imei = fields.Boolean()
    is_paired = fields.Boolean()
    is_exempted_device = fields.Boolean()


class ClassificationStateV2(Schema):
    """Define schema for status of configured conditions for API (version 2.0)."""

    blocking_conditions = fields.List(fields.Dict())
    informative_conditions = fields.List(fields.Dict())


class IMEIV2(Schema):
    """Define schema for IMEI API (version 2.0)."""

    SKIP_VALUES = set([None])

    imei_norm = fields.String(required=True)
    block_date = fields.Date(required=False)
    classification_state = fields.Nested(ClassificationStateV2, required=True)
    realtime_checks = fields.Nested(RealtimeChecksV2, required=True)
    registration_status = fields.Nested(RegistrationStatus, required=True)
    stolen_status = fields.Nested(StolenStatus, required=True)

    @post_dump
    def skip_missing_block_date(self, data):
        """Skip block date if it is missing."""
        if data.get('block_date') in self.SKIP_VALUES:
            del data['block_date']
        return data


class BatchIMEI(Schema):
    """Defines schema for Batch-IMEIs API (version 2.0)."""

    results = fields.List(fields.Nested(IMEIV2, required=True))


class Validators:
    """Defines custom validators for schema fields."""

    @staticmethod
    def validate_imei(val):
        """Validates IMEI format."""
        if len(val) > 16:
            abort(400, 'Bad IMEI format (too long).')

        if re.match(r'\s+', val):
            abort(400, 'Bad IMEI format (whitespces not allowed).')

        if re.match(r'\t+', val):
            abort(400, 'Bad IMEI format (tabs not allowed).')

        if len(val) == 0:
            abort(400, 'Bad IMEI format (empty imei).')

    @staticmethod
    def validate_imei_list(val):
        """Validates IMEI list."""
        if len(val) == 0:
            abort(400, 'Bad Input format (imei list cannot be empty).')

        if len(val) > 1000:
            abort(400, 'Bad Input format (max allowed imeis are 1000).')


class IMEIBatchArgs(Schema):
    """Input args for Batch-IMEI POST API (version 2)."""

    # noinspection PyProtectedMember
    imeis = fields.List(fields.String(required=True, validate=Validators.validate_imei),
                        validate=Validators.validate_imei_list)

    @property
    def fields_dict(self):
        """Convert declared fields to dictionary."""
        return self._declared_fields


class Subscribers(Schema):
    """Defines schema for Subscribers Info for IMEI API (version 2.0)."""

    imsi = fields.String()
    msisdn = fields.String()
    last_seen = fields.String()


class Pairings(Schema):
    """Defines schema for Pairings Info for IMEI API (version 2.0)."""

    imsi = fields.String()
    last_seen = fields.String()


class SortingOrders(Enum):
    """Enum for supported sorting orders."""

    ASC = 'Ascending'
    DESC = 'Descending'


class SubscriberArgs(Schema):
    """Defines schema for IMEI-Subscriber API arguments."""

    offset = fields.Integer(required=False, description='Offset the results on the current page '
                                                        'by the specified imsi-msisdn pair. It should be the value of '
                                                        'imsi-msisdn pair for the last result on the previous page')
    limit = fields.Integer(required=False, description='Number of results to return on the current page')
    order = fields.String(required=False, validate=validate.OneOf([f.value for f in SortingOrders]),
                          description='The sort order for the results using imsi-msisdn as the key')

    @property
    def fields_dict(self):
        """Convert declared fields to dictionary."""
        return self._declared_fields


class Keys(Schema):
    """Defines schema for keys of paginated result set."""

    previous_key = fields.String()
    next_key = fields.String()
    result_size = fields.Integer()


class IMEISubscribers(Schema):
    """Defines schema for IMEI-Subscribers API (version 2.0)."""

    _keys = fields.Nested(Keys, required=True)
    imei_norm = fields.String()
    subscribers = fields.List(fields.Nested(Subscribers, required=True))


class IMEIPairings(Schema):
    """Defines schema for IMEI-Pairings API (version 2.0)."""

    _keys = fields.Nested(Keys, required=True)
    imei_norm = fields.String()
    pairs = fields.List(fields.Nested(Pairings, required=True))


class IMEIInfo(Schema):
    """Response schema for IMEI-Info API."""

    imei_norm = fields.String(required=True)
    status = fields.String(required=False)
    make = fields.String(required=False)
    model = fields.String(required=False)
    model_number = fields.String(required=False)
    brand_name = fields.String(required=False)
    device_type = fields.String(required=False)
    radio_interface = fields.String(required=False)
