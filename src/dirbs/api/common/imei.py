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

from flask import current_app, abort
from psycopg2 import sql

from dirbs.utils import filter_imei_list_sql_by_device_type, registration_list_status_filter_sql


def validate_imei(imei: str) -> str:
    """
    Method for validating imei format.

    Arguments:
        imei: IMEI value to evaluate and normalize
    Returns:
        imei_norm: 14 digits normalized IMEI
    """
    if len(imei) > 16:
        abort(400, 'Bad IMEI format (too long)')

    if re.match(r'^\d{14}', imei):
        imei_norm = imei[:14]
    else:
        imei_norm = imei.upper()

    return imei_norm


def get_conditions(cursor, imei_norm: str) -> dict:
    """
    Method for reading conditions from config & DB.

    Arguments:
        cursor: PostgreSQL connection cursor
        imei_norm: normalized IMEI
    Returns:
        condition_results: dict of matched condition results
    """
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


def ever_observed_on_network(cursor, imei_norm: str) -> bool:
    """
    Method to check if an IMEI is ever observed on the network.

    Arguments:
        cursor: PostgreSQL connection cursor
        imei_norm: normalized IMEI
    Returns:
        Boolean: True/False
    """
    cursor.execute(
        """SELECT EXISTS(SELECT 1
                           FROM network_imeis
                          WHERE imei_norm = %(imei_norm)s
                            AND virt_imei_shard =
                                    calc_virt_imei_shard(%(imei_norm)s)) AS ever_observed_on_network""",  # noqa: Q449
        {'imei_norm': imei_norm})
    return cursor.fetchone().ever_observed_on_network


def is_in_registration_list(db_conn, cursor, imei_norm: str) -> bool:
    """
    Method to check if an IMEI exists in the Registration List.

    TODO: use either of "db_conn, cursor" in the function.

    Arguments:
        db_conn: PostgreSQL database connection object
        cursor: PostgreSQL connection cursor
        imei_norm: normalized IMEI
    Returns:
        is_in_registration_list: Boolean
    """
    cursor.execute(sql.SQL("""SELECT EXISTS(SELECT 1
                                              FROM registration_list
                                             WHERE imei_norm = %(imei_norm)s
                                               AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)
                                               AND {wl_status_filter}) AS in_registration_list""")  # noqa: Q449
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


def get_subscribers(cursor, imei_norm: str) -> list:
    """
    Method to get IMSI-MSISDN pairs seen on the network with imei_norm.

    Arguments:
        cursor: PostgreSQL connection cursor object
        imei_norm: Normalized IMEI
    Returns:
        []: list of expected results
    """
    cursor.execute("""SELECT DISTINCT imsi, msisdn, last_seen
                        FROM monthly_network_triplets_country_no_null_imeis
                       WHERE imei_norm = %(imei_norm)s
                         AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)""",
                   {'imei_norm': imei_norm})
    if cursor is not None:
        return [{'imsi': x.imsi, 'msisdn': x.msisdn, 'last_seen': x.last_seen} for x in cursor]
    return []


def is_paired(cursor, imei_norm: str) -> bool:
    """
    Method to check if an IMEI is paired.

    Arguments:
        cursor: PostgreSQL connection cursor object
        imei_norm: Normalized IMEI
    Returns:
        Bool
    """
    cursor.execute("""SELECT EXISTS(SELECT 1
                                      FROM pairing_list
                                     WHERE imei_norm = %(imei_norm)s
                                       AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s))""",  # noqa: Q449
                   {'imei_norm': imei_norm})
    return cursor.fetchone()[0]
