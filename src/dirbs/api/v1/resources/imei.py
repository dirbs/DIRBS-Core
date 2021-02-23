"""
DIRBS REST-ful API-V1 imei module.

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

from flask import jsonify

from dirbs.api.v1.schemas.imei import IMEI
from dirbs.api.common.db import get_db_connection
from dirbs.api.common.imei import validate_imei, get_conditions, ever_observed_on_network, is_in_registration_list, \
    get_subscribers, is_paired


def imei_api(imei: str, include_seen_with: bool = False, include_paired_with: bool = False) -> jsonify:
    """
    IMEI API handler.

    Arguments:
        imei: IMEI number in format [15, 16] digits
        include_seen_with: bool to include seen with information in response (default False)
        include_paired_with: bool to include paired with information in response (default False)
    Returns:
        JSON response
    """
    imei_norm = validate_imei(imei)

    tac = imei_norm[:8]
    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        cursor.execute('SELECT NOT EXISTS (SELECT * FROM gsma_data WHERE tac = %s) AS not_in_gsma', [tac])
        rt_gsma_not_found = cursor.fetchone()[0]

        condition_results = get_conditions(cursor, imei_norm)

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
        resp['realtime_checks']['in_registration_list'] = is_in_registration_list(db_conn, cursor, imei_norm)

        # add a real-time check for if IMEI was ever observed on the network
        resp['realtime_checks']['ever_observed_on_network'] = ever_observed_on_network(cursor, imei_norm)
        resp['is_paired'] = is_paired(cursor, imei_norm)
        if include_seen_with:
            resp['seen_with'] = get_subscribers(cursor, imei_norm)
        if include_paired_with:
            cursor.execute("""SELECT imsi
                                FROM pairing_list
                               WHERE imei_norm = %(imei_norm)s
                                 AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)""",
                           {'imei_norm': imei_norm})
            resp['paired_with'] = [x.imsi for x in cursor]

        return jsonify(IMEI().dump(resp).data)
