"""
DIRBS REST-ful MSISDN API module.

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
from flask import abort, jsonify

from dirbs.api.common.db import get_db_connection
from dirbs.api.v1.schemas.msisdn import MSISDN


def msisdn_api(msisdn):
    """MSISDN API endpoint."""
    if len(msisdn) > 15:
        abort(400, 'Bad MSISDN format (too long)')

    try:
        int(msisdn)
    except ValueError:
        abort(400, 'Bad MSISDN format (can only contain digit characters)')

    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        cursor.execute("""SELECT imei_norm, imsi, manufacturer AS gsma_manufacturer, model_name AS gsma_model_name
                            FROM gsma_data
                      RIGHT JOIN monthly_network_triplets_country_no_null_imeis
                                            ON SUBSTRING(imei_norm, 1, 8) = tac
                           WHERE %s = msisdn """, [msisdn])

        resp = [MSISDN().dump(rec._asdict()).data for rec in cursor]
        return jsonify(resp)
