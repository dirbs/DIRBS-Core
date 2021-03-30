"""
DIRBS REST-ful MSISDN API module.

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

from typing import Union

from flask import abort, jsonify

from dirbs.api.common.db import get_db_connection
from dirbs.api.v2.schemas.msisdn import MSISDN


def msisdn_api(msisdn: str) -> Union[abort, jsonify]:
    """
    MSISDN API (version 2) GET endpoint.

    Arguments:
        msisdn: 15 digits MSISDN value
    """
    if len(msisdn) > 15:
        abort(400, 'Bad MSISDN format (too long)')

    try:
        int(msisdn)
    except ValueError:
        abort(400, 'Bad MSISDN format (can only contain digit characters)')

    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        cursor.execute("""SELECT mntc.imei_norm, mntc.imsi, reg.imei_norm AS reg_imei, reg.make, reg.model,
                                 reg.brand_name, gsma.tac, gsma.manufacturer,
                                 gsma.model_name, gsma.optional_fields, mntc.last_seen
                            FROM gsma_data AS gsma
                      RIGHT JOIN monthly_network_triplets_country_no_null_imeis AS mntc
                                            ON SUBSTRING(imei_norm, 1, 8) = tac
                       LEFT JOIN registration_list AS reg
                                            ON reg.imei_norm = mntc.imei_norm
                           WHERE msisdn = %s """, [msisdn])
        recs = cursor.fetchall()
        data = [MSISDN().dump(dict(imei_norm=rec[0], imsi=rec[1], last_seen=rec[10],
                                   gsma=rec._asdict() if rec[6] else None,
                                   registration=rec if rec[2] else None)).data for rec in recs]

    return jsonify({'results': data})
