"""
DIRBS REST-ful TAC API module.

SPDX-License-Identifier: BSD-4-Clause-Clear

Copyright (c) 2018 Qualcomm Technologies, Inc.

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
limitations in the disclaimer below) provided that the following conditions are met:

    - Redistributions of source code must retain the above copyright notice, this list of conditions and the following
      disclaimer.
    - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
      following disclaimer in the documentation and/or other materials provided with the distribution.
    - All advertising materials mentioning features or use of this software, or any deployment of this software,
      or documentation accompanying any distribution of this software, must display the trademark/logo as per the
      details provided here: https://www.qualcomm.com/documents/dirbs-logo-and-brand-guidelines
    - Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or
      promote products derived from this software without specific prior written permission.



SPDX-License-Identifier: ZLIB-ACKNOWLEDGEMENT

Copyright (c) 2018 Qualcomm Technologies, Inc.

This software is provided 'as-is', without any express or implied warranty. In no event will the authors be held liable
for any damages arising from the use of this software. Permission is granted to anyone to use this software for any
purpose, including commercial applications, and to alter it and redistribute it freely, subject to the following
restrictions:

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

from flask import abort, jsonify

from dirbs.api.common.db import get_db_connection
from dirbs.api.common.tac import validate_tac
from dirbs.api.v2.schemas.tac import TacInfo


def tac_api(tac):
    """
    TAC GET API endpoint (version 2).

    :param tac: gsma tac
    :return: json
    """
    validate_tac(tac)
    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        cursor.execute("""SELECT tac, manufacturer, bands, allocation_date, model_name, device_type,
                                 optional_fields
                            FROM gsma_data
                           WHERE tac = %s""", [tac])
        gsma_data = cursor.fetchone()
        return jsonify(TacInfo().dump(dict(tac=tac,
                                           gsma=gsma_data._asdict() if gsma_data is not None else None)).data)


def tac_batch_api(**kwargs):
    """
    TAC POST API endpoint (version 2).

    :param kwargs: list of gsma tacs
    :return: json
    """
    if kwargs is not None:
        tacs = kwargs.get('tacs')
        if tacs is not None:
            tacs = list(set(tacs))
        else:
            abort(400, 'Bad TAC Input format.')

        if tacs is not None:
            if not len(tacs) > 1000 and not len(tacs) == 0:
                with get_db_connection() as db_conn, db_conn.cursor() as cursor:
                    cursor.execute("""SELECT tac, manufacturer, bands, allocation_date, model_name, device_type,
                                             optional_fields
                                        FROM gsma_data
                                       WHERE tac IN %(tacs)s""", {'tacs': tuple(tacs)})
                    gsma_data = cursor.fetchall()
                    response = []
                    for rec in gsma_data:
                        response.append(TacInfo().dump(dict(tac=rec.tac,
                                                            gsma=rec._asdict())).data)
                    existing_tacs = [res['tac'] for res in response]
                    for tac in tacs:
                        if tac not in existing_tacs:
                            response.append(TacInfo().dump(dict(tac=tac, gsma=None)).data)
                    return jsonify({'results': response})
            abort(400, 'Bad TAC Input format (Minimum 1 & Maximum 1000 allowed).')
        abort(400, 'Bad TAC Input format.')
    abort(400, 'Bad TAC Input format.')
