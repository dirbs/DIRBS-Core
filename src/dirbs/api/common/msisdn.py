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
from marshmallow import Schema, fields, pre_dump

from dirbs.api.common.db import get_db_connection


def api(msisdn):
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


class MsisdnApi:
    """MSISDN API (version 2) endpoints."""

    def get(self, msisdn):
        """MSISDN API (version 2) GET endpoint."""
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
            data = [MSISDNV2().dump(dict(imei_norm=rec[0], imsi=rec[1], last_seen=rec[10],
                                         gsma=rec._asdict() if rec[6] else None,
                                         registration=rec if rec[2] else None)).data for rec in recs]

        return jsonify({'results': data})


class MSISDN(Schema):
    """Defines the MSISDN schema."""

    imei_norm = fields.String()
    imsi = fields.String()
    gsma_manufacturer = fields.String()
    gsma_model_name = fields.String()


class GSMA(Schema):
    """Defines sub-schema for MSISDNV2 schema."""

    manufacturer = fields.String()
    model_name = fields.String()
    brand_name = fields.String()

    @pre_dump(pass_many=False)
    def extract_fields(self, data):
        """Extract and map related fields."""
        if data['optional_fields'] is not None:
            for key in data['optional_fields']:
                data[key] = data['optional_fields'][key]


class REGISTRATION(Schema):
    """Defines sub-schema for MSISDNV2 schema."""

    make = fields.String()
    model = fields.String()
    brand_name = fields.String()


class MSISDNV2(Schema):
    """Defines the MSISDN schema for API (version 2)."""

    imei_norm = fields.String()
    imsi = fields.String()
    gsma = fields.Nested(GSMA, required=True)
    registration = fields.Nested(REGISTRATION, required=True)
    last_seen = fields.String()


class MSISDNResp(Schema):
    """Defines MSISDN API (version 2.0) response schema."""

    results = fields.List(fields.Nested(MSISDNV2, required=True))
