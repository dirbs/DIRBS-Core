"""
DIRBS REST-ful TAC API module.

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


def api(tac):
    """TAC API endpoint (version 1)."""
    if len(tac) != 8:
        abort(400, 'Bad TAC format')

    try:
        int(tac)
    except ValueError:
        abort(400, 'Bad TAC format')

    with get_db_connection() as db_conn, db_conn.cursor() as cursor:
        cursor.execute('SELECT * FROM gsma_data WHERE tac = %s', [tac])
        rec = cursor.fetchone()

        if rec is None:
            return jsonify(GSMATacInfo().dump(dict(tac=tac, gsma=None)).data)
        return jsonify(GSMATacInfo().dump(dict(tac=tac, gsma=rec._asdict())).data)


class TacApi:
    """TAC API version 2 methods."""

    @staticmethod
    def _validate_tac(val):
        """Validate TAC input argument format."""
        if len(val) != 8:
            abort(400, 'Bad TAC format')

        try:
            int(val)
        except ValueError:
            abort(400, 'Bad Tac format')

    def get(self, tac):
        """TAC GET API endpoint (version 2)."""
        self._validate_tac(tac)
        with get_db_connection() as db_conn, db_conn.cursor() as cursor:
            cursor.execute("""SELECT tac, manufacturer, bands, allocation_date, model_name, device_type,
                                     optional_fields
                                FROM gsma_data
                               WHERE tac = %s""", [tac])
            gsma_data = cursor.fetchone()
            print(gsma_data)
            return jsonify(TacInfo().dump(dict(tac=tac,
                                          gsma=gsma_data._asdict() if gsma_data is not None else None)).data)

    def post(self, **kwargs):
        """TAC POST API endpoint (version 2)."""
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


class GSMA(Schema):
    """Defines the GSMA schema for API V1."""

    marketing_name = fields.String()
    internal_model_name = fields.String()
    manufacturer = fields.String()
    bands = fields.String()
    allocation_date = fields.String()
    country_code = fields.String()
    fixed_code = fields.String()
    manufacturer_code = fields.String()
    radio_interface = fields.String()
    brand_name = fields.String()
    model_name = fields.String()
    operating_system = fields.String()
    nfc = fields.String()
    bluetooth = fields.String()
    wlan = fields.String()
    device_type = fields.String()

    @pre_dump(pass_many=False)
    def extract_fields(self, data):
        """Flatten the optional_fields to schema fields."""
        for key in data['optional_fields']:
            data[key] = data['optional_fields'][key]


class GSMAV2(Schema):
    """Defines the GSMA schema for API V2."""

    allocation_date = fields.String()
    bands = fields.String()
    brand_name = fields.String()
    device_type = fields.String()
    internal_model_name = fields.String()
    manufacturer = fields.String()
    marketing_name = fields.String()
    model_name = fields.String()
    bluetooth = fields.String()
    nfc = fields.String()
    wlan = fields.String()
    radio_interface = fields.String()

    @pre_dump(pass_many=False)
    def extract_fields(self, data):
        """Map optional fields to corresponding schema fields."""
        for key in data['optional_fields']:
            data[key] = data['optional_fields'][key]


class GSMATacInfo(Schema):
    """Defines the schema for TAC API(version 1) response."""

    tac = fields.String(required=True)
    gsma = fields.Nested(GSMA, required=True)


class TacInfo(Schema):
    """Defines the schema for TAC API(version 2) response."""

    tac = fields.String(required=True)
    gsma = fields.Nested(GSMAV2, required=True)


class BatchTacInfo(Schema):
    """Defines schema for Batch TAC API version 2 response."""

    results = fields.List(fields.Nested(TacInfo, required=True))


class TacArgs(Schema):
    """Input args for TAC POST API (version 2)."""

    # noinspection PyProtectedMember
    tacs = fields.List(fields.String(required=True, validate=TacApi._validate_tac))

    @property
    def fields_dict(self):
        """Convert declared fields to dictionary."""
        return self._declared_fields
