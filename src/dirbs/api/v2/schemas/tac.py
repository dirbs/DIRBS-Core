"""
DIRBS REST-ful TAC API schema module.

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
from marshmallow import Schema, fields, pre_dump, validate


class GSMA(Schema):
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
    imeiquantitysupport = fields.String()
    simslot = fields.String()
    operating_system = fields.String()
    marketing_name = fields.String()
    country_code = fields.String()
    fixed_code = fields.String()
    removable_uicc = fields.String()
    removable_euicc = fields.String()
    nonremovable_uicc = fields.String()
    nonremovable_euicc = fields.String()

    @pre_dump(pass_many=False)
    def extract_fields(self, data):
        """
        Map optional fields to corresponding schema fields.

        :param data: dumped data
        """
        for key in data['optional_fields']:
            data[key] = data['optional_fields'][key]


class TacInfo(Schema):
    """Defines the schema for TAC API(version 2) response."""

    tac = fields.String(required=True)
    gsma = fields.Nested(GSMA, required=True)


class BatchTacInfo(Schema):
    """Defines schema for Batch TAC API version 2 response."""

    results = fields.List(fields.Nested(TacInfo, required=True))


class TacArgs(Schema):
    """Input args for TAC POST API (version 2)."""

    # noinspection PyProtectedMember
    tacs = fields.List(fields.String(required=True,
                                     validate=validate.Length(min=8, max=8, error='TAC length must be 8 characters')),
                       required=True,
                       validate=validate.Length(min=1, max=1000, error='Min 1 and Max 1000 TACs are allowed'))

    @property
    def fields_dict(self):
        """Convert declared fields to dictionary."""
        return self._declared_fields
