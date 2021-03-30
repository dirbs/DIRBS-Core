"""
DIRBS REST-ful IMEI API Schema module.

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
from marshmallow import Schema, fields


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
                                       description="Whether or not to include \'seen_with\' field in the response")
    include_paired_with = fields.Boolean(required=False, missing=False,
                                         description="Whether or not to include \'paired_with\' field in the response")

    @property
    def fields_dict(self):
        """Convert declared fields to dictionary."""
        return self._declared_fields
