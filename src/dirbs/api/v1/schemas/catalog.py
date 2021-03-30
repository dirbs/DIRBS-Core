"""
DIRBS REST-ful data_catalog API schema module.

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
from enum import Enum

from marshmallow import Schema, fields, pre_dump, validate


class FileType(Enum):
    """Enum for the supported data import file types."""

    OPERATOR = 'operator'
    GSMA = 'gsma_tac'
    STOLEN = 'stolen_list'
    PAIRING = 'pairing_list'
    REGISTRATION = 'registration_list'
    GOLDEN = 'golden_list'


class CatalogFile(Schema):
    """Defines the schema for the cataloged file."""

    file_id = fields.Integer()
    filename = fields.String()
    file_type = fields.String()
    compressed_size_bytes = fields.Integer()
    modified_time = fields.DateTime()
    is_valid_zip = fields.Boolean()
    is_valid_format = fields.Boolean()
    md5 = fields.String()
    extra_attributes = fields.Dict()
    first_seen = fields.DateTime()
    last_seen = fields.DateTime()
    uncompressed_size_bytes = fields.Integer()
    num_records = fields.Integer()
    import_status = fields.Dict()

    @pre_dump(pass_many=False)
    def extract_fields(self, data, **kwargs):
        """
        Extract import status.

        :param data: input data dump
        """
        data['import_status'] = {
            'ever_imported_successfully': True if 'success' in data['status_list'] else False,
            'most_recent_import': data['status_list'][0] if data['status_list'] else None
        }


class Catalog(Schema):
    """Defines the schema for data catalog API response."""

    catalog = fields.List(fields.Nested(CatalogFile))


class CatalogArgs(Schema):
    """Input arguments for the Catalog API."""

    max_results = fields.Integer(required=False, description='Number of entries to return '
                                                             'sorted by last_seen timestamp in descending order')
    file_type = fields.String(required=False, validate=validate.OneOf([f.value for f in FileType]),
                              description='Filter results to include only the specified file type')
    is_valid_zip = fields.Boolean(required=False, description='Filter results to include only valid ZIP files')
    modified_time = fields.DateTime(required=False, format='%Y%m%d',
                                    load_from='modified_since', dump_to='modified_since',
                                    description='Filter results to only include files '
                                                'that were modified since the specified time')
    last_seen = fields.DateTime(required=False, format='%Y%m%d',
                                load_from='cataloged_since', dump_to='cataloged_since',
                                description='Filter results to include only files that were '
                                            'cataloged since the specified time')

    @property
    def fields_dict(self):
        """Convert declared fields to dictionary."""
        return self._declared_fields
