"""
DIRBS REST-ful job_metadata API schema module.

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

from marshmallow import Schema, fields, validate


class JobMetadata(Schema):
    """Define schema for the metadata associated with a DIRBS job."""

    command = fields.String(required=True)
    subcommand = fields.String(required=True)
    command_line = fields.String(required=True)
    db_user = fields.String(required=True)
    start_time = fields.DateTime(required=True)
    end_time = fields.DateTime(required=True)
    exception_info = fields.String(required=True)
    run_id = fields.Integer(required=True)
    status = fields.String(required=True)
    extra_metadata = fields.Dict(required=False)


class JobStatus(Enum):
    """Enum for the supported data import file types."""

    RUNNING = 'running'
    SUCCESS = 'success'
    ERROR = 'error'


class JobCommandType(Enum):
    """Enum for the supported job command types."""

    CATALOG = 'dirbs-catalog'
    CLASSIFY = 'dirbs-classify'
    DB = 'dirbs-db'
    IMPORT = 'dirbs-import'
    LISTGEN = 'dirbs-listgen'
    PRUNE = 'dirbs-prune'
    REPORT = 'dirbs-report'


class SortingOrders(Enum):
    """Enum for supported sorting orders."""

    ASC = 'ASC'
    DESC = 'DESC'


class JobMetadataArgs(Schema):
    """Input arguments for the job metadata API."""

    command = fields.List(fields.String(validate=validate.OneOf([f.value for f in JobCommandType])),
                          required=False, missing=[], description='Filter results to include only '
                                                                  'jobs belonging to specified command(s)')
    subcommand = fields.List(fields.String(), required=False, missing=[],
                             description='Filter results to include only jobs belonging to specified subcommand(s)')
    run_id = fields.List(fields.Integer(validate=validate.Range(min=1)), required=False, missing=[],
                         description='Filter results to only include job with the specified run_id(s)')
    status = fields.List(fields.String(validate=validate.OneOf([f.value for f in JobStatus])),
                         required=False, missing=[], description='Filter results to only include jobs '
                                                                 'having the specified status')
    show_details = fields.Boolean(required=False, missing=True, description='Whether or not to include '
                                                                            "\'extra_metadata\' field in the results")
    offset = fields.Integer(missing=0,
                            validate=[validate.Range(min=0, error='Value must be 0 or greater than 0')],
                            description='Offset the results on the current page by the specified '
                                        'run_id. It should be the value of run_id for the last '
                                        'result on the previous page')
    order = fields.String(missing='ASC',
                          validate=validate.OneOf([f.value for f in SortingOrders]),
                          description='The sort order for the results using start_time as the key')
    limit = fields.Integer(missing=10,
                           validate=validate.Range(min=1, error='Value must be greater than 0'),
                           description='Number of results to return on the current page')

    @property
    def fields_dict(self):
        """Convert declared fields to dictionary."""
        return self._declared_fields


class JobKeys(Schema):
    """Defines schema for keys of paginated result set."""

    current_key = fields.String()
    next_key = fields.String()
    result_size = fields.Integer()


class Jobs(Schema):
    """Defines schema for the metadata associated with DIRBS jobs."""

    _keys = fields.Nested(JobKeys, required=True)
    jobs = fields.List(fields.Nested(JobMetadata, required=True))
