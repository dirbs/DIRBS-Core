"""
Package for DIRBS REST-ful API (version 1).

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

from flask import Blueprint
from flask_apispec import use_kwargs, marshal_with, doc
from werkzeug.exceptions import BadRequest
from marshmallow import fields, validate

from dirbs.api.common import catalog as catalog_common, \
    job_metadata as job_metadata_common
from dirbs.api.v1.resources import imei as imei_resource
from dirbs.api.v1.resources import version as version_resource
from dirbs.api.v1.resources import tac as tac_resource
from dirbs.api.v1.resources import msisdn as msisdn_resource
from dirbs.api.v1.schemas.msisdn import MSISDN
from dirbs.api.v1.schemas.tac import GSMATacInfo
from dirbs.api.common.catalog import Catalog, CatalogArgs
from dirbs.api.v1.schemas.version import Version
from dirbs.api.v1.schemas.imei import IMEI, IMEIArgs
from dirbs.api.common.job_metadata import JobMetadata, JobMetadataArgs


api = Blueprint('v1', __name__.split('.')[0])


@api.app_errorhandler(422)
def validation_errors(error):
    """Transform marshmallow validation errors to custom responses to maintain backward-compatibility."""
    field_name = error.exc.field_names[0]
    field_value = error.exc.data[field_name]
    field_type = error.exc.fields[0]
    if isinstance(field_type, fields.List):
        field_type = error.exc.fields[0].container
        field_value = error.exc.data[field_name][next(iter(error.exc.messages[field_name]))]
    return BadRequest(description=get_error_desc(field_type, field_name, field_value))


def get_error_desc(field, name, value):
    """Helper function to construct error description."""
    error_desc = 'Bad \'{0}\':\'{1}\' argument format.'.format(name, value)
    if isinstance(field, fields.Integer):
        try:
            int(value)
            msg_allow_zero = 'or equal to ' if field.validate.min < 1 else ''
            error_desc = 'Param \'{0}\':\'{1}\' must be greater than {2}0' \
                .format(name, value, msg_allow_zero)
        except ValueError:
            error_desc = 'Bad \'{0}\':\'{1}\' argument format. Accepts only integer'.format(name, value)
    if isinstance(field, fields.Boolean):
        allowed_values = ['0', '1', 'true', 'false']
        error_desc = 'Bad \'{0}\':\'{1}\' argument format. Accepts only one of {2}'\
            .format(name, value, allowed_values)
    if isinstance(field, fields.String) and isinstance(field.validate, validate.OneOf):
        error_desc = 'Bad \'{0}\':\'{1}\' argument format. Accepts only one of {2}'\
            .format(name, value, field.validate.choices)
    if isinstance(field, fields.DateTime):
        dateformat = 'YYYYMMDD' if field.dateformat == '%Y%m%d' else field.dateformat
        error_desc = 'Bad \'{0}\':\'{1}\' argument format. Date must be in \'{2}\' format.'\
            .format(name, value, dateformat)
    return error_desc


def disable_options_method():
    """Decorator function to disable OPTIONS method on the endpoint."""
    def wrapper(f):
        f.provide_automatic_options = False
        return f
    return wrapper


def register_docs(apidoc):
    """Register all endpoints with the ApiDoc object."""
    for endpoint in [tac_api, catalog_api, version_api, msisdn_api, imei_api, job_metadata_api]:
        apidoc.register(endpoint, blueprint='v1')


@doc(description='Information Core knows about the IMEI, as well as the results of all \'conditions\' '
                 'evaluated as part of DIRBS core. Calling systems should expose as little or as much '
                 'of this information to the end user as is appropriate.', tags=['IMEI'])
@api.route('/imei/<imei>', methods=['GET'])
@use_kwargs(IMEIArgs().fields_dict, locations=['query'])
@marshal_with(IMEI, code=200, description='On success')
@marshal_with(None, code=400, description='Bad parameter value')
@disable_options_method()
def imei_api(imei, **kwargs):
    """IMEI API route."""
    return imei_resource.api(imei, **kwargs)


@doc(description='Fetch GSMA TAC information', tags=['TAC'])
@api.route('/tac/<tac>', methods=['GET'])
@marshal_with(GSMATacInfo, code=200, description='On success (TAC found in the GSMA database)')
@marshal_with(None, code=400, description='Bad TAC format')
@disable_options_method()
def tac_api(tac):
    """TAC API route."""
    return tac_resource.api(tac)


@doc(description='Information Core knows about the cataloged data files. It returns a list of files '
                 'along with their properties and state of validation checks run by Core.', tags=['Catalog'])
@api.route('/catalog', methods=['GET'])
@use_kwargs(CatalogArgs().fields_dict, locations=['query'])
@marshal_with(Catalog, code=200, description='On success')
@marshal_with(None, code=400, description='Bad parameter value')
@disable_options_method()
def catalog_api(**kwargs):
    """Catalog API route."""
    return catalog_common.api(**kwargs)


@doc(description='Information Core knows about the DIRBS jobs run on the system. It is intended '
                 'to be used by operational staff to generate data for the admin panel.', tags=['Jobs'])
@api.route('/job_metadata', methods=['GET'])
@use_kwargs(JobMetadataArgs().fields_dict, locations=['query'])
@marshal_with(JobMetadata, code=200, description='On success')
@marshal_with(None, code=400, description='Bad parameter value')
@disable_options_method()
def job_metadata_api(**kwargs):
    """Job Metadata API route."""
    return job_metadata_common.api(**kwargs)


@doc(description='Information about the code and DB schema version used by Core and presence of '
                 'potential whitespace IMSIs and MSISDNs in imported operator data.', tags=['Version'])
@api.route('/version', methods=['GET'])
@marshal_with(Version, code=200, description='On success')
@disable_options_method()
def version_api():
    """Version API route."""
    return version_resource.version()


@doc(description='Information Core knows about the MSISDN. It returns a list of IMEI, IMSI, '
                 'GSMA Manufacturer, GSMA Model Name for the MSISDN specified.', tags=['MSISDN'])
@api.route('/msisdn/<msisdn>', methods=['GET'])
@marshal_with(MSISDN, code=200, description='On success')
@disable_options_method()
def msisdn_api(msisdn):
    """MSISDN API route."""
    return msisdn_resource.msisdn_api(msisdn)
