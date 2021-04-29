"""
Package for DIRBS REST-ful API (version 1).

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

from typing import Callable

from flask import Blueprint
from flask_apispec import use_kwargs, marshal_with, doc

from dirbs.api.common.cache import cache
from dirbs.api.v1.resources import imei as imei_resource
from dirbs.api.v1.resources import version as version_resource
from dirbs.api.v1.resources import tac as tac_resource
from dirbs.api.v1.resources import msisdn as msisdn_resource
from dirbs.api.v1.resources import catalog as catalog_resource
from dirbs.api.v1.resources import job_metadata as job_resource
from dirbs.api.v1.schemas.job_metadata import JobMetadata, JobMetadataArgs
from dirbs.api.v1.schemas.msisdn import MSISDN
from dirbs.api.v1.schemas.tac import GSMATacInfo
from dirbs.api.v1.schemas.catalog import Catalog, CatalogArgs
from dirbs.api.v1.schemas.version import Version
from dirbs.api.v1.schemas.imei import IMEI, IMEIArgs
from dirbs.api.common.handlers import validate_error, disable_options_method


api = Blueprint('v1', __name__.split('.')[0])


@api.app_errorhandler(422)
def validation_errors(error) -> Callable:
    """
    Transform marshmallow validation errors to custom responses to maintain backward-compatibility.

    Arguments:
        error: intercepted http error message object
    Returns:
        f(): custom validated error message
    """
    return validate_error(error)


def register_docs(apidoc) -> None:
    """
    Register all endpoints with the ApiDoc object.

    Arguments:
        apidoc: flask-apispec api doc object to register
    Returns:
        None
    """
    for endpoint in [tac_api, catalog_api, version_api, msisdn_api, imei_api, job_metadata_api]:
        apidoc.register(endpoint, blueprint='v1')


@doc(description="Information Core knows about the IMEI, as well as the results of all \'conditions\' "
                 'evaluated as part of DIRBS core. Calling systems should expose as little or as much '
                 'of this information to the end user as is appropriate.', tags=['IMEI'])
@api.route('/imei/<imei>', methods=['GET'])
@use_kwargs(IMEIArgs().fields_dict, locations=['query'])
@marshal_with(IMEI, code=200, description='On success')
@marshal_with(None, code=400, description='Bad parameter value')
@disable_options_method()
@cache.memoize()
def imei_api(imei: str, **kwargs: dict) -> Callable[[str, dict], str]:
    """
    IMEI API route.

    Arguments:
        imei: IMEI to search and respond about
        kwargs: other optional arguments
    Returns:
        Json response
    """
    return imei_resource.imei_api(imei, **kwargs)


@doc(description='Fetch GSMA TAC information', tags=['TAC'])
@api.route('/tac/<tac>', methods=['GET'])
@marshal_with(GSMATacInfo, code=200, description='On success (TAC found in the GSMA database)')
@marshal_with(None, code=400, description='Bad TAC format')
@disable_options_method()
@cache.memoize()
def tac_api(tac: str) -> Callable[[str], str]:
    """
    TAC API route.

    Arguments:
        tac: TAC value
    Returns:
        JSON response
    """
    return tac_resource.api(tac)


@doc(description='Information Core knows about the cataloged data files. It returns a list of files '
                 'along with their properties and state of validation checks run by Core.', tags=['Catalog'])
@api.route('/catalog', methods=['GET'])
@use_kwargs(CatalogArgs().fields_dict, locations=['query'])
@marshal_with(Catalog, code=200, description='On success')
@marshal_with(None, code=400, description='Bad parameter value')
@disable_options_method()
def catalog_api(**kwargs: dict) -> Callable[[dict], str]:
    """
    Catalog API route.

    Arguments:
        kwargs: required keyword arguments
    Returns:
        JSON response
    """
    return catalog_resource.catalog_api(**kwargs)


@doc(description='Information Core knows about the DIRBS jobs run on the system. It is intended '
                 'to be used by operational staff to generate data for the admin panel.', tags=['Jobs'])
@api.route('/job_metadata', methods=['GET'])
@use_kwargs(JobMetadataArgs().fields_dict, locations=['query'])
@marshal_with(JobMetadata, code=200, description='On success')
@marshal_with(None, code=400, description='Bad parameter value')
@disable_options_method()
def job_metadata_api(**kwargs: dict) -> Callable[[dict], str]:
    """
    Job Metadata API route.

    Arguments:
        kwargs: required keyword arguments
    Returns:
        JSON response
    """
    return job_resource.job_metadata_api(**kwargs)


@doc(description='Information about the code and DB schema version used by Core and presence of '
                 'potential whitespace IMSIs and MSISDNs in imported operator data.', tags=['Version'])
@api.route('/version', methods=['GET'])
@marshal_with(Version, code=200, description='On success')
@disable_options_method()
def version_api() -> Callable[[], str]:
    """Version API route."""
    return version_resource.version()


@doc(description='Information Core knows about the MSISDN. It returns a list of IMEI, IMSI, '
                 'GSMA Manufacturer, GSMA Model Name for the MSISDN specified.', tags=['MSISDN'])
@api.route('/msisdn/<msisdn>', methods=['GET'])
@marshal_with(MSISDN, code=200, description='On success')
@disable_options_method()
@cache.memoize()
def msisdn_api(msisdn: str) -> Callable[[str], str]:
    """
    MSISDN API route.

    Arguments:
        msisdn: MSISDN value
    Returns:
        JSON response
    """
    return msisdn_resource.msisdn_api(msisdn)
