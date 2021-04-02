"""
Package for DIRBS REST-ful API (version 2).

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

from flask import Blueprint
from flask_apispec import use_kwargs, marshal_with, doc

from dirbs.api.common.cache import cache
from dirbs.api.v2.resources import imei as imei_resource
from dirbs.api.v2.schemas.imei import IMEI, BatchIMEI, IMEIBatchArgs, IMEISubscribers, \
    SubscriberArgs, IMEIPairings, IMEIInfo, IMEIArgs
from dirbs.api.v2.resources import catalog as catalog_resource
from dirbs.api.v2.resources import msisdn as msisdn_resource
from dirbs.api.v2.resources import job_metadata as job_resource
from dirbs.api.v2.resources import tac as tac_resource
from dirbs.api.v2.resources import version as version_resource
from dirbs.api.v2.schemas.job_metadata import JobMetadataArgs, Jobs
from dirbs.api.v2.schemas.catalog import Catalog, CatalogArgs
from dirbs.api.v2.schemas.msisdn import MSISDNResp
from dirbs.api.v2.schemas.tac import TacInfo, TacArgs, BatchTacInfo
from dirbs.api.v2.schemas.version import Version
from dirbs.api.common.handlers import validate_error, disable_options_method

api = Blueprint('v2', __name__.split('.')[0])


@api.app_errorhandler(422)
def validation_errors(error):
    """
    Transform marshmallow validation errors to custom responses to maintain backward-compatibility.

    Arguments:
        error: intercepted 422 http error
    Returns:
        Modified error response
    """
    return validate_error(error)


def register_docs(api_doc):
    """
    Register all endpoints with the ApiDoc object.

    Arguments:
        api_doc: Flask Apispec instance to register
    Returns:
        None
    """
    for endpoint in [version_api, tac_post_api, tac_get_api, msisdn_get_api, imei_get_api,
                     imei_get_subscribers_api, imei_get_pairings_api, imei_batch_api,
                     job_metadata_get_api, catalog_get_api, imei_info_api]:
        api_doc.register(endpoint, blueprint='v2')


@doc(description='Information Core knows about each TAC (max:1000) in the batch request, from both the GSMA '
                 'database and the device registration system.', tags=['TAC'])
@api.route('/tac', methods=['POST'])
@marshal_with(BatchTacInfo, code=200, description='On success (TAC found in GSMA database)')
@use_kwargs(TacArgs().fields_dict, locations=['json'])
@marshal_with(None, code=400, description='Bad TAC format')
@disable_options_method()
@cache.memoize()
def tac_post_api(**kwargs: dict) -> str:
    """
    Batch TAC API (version 2) POST route.

    Arguments:
        kwargs: list of TACs to process
    Returns:
        JSON response
    """
    return tac_resource.tac_batch_api(**kwargs)


@doc(description='Fetch TAC information from both the GSMA database and the device registration system.', tags=['TAC'])
@api.route('/tac/<tac>', methods=['GET'])
@marshal_with(TacInfo, code=200, description='On success (TAC found in GSMA database)')
@marshal_with(None, code=400, description='Bad TAC format')
@disable_options_method()
@cache.memoize()
def tac_get_api(tac: str) -> str:
    """
    TAC API (version 2) GET route.

    Arguments:
        tac: 8 digit TAC
    Returns:
        JSON response
    """
    return tac_resource.tac_api(tac)


@doc(description='Information Core knows about the MSISDN. It returns a list of IMEI, '
                 'IMSI, GSMA Manufacturer, GSMA Model Name for the MSISDN specified.', tags=['MSISDN'])
@api.route('/msisdn/<msisdn>', methods=['GET'])
@marshal_with(MSISDNResp, code=200, description='On success (MSISDN info found in database)')
@marshal_with(None, code=400, description='Bad MSISDN format')
@disable_options_method()
@cache.memoize()
def msisdn_get_api(msisdn: str) -> str:
    """
    MSISDN API (version 2) GET route.

    Arguments:
        msisdn: 15 digit MSISDN
    Returns:
        JSON response
    """
    return msisdn_resource.msisdn_api(msisdn)


@doc(description='Information Core knows about the IMEI, as well as the results of '
                 'all conditions evaluated as part of DIRBS core. Calling systems should expose as '
                 'little or as much of this information to the end user as is appropriate.', tags=['IMEI'])
@api.route('/imei/<imei>', methods=['GET'])
@use_kwargs(IMEIArgs().fields_dict, locations=['query'])
@marshal_with(IMEI, code=200, description='On success (IMEI info found in Core)')
@marshal_with(None, code=400, description='Bad IMEI format')
@disable_options_method()
@cache.memoize()
def imei_get_api(imei: str, **kwargs: dict) -> str:
    """
    IMEI API (version 2.0) GET route.

    Arguments:
        imei: 14-15 digit IMEI
        kwargs: other optional keyword arguments
    Returns:
        JSON response
    """
    return imei_resource.imei_api(imei, **kwargs)


@doc(description='Information Core knows about the IMSI-MSISDN pairs the IMEI has been '
                 'seen with on the network. The results are returned in paginated format.', tags=['IMEI'])
@api.route('/imei/<imei>/subscribers', methods=['GET'])
@use_kwargs(SubscriberArgs().fields_dict, locations=['query'])
@marshal_with(IMEISubscribers, code=200, description='On success (Info found in database)')
@marshal_with(None, code=400, description='Bad IMEI format')
@disable_options_method()
@cache.memoize()
def imei_get_subscribers_api(imei: str, **kwargs: dict) -> str:
    """
    IMEI Subscribers API (version 2.0) GET route.

    Arguments:
        imei: 14-15 digit IMEI
        kwargs: other optional kwargs
    Returns:
        JSON response
    """
    return imei_resource.imei_subscribers_api(imei, **kwargs)


@doc(description='Information Core knows about the IMSIs paired with the IMEI in the '
                 'device pairing system. The results are returned in paginated format.', tags=['IMEI'])
@api.route('/imei/<imei>/pairings', methods=['GET'])
@use_kwargs(SubscriberArgs().fields_dict, locations=['query'])
@marshal_with(IMEIPairings, code=200, description='On success (Info found in database)')
@marshal_with(None, code=400, description='Bad IMEI format')
@disable_options_method()
@cache.memoize()
def imei_get_pairings_api(imei: str, **kwargs: dict) -> str:
    """
    IMEI Pairings API (version 2.0) GET route.

    Arguments:
        imei: 14-15 digits IMEI
        kwargs: other optional kwargs
    Returns:
        JSON response
    """
    return imei_resource.imei_pairings_api(imei, **kwargs)


@doc(description='Information (such as make, model, brand etc) Core knows about an IMEI in the '
                 'Device Registration System.', tags=['IMEI'])
@api.route('/imei/<imei>/info', methods=['GET'])
@marshal_with(IMEIInfo, code=200, description='On success (Info found in database)')
@marshal_with(None, code=400, description='Bad IMEI format')
@disable_options_method()
@cache.memoize()
def imei_info_api(imei: str) -> str:
    """
    IMEI-Info API (Version 2.0) GET route.

    Arguments:
        imei: 14-15 digits IMEI
    Returns:
        JSON response
    """
    return imei_resource.imei_info_api(imei)


@doc(description='Information Core knows about each IMEI (max:1000) in the batch request, '
                 'as well as the results of all conditions evaluated as part of DIRBS core.', tags=['IMEI'])
@api.route('/imei-batch', methods=['POST'])
@use_kwargs(IMEIBatchArgs().fields_dict, locations=['json'])
@marshal_with(BatchIMEI, code=200, description='On success (Info found in database)')
@marshal_with(None, code=400, description='Bad IMEI format')
@disable_options_method()
@cache.memoize()
def imei_batch_api(**kwargs: dict) -> str:
    """
    IMEI Batch API (version 2.0) POST route.

    Arguments:
        kwargs: list of IMEIs to process
    Returns:
        JSON response
    """
    return imei_resource.imei_batch_api(**kwargs)


@doc(description='Information Core knows about the DIRBS jobs run on the system.'
                 'It is intended to be used by operational staff to generate data '
                 'for the admin panel.', tags=['Jobs'])
@api.route('/job_metadata', methods=['GET'])
@use_kwargs(JobMetadataArgs().fields_dict, locations=['query'])
@marshal_with(Jobs, code=200, description='On success')
@marshal_with(None, code=400, description='Bad parameter value')
@disable_options_method()
def job_metadata_get_api(**kwargs: dict) -> str:
    """
    Job Metadata API GET route.

    Arguments:
        kwargs: input arguments
    Returns:
        JSON response
    """
    return job_resource.job_metadata_api(**kwargs)


@doc(description='Information Core knows about the cataloged data files.'
                 'It returns a list of files along with their properties'
                 'and state of validation checks run by Core.', tags=['Catalog'])
@api.route('/catalog', methods=['GET'])
@use_kwargs(CatalogArgs().fields_dict, locations=['query'])
@marshal_with(Catalog, code=200, description='On success')
@marshal_with(None, code=400, description='Bad parameter value')
@disable_options_method()
def catalog_get_api(**kwargs: dict) -> str:
    """
    Catalog API GET route.

    Arguments:
        kwargs: input arguments
    Returns:
        JSON response
    """
    return catalog_resource.catalog_api(**kwargs)


@doc(description='Information about the code and DB schema version used by Core and presence of potential whitespace '
                 'IMSIs and MSISDNs in imported operator data.', tags=['Version'])
@api.route('/version', methods=['GET'])
@marshal_with(Version, code=200, description='On success')
@disable_options_method()
def version_api() -> str:
    """Version API (version 2) route."""
    return version_resource.version()
