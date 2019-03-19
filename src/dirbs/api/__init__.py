"""
Package for DIRBS REST-ful API modules.

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
import sys
import time
import logging
from datetime import datetime

from flask import Flask, request, g
from werkzeug.exceptions import HTTPException, InternalServerError, BadRequest, ServiceUnavailable

import dirbs.utils as utils
from dirbs.config import ConfigParser, ConfigParseException
from dirbs.logging import StatsClient, setup_initial_logging, configure_logging, setup_file_logging
from dirbs.api.v1 import api, register_docs
from dirbs.api.v2 import api as api_v2, register_docs as register_docs_v2
from dirbs.api.common.db import close_db_connection, get_db_connection
from dirbs.api.common.apidoc import ApiDoc


# HACK: We import _strptime to avoid weird issues as described at
# http://bugs.python.org/msg221094
datetime.strptime('', '')

# Setup initial logging first so we make sure we catch any errors parsing the config, etc.
setup_initial_logging()

# Create WSGI app instance - this __name__.split('.') handles
# the case where the file is part of a package.
app = Flask(__name__.split('.')[0])

# Update config to disable standard logging handlers and set our logger name
app.config.update(
    LOGGER_NAME='dirbs.flask',
    LOGGER_HANDLER_POLICY='never'
)

# Create ApiDoc object(s) to document the OpenAPI spec for each version of the API
docs_v1 = ApiDoc(app, version='v1')
docs_v2 = ApiDoc(app, version='v2')

# Parse config
try:
    cp = ConfigParser()
    app.config['DIRBS_CONFIG'] = cp.parse_config(ignore_env=False)
except ConfigParseException:
    logger = logging.getLogger('dirbs.config')
    logger.critical('Exception encountered during parsing of config (.yml file). '
                    'See console above for details...')
    sys.exit(1)

# Update the logging based on the parsed config
configure_logging(app.config['DIRBS_CONFIG'].log_config)

# Initialize file logging
setup_file_logging(app.config['DIRBS_CONFIG'].log_config, 'dirbs-api')

# Init statsd client
statsd = StatsClient(app.config['DIRBS_CONFIG'].statsd_config)

# Init custom JSONEncoder (handles dates, etc.)
app.json_encoder = utils.JSONEncoder


def _metrics_type_from_req_ctxt(req):
    """Utility method to get a metrics path for the API type."""
    p = req.path.split('/')
    if (p[1] == 'api') and len(p) > 3:
        # Metrics are in format of 'imei.v1' to allow metrics to be aggregated across versions for the same API
        api_type = p[3]
        api_version = p[2]
        return '{0}.{1}'.format(api_type, api_version)
    else:
        return 'unknown'


@app.before_first_request
def verify_schema():
    """Function to verify the schema before the first request."""
    with get_db_connection() as conn:
        try:
            utils.verify_db_schema(conn, 'dirbs_core_api')
        except utils.DatabaseSchemaException:
            raise ServiceUnavailable(description='Invalid database schema or database schema requires upgrade')


@app.before_first_request
def validate_exempted_device_types():
    """Function to validate exempted devices types before the first request."""
    with get_db_connection() as conn:
        try:
            utils.validate_exempted_device_types(conn, app.config['DIRBS_CONFIG'])
        except ConfigParseException:
            msg = 'Exempted device types specified in config are not valid device types as per GSMA database.'
            raise ServiceUnavailable(description=msg)


@app.before_request
def log_api_perf_start():
    """Logs the start time of every request by using Flasks's before_request hook."""
    g.request_start_time = time.time()


@app.after_request
def add_no_cache(response):
    """Makes sure no API responses are cached by setting headers on the response."""
    response.cache_control.no_cache = True
    response.cache_control.no_store = True
    response.cache_control.must_revalidate = True
    response.cache_control.max_age = 0
    response.headers['Pragma'] = 'no-cache'
    response.expires = 0
    return response


@app.after_request
def log_api_successes(response):
    """Makes sure we record the number of successful API responses for each API."""
    code = response.status_code
    if 200 <= code < 300:
        statsd.incr('dirbs.api.successes.{0}.{1}'.format(_metrics_type_from_req_ctxt(request), code))
    return response


@app.after_request
def add_security_headers(response):
    """Makes sure appropriate security headers are added for each API."""
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    return response


@app.after_request
def log_api_perf_end(response):
    """Logs the response time of every request to StatsD."""
    start_time = getattr(g, 'request_start_time', None)
    if start_time is not None:
        dt = int((time.time() - start_time) * 1000)
        api_type = _metrics_type_from_req_ctxt(request)
        code = response.status_code
        statsd.timing('dirbs.api.response_time.{0}.{1}.{2}'.format(api_type, request.method, code), dt)
    else:
        logger = logging.getLogger('dirbs.flask')
        logger.warn('request_start_time not present on app context - likely an exception in a before_request handler!')
    return response


@app.teardown_appcontext
def on_request_end(error):
    """Make sure we always close the DB at the end of a request."""
    close_db_connection()


@app.errorhandler(400)
@app.errorhandler(403)
@app.errorhandler(404)
@app.errorhandler(405)
@app.errorhandler(500)
@app.errorhandler(Exception)
def app_error_handler(error):
    """Make sure we log metrics for all failures."""
    if isinstance(error, HTTPException):
        code = 400 if error.code == 422 else error.code
    else:
        code = 500

    metrics_api_type = _metrics_type_from_req_ctxt(request)
    statsd.incr('dirbs.api.failures.{0}.{1}'.format(metrics_api_type, code))
    if code == 500:
        # Log exception to the dirbs.exception logger
        logger = logging.getLogger('dirbs.exception')
        logger.error('DIRBS encountered an uncaught software exception', exc_info=sys.exc_info())
        statsd.incr('dirbs.exceptions.api.{0}'.format(metrics_api_type))

    # Send a 400 response back if query parameter validation fails.
    if isinstance(error, HTTPException) and error.code == 422:
        return BadRequest(description=error.exc.messages)

    if isinstance(error, HTTPException):
        return error.get_response(environ=None), code
    elif app.propagate_exceptions:
        # In debug or testing mode, re-raise any non-HTTP exceptions to trigger the debugger
        raise error
    else:
        return InternalServerError().get_response()


# Register versioned blueprints on the app at the appropriate URL prefix
app.register_blueprint(api, url_prefix='/api/v1')
app.register_blueprint(api_v2, url_prefix='/api/v2')

# Register endpoints for OpenAPI documentation
register_docs(docs_v1)
register_docs_v2(docs_v2)
