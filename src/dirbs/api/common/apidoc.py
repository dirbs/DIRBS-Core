"""
Subclass FlaskApiSpec to add support for documenting multiple API versions.

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
from apispec import APISpec
from flask import Blueprint, url_for
from flask_apispec.extension import FlaskApiSpec
from apispec.ext.marshmallow import MarshmallowPlugin
from flask_apispec.apidoc import ViewConverter, ResourceConverter


class ApiDoc(FlaskApiSpec):
    """Override base FlaskApiSpec constructor."""

    def __init__(self, app, *, version: str):
        """
        Constructor.

        Arguments:
            app: current flask/wsgi app instance
            version: version of the api (string format)
        """
        self.title = 'DIRBS Core'
        self.version = version
        self.apidoc_ui_url = '/apidocs/{0}/'.format(self.version)
        self.apidoc_json_url = '/apidocs-json/{0}/'.format(self.version)

        self._deferred = []
        self.app = app
        self.view_converter = None
        self.resource_converter = None
        self.spec = None
        self.app = app
        self.init_app()

    def init_app(self, **kwargs: dict) -> None:
        """
        Override base init_app method.

        Arguments:
            kwargs: required kwargs
        """
        self.spec = APISpec(
            title=self.title,
            version=self.version,
            info={'description': self.top_level_description},
            plugins=[MarshmallowPlugin()],
            openapi_version='2.0'
        )

        self.resource_converter = ResourceConverter(self.app, self.spec)
        self.view_converter = ViewConverter(self.app, self.spec)
        self.add_swagger_routes()

        for deferred in self._deferred:
            deferred()

    def add_swagger_routes(self) -> None:
        """Override base add_swagger_routes method.

        Define blueprint for the OpenAPI spec to be served.
        """
        spec_blueprint = Blueprint(
            'flask-apispec-{0}'.format(self.version),
            FlaskApiSpec.__module__,
            static_folder='./static',
            template_folder='./templates',
            static_url_path='/flask-apispec/static',
        )

        @spec_blueprint.context_processor
        def override_url_for():
            return dict(url_for=custom_url_for)

        def custom_url_for(endpoint, **values):
            """
            Method to map custom urls for swagger.

            Arguments:
                endpoint: swagger designated endpoint
                values: url values to map to endpoints
            """
            endpoint = endpoint.replace('flask-apispec', 'flask-apispec-{0}'.format(self.version))
            return url_for(endpoint, **values)

        spec_blueprint.add_url_rule(self.apidoc_json_url, 'swagger-json', self.swagger_json)
        spec_blueprint.add_url_rule(self.apidoc_ui_url, 'swagger-ui', self.swagger_ui)
        self.app.register_blueprint(spec_blueprint)

    @property
    def top_level_description(self) -> str:
        """Generate text for top level API document description."""
        description = 'The document lists the APIs exposed by DIRBS Core system. ' \
                      'The APIs provide programmatic access to read data from DIRBS Core. ' \
                      'This documentation was built using Swagger UI. ' \
                      "Swagger UI allows users to visualize and interact with the API\'s resources " \
                      'without having any of the implementation logic in place. ' \
                      '\n' \
                      '## MIME Types ' \
                      '\n' \
                      'The Core API supports [RFC 6838](https://tools.ietf.org/html/rfc6838) ' \
                      'compliant media types:' \
                      '\n\t * application/json' \
                      '\n' \
                      '## HTML Status Codes and Error Handling ' \
                      '\n' \
                      'The Core API will attempt to send the appropriate HTML status codes. ' \
                      'On error, the request response will contain details about the error cause when possible.' \
                      '\n\n' \
                      'Copyright \xA9 2019 Qualcomm Technologies, Inc. All rights reserved.'
        return description
