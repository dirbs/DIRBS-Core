"""
DIRBS Core message broker configuration section parser.

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

from dirbs.config.common import ConfigSection, ConfigParseException, \
    check_for_duplicates, _logger, parse_alphanum


class BrokerConfig(ConfigSection):
    """Class representing the 'broker' section of the config."""

    def __init__(self, *, ignore_env, **broker_config):
        """Constructor which parses the broker config."""
        super(BrokerConfig, self).__init__(ignore_env=ignore_env, **broker_config)
        self.kafka = KafkaConfig(ignore_env=ignore_env, **broker_config.get('kafka'))
        self.operators = [BrokerOperatorConfig(ignore_env=ignore_env, **o) for o in broker_config.get('operators', [])]

        # Check that operator_ids are unique and case-insensitive
        dupl_op_id_found_error_message = 'Duplicate operator_ids {0} found in config. ' \
                                         'Operator_ids are case insensitive!'
        operator_id_list = [o.id for o in self.operators]
        check_for_duplicates(operator_id_list, dupl_op_id_found_error_message)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'BrokerConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'kafka': {},
            'operators': []
        }


class KafkaConfig(ConfigSection):
    """Class representing kafka config subsection in the broker config."""

    def __init__(self, **kafka_config):
        """Constructor which parses the kafka config."""
        super(KafkaConfig, self).__init__(**kafka_config)
        self.hostname = self._parse_string('hostname')
        self.port = self._parse_positive_int('port')
        self.topic = self._parse_string('topic')

        # protocol and checks, plain and ssl only
        self.security_protocol = self._parse_string('security_protocol').upper()
        if self.security_protocol not in ['SSL', 'PLAINTEXT']:
            msg = 'Invalid security protocol specified, use one on [PLAIN, SSL] only'
            _logger.error(msg)
            raise ConfigParseException(msg)

        # if security protocol is set to SSL then verify the required options
        if self.security_protocol == 'SSL':
            self.client_certificate = self._parse_file_path('client_certificate', ext='.pem')
            self.client_key = self._parse_file_path('client_key', ext='.pem')
            self.caroot_certificate = self._parse_file_path('caroot_certificate', ext='.pem')
            self.skip_tls_verifications = self._parse_bool('skip_tls_verifications')

            if self.skip_tls_verifications is True:
                _logger.warning('TLS verifications should only be turned off in DEV Env, '
                                'not recommended for production environment')
        # if security protocol is set to PLAIN show warning
        else:
            _logger.warning('Security protocol in broker config is set to PLAIN, which is not recommended '
                            'in production environment')

    @property
    def section_name(self):
        """Property for the section name."""
        return 'KafkaConfig'

    @property
    def bootstrap_server(self):
        """Property to formulate and return bootstrap server addr."""
        return '{0}:{1}'.format(self.hostname, self.port)

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'hostname': 'localhost',
            'port': 9092,
            'topic': 'dirbs',
            'security_protocol': 'PLAINTEXT',
            'skip_tls_verifications': False,
            'client_certificate': None,
            'client_key': None,
            'caroot_certificate': None
        }

    @property
    def env_overrides(self):
        """Property describing a key->envvar mapping for overriding config values."""
        return {
            'hostname': 'DIRBS_KAFKA_HOST',
            'port': 'DIRBS_KAFKA_PORT',
            'topic': 'DIRBS_KAFKA_TOPIC',
            'security_protocol': 'DIRBS_KAFKA_PROTOCOL',
            'client_certificate': 'DIRBS_KAFKA_CLIENT_CERT',
            'client_key': 'DIRBS_KAFKA_CLIENT_KEY',
            'caroot_certificate': 'DIRBS_KAFKA_CAROOT_CERT'
        }


class BrokerOperatorConfig(ConfigSection):
    """Class representing each operator in the operators section of the broker config."""

    COUNTRY_OPERATOR_NAME = '__all__'

    def __init__(self, **operator_config):
        """Constructor which parses the operator config."""
        super(BrokerOperatorConfig, self).__init__(**operator_config)
        self.id = self._parse_string('id', max_len=16)

        if self.id != self.id.lower():
            _logger.warning('operator_id: {0} has been changed to '
                            'lower case: {1}'.format(self.id, self.id.lower()))
            self.id = self.id.lower()

        # Check that operator_ids contains only letters, underscores and digits(0-9)
        bad_symbol_error_message = 'Operator_id {0} must contain only letters, underscores or digits(0-9)!'
        parse_alphanum(self.id, bad_symbol_error_message)

        self.name = self._parse_string('name')
        if self.id == self.COUNTRY_OPERATOR_NAME:
            msg = "Invalid use of reserved operator name \'__all__\' in config!"
            _logger.error(msg)
            raise ConfigParseException(msg)
        self.topic = self._parse_string('topic')

    @property
    def section_name(self):
        """Property for the section name."""
        return 'BrokerOperatorConfig'

    @property
    def valid_keys(self):
        """Property describing valid config keys."""
        return list(self.defaults.keys()) + ['id', 'name', 'topic']

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {}

    def as_dict(self):
        """Method to turn this config into a dict for serialization purposes."""
        return {
            'operator_id': self.id,
            'operator_name': self.name,
            'topic': self.topic
        }
