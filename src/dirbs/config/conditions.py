"""
DIRBS Core conditions configuration section parser.

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

import importlib

from dirbs.config.common import ConfigSection, parse_alphanum, ConfigParseException, _logger


class ConditionConfig(ConfigSection):
    """Class representing the configuration for a classification condition."""

    def __init__(self, *, ignore_env, **cond_config):
        """Constructor which parses the condition config."""
        super(ConditionConfig, self).__init__(ignore_env=ignore_env, **cond_config)
        self.label = self._parse_string('label', max_len=64)

        # Check that condition name contains only letters, underscores and digits(0-9)
        bad_symbol_error_message = 'Condition label {0} must contain only letters, underscores or digits(0-9)!'
        parse_alphanum(self.label.lower(), bad_symbol_error_message)

        self.grace_period = self._parse_positive_int('grace_period_days')
        self.blocking = self._parse_bool('blocking')
        self.sticky = self._parse_bool('sticky')
        self.reason = self._parse_string('reason')
        self.max_allowed_matching_ratio = self._parse_float_ratio('max_allowed_matching_ratio')
        self.amnesty_eligible = self._parse_bool('amnesty_eligible')
        if self.reason.find('|') != -1:
            msg = 'Illegal pipe character in reason string for condition: {0}'.format(self.label)
            _logger.error(msg)
            raise ConfigParseException(msg)

        dimensions = self.raw_config['dimensions']
        if not isinstance(dimensions, list):
            msg = 'Dimensions not a list type!'
            _logger.error('{0}: {1}'.format(self.section_name, msg))
            raise ConfigParseException(msg)
        self.dimensions = [DimensionConfig(ignore_env=ignore_env, **d) for d in dimensions]

        if self.amnesty_eligible and not self.blocking:
            msg = 'Informational conditions cannot have amnesty_eligible flag set to True.'
            _logger.error('{0}: {1}'.format(self.section_name, msg))
            raise ConfigParseException(msg)

    def as_dict(self):
        """Method to turn this config into a dict for serialization purposes."""
        rv = self.raw_config
        rv['dimensions'] = [d.raw_config for d in self.dimensions]
        return rv

    @property
    def section_name(self):
        """Property for the section name."""
        return 'ConditionConfig'

    @property
    def valid_keys(self):
        """Property describing valid config keys."""
        return list(self.defaults.keys()) + ['label']

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'dimensions': [],
            'grace_period_days': 30,
            'blocking': False,
            'sticky': False,
            'reason': None,
            'max_allowed_matching_ratio': 0.1,
            'amnesty_eligible': False
        }


class DimensionConfig(ConfigSection):
    """Class representing the configuration for an individual classification dimension."""

    def __init__(self, **dim_config):
        """Constructor which parses the dimension config."""
        if 'module' not in dim_config:
            msg = 'No module specified!'
            _logger.error('DimensionConfig: {0}'.format(msg))
            raise ConfigParseException(msg)
        self.module = dim_config['module']

        super(DimensionConfig, self).__init__(**dim_config)

        try:
            module = self.raw_config['module']
            mod = importlib.import_module('dirbs.dimensions.' + module)
        except ImportError as ex:
            _logger.error(str(ex))
            msg = '{0}: module {1} can not be imported'.format(self.section_name, module)
            _logger.error('{0}'.format(msg))
            raise ConfigParseException(msg)

        dim_constructor = mod.__dict__.get('dimension')
        try:
            params = self.raw_config['parameters']
            dim_constructor(**params)
            self.params = params

        except Exception as e:
            msg_error = "Could not create dimension \'{0}\' with supplied parameters".format(self.module)
            msg = '{0}: {1}. Cause: {2}'.format(self.section_name, msg_error, str(e))
            _logger.error(msg)
            raise ConfigParseException(msg)

        self.invert = self._parse_bool('invert')

    @property
    def section_name(self):
        """Property for the section name."""
        return 'DimensionConfig'

    @property
    def valid_keys(self):
        """Property describing valid config keys."""
        return list(self.defaults.keys()) + ['module']

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'parameters': {},
            'invert': False
        }
