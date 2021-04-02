"""
DIRBS Core configuration file parser commons.

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

import os
import re
import collections
import logging
from datetime import datetime
from pathlib import Path

from redis import Redis, ConnectionError


_logger = logging.getLogger('dirbs.config')


class ConfigParseException(Exception):
    """Indicates that there was an exception encountered when parsing the DIRBS config file."""

    pass


def parse_alphanum(string, bad_symbol_error_message):
    """Check that string contains only letters, underscores and digits(0-9)."""
    if not re.match(r'^\w*$', string):
        msg = bad_symbol_error_message.format(string)
        _logger.error(msg)
        raise ConfigParseException(msg)


def check_for_duplicates(input_list, duplicates_found_error_message):
    """Check that input_list elems are unique."""
    dupe_names = [name for name, count in collections.Counter(input_list).items() if count > 1]
    duplicates_found_error_message = duplicates_found_error_message.format(', '.join(dupe_names))
    if len(dupe_names) > 0:
        _logger.error(duplicates_found_error_message)
        raise ConfigParseException(duplicates_found_error_message)


def check_redis_status(redis_config):
    """Helper method that checks if the redis server is up and running."""
    server = Redis(host=redis_config.hostname, socket_timeout=1)

    try:
        server.ping()
    except ConnectionError:
        _logger.error("Can\'t connect to Redis server. Check if the server is up and running...")


class ConfigSection:
    """Base config section class."""

    def __init__(self, *, ignore_env, **config):
        """Constructor performing common section parsing for config sections."""
        invalid_keys = set(config.keys()) - set(self.valid_keys)
        environment_config = {} \
            if ignore_env else {k: os.environ.get(v) for k, v in self.env_overrides.items() if v in os.environ}
        for k in invalid_keys:
            _logger.warning('{0}: Ignore invalid setting {1}={2}'.format(self.section_name, k, config[k]))
            del config[k]
        self.raw_config = {**self.defaults, **config, **environment_config}

    @property
    def section_name(self):
        """Property for the section name."""
        raise NotImplementedError()

    @property
    def defaults(self):  # pragma: no cover
        """Property describing defaults for config values."""
        raise NotImplementedError()

    @property
    def env_overrides(self):  # pragma: no cover
        """Property describing a key->envvar mapping for overriding config valies."""
        return {}

    @property
    def valid_keys(self):
        """Property describing valid config keys."""
        return list(self.defaults.keys())

    def _check_for_missing_propname(self, propname):
        """Check if property exists and throw an error if not."""
        if propname not in self.raw_config:
            msg = '{0}: Missing attribute {1} in config!'.format(self.section_name, propname)
            _logger.error(msg)
            raise ConfigParseException(msg)

    def _parse_positive_int(self, propname, allow_zero=True):
        """Helper function to parse an integer value and bound-check it."""
        try:
            self._check_for_missing_propname(propname)
            parsed_val = int(self.raw_config[propname])
            if allow_zero:
                if parsed_val < 0:
                    msg = '{0}: {1} value "{2}" must be greater than or equal to 0'\
                          .format(self.section_name, propname, parsed_val)
                    _logger.error(msg)
                    raise ConfigParseException(msg)
            elif parsed_val <= 0:
                msg = '{0}: {1} value "{2}" must be greater than 0'.format(self.section_name, propname, parsed_val)
                _logger.error(msg)
                raise ConfigParseException(msg)

            return parsed_val
        except ValueError:
            msg = '{0}: {1} value "{2}" must be an integer value'\
                  .format(self.section_name, propname, self.raw_config[propname])
            _logger.error((msg))
            raise ConfigParseException(msg)

    def _parse_file_path(self, propname, ext=None):
        """Helper function to parse a valid file path. If ext is defined then check for the suffix as well."""
        try:
            self._check_for_missing_propname(propname)
            self._parse_string(propname)
            file_path = Path(self.raw_config[propname])
            resolved_path = file_path.resolve(strict=True)

            if ext is not None:
                if resolved_path.suffix != ext:
                    msg = 'Invalid file type or extension, required is {0}'.format(ext)
                    _logger.error(msg)
                    raise ConfigParseException(msg)
            return resolved_path
        except FileNotFoundError:
            msg = '{0}:{1} value is not valid, either file does not exists or permission issue'\
                .format(self.section_name, propname)
            _logger.error(msg)
            raise ConfigParseException(msg)

    def _parse_float_ratio(self, propname):
        """Helper function to parse a ration between 0 <= value <= 1."""
        try:
            self._check_for_missing_propname(propname)
            parsed_val = float(self.raw_config[propname])
            if parsed_val < 0 or parsed_val > 1:
                msg = '{0}: {1} value "{2}" not between 0 and 1!'\
                      .format(self.section_name, propname, parsed_val)
                _logger.error(msg)
                raise ConfigParseException(msg)

            return parsed_val
        except ValueError:
            msg = '{0}: {1} value "{2}" is non-numeric!'\
                  .format(self.section_name, propname, self.raw_config[propname])
            _logger.error(msg)
            raise ConfigParseException(msg)

    def _parse_string(self, propname, max_len=None, optional=False):
        """Helper function to parse a string."""
        self._check_for_missing_propname(propname)
        val = self.raw_config[propname]
        if val is None:
            if not optional:
                msg = '{0}: {1} value is None!'.format(self.section_name, propname)
                _logger.error(msg)
                raise ConfigParseException(msg)
            else:
                return val

        val = str(val)
        if max_len is not None and len(val) > max_len:
            msg = '{0}: {1} value "{2}" is limited to {3:d} characters and has {4:d}!'\
                  .format(self.section_name, propname, val, max_len, len(val))
            _logger.error(msg)
            raise ConfigParseException(msg)
        return val

    def _parse_bool(self, propname):
        """Helper function to parse a bool."""
        self._check_for_missing_propname(propname)
        val = self.raw_config[propname]
        if val not in [True, False]:
            msg = '{0}: {1} value "{2}" is not a valid boolean value!'\
                  .format(self.section_name, propname, val)
            _logger.error(msg)
            raise ConfigParseException(msg)
        return val

    def _parse_date(self, propname, date_format, pretty_date_format):
        """Helper function to parse a date."""
        self._check_for_missing_propname(propname)
        val = self.raw_config[propname]
        try:
            return datetime.strptime(str(val), date_format).date()
        except ValueError:
            msg = '{0}: {1} value "{2}" is not a valid date. Date must be in \'{3}\' format.' \
                .format(self.section_name, propname, val, pretty_date_format)
            _logger.error(msg)
            raise ConfigParseException(msg)
