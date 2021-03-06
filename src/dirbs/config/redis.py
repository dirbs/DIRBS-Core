"""
DIRBS Core redis configuration section parser.

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
  If you use this software in a product, an acknowledgment is required by displaying the trademark/log as per the
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

from dirbs.config.common import ConfigSection


class RedisConfig(ConfigSection):
    """Class representing the configuration of the redis server."""

    def __init__(self, **redis_config):
        """Constructor which parses the redis config."""
        super(RedisConfig, self).__init__(**redis_config)
        self.hostname = self._parse_string('hostname')
        self.port = self._parse_positive_int('port')
        self.password = self._parse_string('password', optional=True)
        self.db = self._parse_string('db', optional=True)
        self.cache_timeout = self._parse_positive_int('cache_timeout')

    @property
    def section_name(self):
        """Property for the section name."""
        return 'RedisConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'hostname': 'localhost',
            'port': 6379,
            'db': '0',
            'password': '',
            'cache_timeout': 300
        }

    @property
    def env_overrides(self):
        """Property describing a key->envvar mapping for overriding config valies."""
        return {
            'hostname': 'DIRBS_REDIS_HOST',
            'port': 'DIRBS_REDIS_PORT',
            'password': 'DIRBS_REDIS_PASSWORD',
            'db': 'DIRBS_REDIS_DB',
            'cache_timeout': 'DIRBS_REDIS_TIMEOUT'
        }
