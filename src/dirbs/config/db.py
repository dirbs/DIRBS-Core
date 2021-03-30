"""
DIRBS Core database configuration section parser.

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

import codecs

from dirbs.config.common import ConfigSection


class DBConfig(ConfigSection):
    """Class representing the 'postgresql' section of the config."""

    def __init__(self, **db_config):
        """Constructor which parses the database config."""
        super(DBConfig, self).__init__(**db_config)
        self.database = self._parse_string('database')
        self.host = self._parse_string('host')
        self.user = self._parse_string('user')
        self.password = self._parse_string('password', optional=True)
        self.port = self._parse_positive_int('port')

    @property
    def section_name(self):
        """Property for the section name."""
        return 'PGConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'database': 'XXXXXXXX',
            'host': 'localhost',
            'port': 5432,
            'user': 'XXXXXXXX',
            'password': None
        }

    @property
    def env_overrides(self):
        """Property describing a key->envvar mapping for overriding config valies."""
        return {
            'database': 'DIRBS_DB_DATABASE',
            'host': 'DIRBS_DB_HOST',
            'port': 'DIRBS_DB_PORT',
            'user': 'DIRBS_DB_USER',
            'password': 'DIRBS_DB_PASSWORD'
        }

    @property
    def connection_string(self):
        """Connection string for PostgreSQL."""
        key_map = {
            'database': 'dbname'
        }
        valid_props = [
            '{0}={1}'.format(key_map.get(prop, prop), getattr(self, prop))
            for prop in ['database', 'user', 'host', 'port', 'password']
            if getattr(self, prop) is not None
        ]
        return ' '.join(valid_props)

    @property
    def password(self):
        """Property getter for password to unobfuscate the password whilst in memory."""
        if self._password is None:
            return None
        else:
            return codecs.decode(self._password, 'rot-13')

    @password.setter
    def password(self, value):
        """Property setter for password to obfuscate the password whilst in memory."""
        if value is None:
            self._password = None
        else:
            self._password = codecs.encode(value, 'rot-13')
