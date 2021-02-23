"""
DIRBS Core logging configuration section parser.

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

from dirbs.logging import DEFAULT_FORMAT
from dirbs.config.common import ConfigSection


class LoggingConfig(ConfigSection):
    """Class representing the 'logging' section of the config."""

    def __init__(self, **log_config):
        """Constructor which parses the logging config."""
        super(LoggingConfig, self).__init__(**log_config)
        self.level = self._parse_string('level')
        self.format = self._parse_string('format')
        self.show_statsd_messages = self._parse_bool('show_statsd_messages')
        self.show_sql_messages = self._parse_bool('show_sql_messages')
        self.show_werkzeug_messages = self._parse_bool('show_werkzeug_messages')
        self.enable_scrubbing = self._parse_bool('enable_scrubbing')
        self.log_directory = self._parse_string('log_directory', optional=True)
        self.file_prefix = self._parse_string('file_prefix', optional=True)
        self.file_rotation_backup_count = self._parse_positive_int('file_rotation_backup_count', allow_zero=True)
        self.file_rotation_max_bytes = self._parse_positive_int('file_rotation_max_bytes', allow_zero=True)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'LoggingConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'level': 'info',
            'format': DEFAULT_FORMAT,
            'show_statsd_messages': False,
            'show_sql_messages': False,
            'show_werkzeug_messages': False,
            'enable_scrubbing': False,
            'log_directory': None,
            'file_prefix': None,
            'file_rotation_backup_count': 0,
            'file_rotation_max_bytes': 0
        }
