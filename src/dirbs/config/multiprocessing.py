"""
DIRBS Core multi-procs configuration section parser.

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

import math
import multiprocessing

from dirbs.config.common import ConfigSection, ConfigParseException, _logger


class MultiprocessingConfig(ConfigSection):
    """Class representing the configuration of the number parallel workers to use, etc."""

    def __init__(self, **mp_config):
        """Constructor which parses the list generation config."""
        super(MultiprocessingConfig, self).__init__(**mp_config)
        self.max_local_cpus = self._parse_positive_int('max_local_cpus')
        self.max_db_connections = self._parse_positive_int('max_db_connections')

    @property
    def max_local_cpus(self):
        """Property detailing maximum number of local CPUs to use."""
        return self._max_local_cpus

    @property
    def max_db_connections(self):
        """Property detailing maximum number of DB connections to use."""
        return self._max_db_connections

    @max_local_cpus.setter
    def max_local_cpus(self, value):
        """Property setter for max_local_cpus."""
        max_cpus = max(multiprocessing.cpu_count() - 1, 1)
        if value < 1 or value > max_cpus:
            msg = 'max_local_cpus must be at least 1 and can not be set higher than CPUs present in the ' \
                  'system minus one!'
            _logger.error(msg)
            raise ConfigParseException(msg)
        self._max_local_cpus = value

    @max_db_connections.setter
    def max_db_connections(self, value):
        """Property setter for max_db_connections."""
        if value < 1 or value > 32:
            msg = 'max_db_connections must be at least 1 and can not be set higher than 32!'
            _logger.error(msg)
            raise ConfigParseException(msg)
        self._max_db_connections = value

    @property
    def section_name(self):
        """Property for the section name."""
        return 'MultiprocessingConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'max_local_cpus': math.ceil(multiprocessing.cpu_count() / 2),
            'max_db_connections': 4
        }
