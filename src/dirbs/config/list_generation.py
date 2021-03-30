"""
DIRBS Core list-gen configuration section parser.

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

from dirbs.config.common import ConfigSection


class ListGenerationConfig(ConfigSection):
    """Class representing the configuration of the lookback window used in the list generation process."""

    def __init__(self, **listgen_config):
        """Constructor which parses the list generation config."""
        super(ListGenerationConfig, self).__init__(**listgen_config)
        self.lookback_days = self._parse_positive_int('lookback_days')
        self.restrict_exceptions_list = self._parse_bool('restrict_exceptions_list_to_blacklisted_imeis')
        self.generate_check_digit = self._parse_bool('generate_check_digit')
        self.output_invalid_imeis = self._parse_bool('output_invalid_imeis')
        self.include_barred_imeis = self._parse_bool('include_barred_imeis_in_exceptions_list')
        self.notify_imsi_change = self._parse_bool('notify_imsi_change')

    @property
    def section_name(self):
        """Property for the section name."""
        return 'ListGenerationConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'lookback_days': 60,
            'restrict_exceptions_list_to_blacklisted_imeis': False,
            'generate_check_digit': False,
            'output_invalid_imeis': True,
            'include_barred_imeis_in_exceptions_list': False,
            'notify_imsi_change': True
        }
