"""
DIRBS Core region configuration section parser.

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

from dirbs.config.common import ConfigSection, ConfigParseException, check_for_duplicates, parse_alphanum, _logger


class RegionConfig(ConfigSection):
    """Class representing the 'region' section of the config."""

    def __init__(self, *, ignore_env, **region_config):
        """Constructor which parses the region config."""
        super(RegionConfig, self).__init__(ignore_env=ignore_env, **region_config)
        self.name = self._parse_string('name')
        self.import_msisdn_data = self._parse_bool('import_msisdn_data')
        self.import_rat_data = self._parse_bool('import_rat_data')

        # Check that country codes are strings that can be converted to ints
        try:
            [int(x) for x in self.raw_config['country_codes']]
        except ValueError:
            msg = '{0}: non-numeric value for country code!'.format(self.section_name)
            _logger.error(msg)
            raise ConfigParseException(msg)

        # Make sure we store country codes as strings
        self.country_codes = [str(x) for x in self.raw_config['country_codes']]

        if self.country_codes is None or len(self.country_codes) <= 0:
            msg = 'Country Code must be provided for "region" section in config'
            _logger.error(msg)
            raise ConfigParseException(msg)

        # Populate operators array
        self.operators = [OperatorConfig(ignore_env=ignore_env, **o) for o in region_config.get('operators', [])]

        # Check that operator_ids are unique and case-insensitive
        dupl_op_id_found_error_message = 'Duplicate operator_ids {0} found in config. ' \
                                         'Operator_ids are case insensitive!'
        operator_id_list = [o.id for o in self.operators]
        check_for_duplicates(operator_id_list, dupl_op_id_found_error_message)

        # Parse exempted device types if present
        self.exempted_device_types = [str(x) for x in self.raw_config.get('exempted_device_types', [])]

        # Check the mcc_mnc pairs are unique and that no mcc-mnc can begin with another mcc-mnc
        dupl_mcc_mnc_found_error_message = 'Duplicate MCC-MNC pairs {0} found in config. ' \
                                           'MCC-MNC pairs must be unique across all operators!'
        all_mncs = [p['mcc'] + p['mnc'] for o in self.operators for p in o.mcc_mnc_pairs]
        check_for_duplicates(all_mncs, dupl_mcc_mnc_found_error_message)
        all_mncs_set = set(all_mncs)
        substring_mcc_mnc_error_message = 'MCC-MNC pair {0} found which starts with another configured MCC-MNC pair ' \
                                          '{1}. MCC-MNC pairs must be disjoint from each other (not be prefixed by ' \
                                          'another MCC-MNC)!'
        for mcc_mnc in all_mncs_set:
            mcc_mncs_to_check = all_mncs_set.copy()
            mcc_mncs_to_check.remove(mcc_mnc)
            for other_mcc_mnc in mcc_mncs_to_check:
                if mcc_mnc.startswith(other_mcc_mnc):
                    err_msg = substring_mcc_mnc_error_message.format(mcc_mnc, other_mcc_mnc)
                    _logger.error(err_msg)
                    raise ConfigParseException(err_msg)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'RegionConfig'

    @property
    def valid_keys(self):
        """Property describing valid config keys."""
        return list(self.defaults.keys()) + ['name']

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'country_codes': [],
            'operators': [],
            'import_msisdn_data': True,
            'import_rat_data': True,
            'exempted_device_types': []
        }


class OperatorConfig(ConfigSection):
    """Class representing each operator in the 'operators' subsection of the region config."""

    COUNTRY_OPERATOR_NAME = '__all__'

    def __init__(self, **operator_config):
        """Constructor which parses the operator config."""
        super(OperatorConfig, self).__init__(**operator_config)
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

        # Make sure mcc_mnc key is there and is a list
        if 'mcc_mnc_pairs' not in operator_config or type(operator_config['mcc_mnc_pairs']) is not list:
            msg = 'Missing (or non-list) {0} in config for operator ID {1}!'.format('mcc_mnc_pairs', self.id)
            _logger.error(msg)
            raise ConfigParseException(msg)

        # Validate each MCC/MNC pair
        for mcc_mnc in self.raw_config['mcc_mnc_pairs']:
            for key in ['mcc', 'mnc']:
                try:
                    int(mcc_mnc[key])
                except (ValueError, KeyError):
                    msg = 'Non-existent or non integer {0} in config for operator ID {1}!'.format(key, self.id)
                    _logger.error(msg)
                    raise ConfigParseException(msg)

        # Make sure we stringify mcc and mnc values in case they were interpreted as ints by YAML parser
        self.mcc_mnc_pairs = \
            [{'mcc': str(x['mcc']), 'mnc': str(x['mnc'])} for x in self.raw_config['mcc_mnc_pairs']]

        if self.mcc_mnc_pairs is None or len(self.mcc_mnc_pairs) <= 0:
            msg = 'At least one valid MCC-MNC pair must be provided for operator ID {0}.'.format(self.id)
            _logger.error(msg)
            raise ConfigParseException(msg)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'OperatorConfig'

    @property
    def valid_keys(self):
        """Property describing valid config keys."""
        return list(self.defaults.keys()) + ['id', 'name']

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'mcc_mnc_pairs': []
        }

    def as_dict(self):
        """Method to turn this config into a dict for serialization purposes."""
        return {
            'operator_id': self.id,
            'operator_name': self.name,
            'mcc_mnc_pairs': self.mcc_mnc_pairs
        }
