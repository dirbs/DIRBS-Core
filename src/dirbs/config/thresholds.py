"""
DIRBS Core thresholds configuration section parser.

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

from dirbs.config.common import ConfigSection, ConfigParseException, _logger


class OperatorThresholdConfig(ConfigSection):
    """Class representing the configuration of thresholds used for validating operator data."""

    def __init__(self, **op_threshold_config):
        """Constructor which parses the threshold config for importing operator data."""
        super(OperatorThresholdConfig, self).__init__(**op_threshold_config)
        self.null_imei_threshold = self._parse_float_ratio('null_imei_threshold')
        self.null_imsi_threshold = self._parse_float_ratio('null_imsi_threshold')
        self.null_msisdn_threshold = self._parse_float_ratio('null_msisdn_threshold')
        self.null_rat_threshold = self._parse_float_ratio('null_rat_threshold')
        self.null_threshold = self._parse_float_ratio('null_threshold')
        self.unclean_imei_threshold = self._parse_float_ratio('unclean_imei_threshold')
        self.unclean_imsi_threshold = self._parse_float_ratio('unclean_imsi_threshold')
        self.unclean_threshold = self._parse_float_ratio('unclean_threshold')
        self.out_of_region_imsi_threshold = self._parse_float_ratio('out_of_region_imsi_threshold')
        self.out_of_region_msisdn_threshold = self._parse_float_ratio('out_of_region_msisdn_threshold')
        self.out_of_region_threshold = self._parse_float_ratio('out_of_region_threshold')
        self.non_home_network_threshold = self._parse_float_ratio('non_home_network_threshold')
        self.historic_imei_threshold = self._parse_float_ratio('historic_imei_threshold')
        self.historic_imsi_threshold = self._parse_float_ratio('historic_imsi_threshold')
        self.historic_msisdn_threshold = self._parse_float_ratio('historic_msisdn_threshold')
        self.leading_zero_suspect_limit = self._parse_float_ratio('leading_zero_suspect_limit')

    @property
    def section_name(self):
        """Property for the section name."""
        return 'OperatorThresholdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'null_imei_threshold': 0.05,
            'null_imsi_threshold': 0.05,
            'null_msisdn_threshold': 0.05,
            'null_rat_threshold': 0.05,
            'null_threshold': 0.05,
            'unclean_imei_threshold': 0.05,
            'unclean_imsi_threshold': 0.05,
            'unclean_threshold': 0.05,
            'out_of_region_imsi_threshold': 0.1,
            'out_of_region_msisdn_threshold': 0.1,
            'out_of_region_threshold': 0.1,
            'non_home_network_threshold': 0.2,
            'historic_imei_threshold': 0.9,
            'historic_imsi_threshold': 0.9,
            'historic_msisdn_threshold': 0.9,
            'leading_zero_suspect_limit': 0.5
        }


class BaseThresholdConfig(ConfigSection):
    """Class representing the configuration of thresholds used for validating operator data."""

    def __init__(self, **base_threshold_config):
        """Constructor which parses the threshold config for base import data."""
        super(BaseThresholdConfig, self).__init__(**base_threshold_config)
        self.import_size_variation_percent = self._parse_float_ratio('import_size_variation_percent')
        self.import_size_variation_absolute = self._parse_int_or_neg_one('import_size_variation_absolute')

    @property
    def section_name(self):
        """Property for the section name."""
        return 'BaseThresholdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'import_size_variation_percent': 0.95,
            'import_size_variation_absolute': 1000
        }

    def _parse_int_or_neg_one(self, propname):
        """Helper function to parse an integer value or special value neg one and bound-check it."""
        try:
            self._check_for_missing_propname(propname)
            parsed_val = int(self.raw_config[propname])
            # -1 is a special value for _import_size_variation_absolute variable.
            # If _import_size_variation_absolute is a positive integer (zero allowed), it will
            # check that specified absolute rows are bigger than the existing row count.
            # By setting this variable to neg one, this check will be disabled.
            if parsed_val == -1:
                return parsed_val
            else:
                # _parse_positive_int allows zero values for propname by default
                return self._parse_positive_int(propname)

        except ValueError:
            msg = '{0}: {1} value must be a positive integer or special ' \
                  'value -1'.format(self.section_name, propname)
            _logger.error(msg)
            raise ConfigParseException(msg)


class GSMAThresholdConfig(BaseThresholdConfig):
    """Class representing the configuration of thresholds used for validating operator data."""

    def __init__(self, **gsma_threshold_config):
        """Constructor which parses the threshold config for GSMA import data."""
        super(GSMAThresholdConfig, self).__init__(**gsma_threshold_config)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'GSMAThresholdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'import_size_variation_percent': 0,
            'import_size_variation_absolute': 100
        }


class PairingListThresholdConfig(BaseThresholdConfig):
    """Class representing the configuration of thresholds used for validating operator data."""

    def __init__(self, **pairing_threshold_config):
        """Constructor which parses the threshold config for pairing list import data."""
        super(PairingListThresholdConfig, self).__init__(**pairing_threshold_config)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'PairingListThresholdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'import_size_variation_percent': 0.95,
            'import_size_variation_absolute': 1000
        }


class SubscribersListThresholdConfig(BaseThresholdConfig):
    """Class representing the configuration of thresholds used for validating subscribers registration data."""

    def __init__(self, **subscribers_list_threshold_config):
        """Constructor which parses the threshold config for pairing list import data."""
        super(SubscribersListThresholdConfig, self).__init__(**subscribers_list_threshold_config)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'SubscribersListThresholdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'import_size_variation_percent': 0.95,
            'import_size_variation_absolute': 1000
        }


class DeviceAssociationListThresholdConfig(BaseThresholdConfig):
    """Class representing the configuration of thresholds used for validating device association list data."""

    def __init__(self, **association_list_threshold_config):
        """Constructor which parses the threshold config for pairing list import data."""
        super(DeviceAssociationListThresholdConfig, self).__init__(**association_list_threshold_config)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'AssociationListThresholdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'import_size_variation_percent': 0.75,
            'import_size_variation_absolute': -1
        }


class StolenListThresholdConfig(BaseThresholdConfig):
    """Class representing the configuration of thresholds used for validating operator data."""

    def __init__(self, **stolen_threshold_config):
        """Constructor which parses the threshold config for stolen list import data."""
        super(StolenListThresholdConfig, self).__init__(**stolen_threshold_config)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'StolenListThresholdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'import_size_variation_percent': 0.75,
            'import_size_variation_absolute': -1
        }


class GoldenListThresholdConfig(BaseThresholdConfig):
    """Class representing the configuration of thresholds used for validating operator data."""

    def __init__(self, **golden_threshold_config):
        """Constructor which parses the threshold config for golden list import data."""
        super(GoldenListThresholdConfig, self).__init__(**golden_threshold_config)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'GoldenListThresholdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'import_size_variation_percent': 0.75,
            'import_size_variation_absolute': -1
        }


class BarredListThresholdConfig(BaseThresholdConfig):
    """Class representing the configuration of thresholds used for validating barred list data."""

    def __init__(self, **barred_threshold_config):
        """Constructor which parses the threshold config for barred list import data."""
        super(BarredListThresholdConfig, self).__init__(**barred_threshold_config)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'BarredListThresholdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'import_size_variation_percent': 0.75,
            'import_size_variation_absolute': -1
        }


class MonitoringListThresholdConfig(BaseThresholdConfig):
    """Class representing the configuration of thresholds used for validating monitoring list data."""

    def __init__(self, **monitoring_threshold_config):
        """Constructor which parses the threshold config for barred list import data."""
        super(MonitoringListThresholdConfig, self).__init__(**monitoring_threshold_config)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'MonitoringListThresholdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'import_size_variation_percent': 0.75,
            'import_size_variation_absolute': -1
        }


class BarredTacListThresholdConfig(BaseThresholdConfig):
    """Class representing the configuration of thresholds used for validating barred tac list data."""

    def __init__(self, **barred_tac_threshold_config):
        """Constructor which parses the threshold config for barred tac list import data."""
        super(BarredTacListThresholdConfig, self).__init__(**barred_tac_threshold_config)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'BarredTacListThresholdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'import_size_variation_percent': 0.75,
            'import_size_variation_absolute': -1
        }


class RegistrationListThresholdConfig(BaseThresholdConfig):
    """Class representing the configuration of thresholds used for validating operator data."""

    def __init__(self, **import_threshold_config):
        """Constructor which parses the threshold config for registration list import data."""
        super(RegistrationListThresholdConfig, self).__init__(**import_threshold_config)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'RegistrationListThresholdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'import_size_variation_percent': 0.75,
            'import_size_variation_absolute': -1
        }
