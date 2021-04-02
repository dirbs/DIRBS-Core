"""
DIRBS Core configuration file parser package.

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
import logging

import yaml

from dirbs.config.common import ConfigParseException, check_for_duplicates, check_redis_status
from dirbs.config.db import DBConfig
from dirbs.config.region import RegionConfig
from dirbs.config.dirbs_logging import LoggingConfig
from dirbs.config.importer import ImporterConfig
from dirbs.config.conditions import ConditionConfig
from dirbs.config.retention import RetentionConfig
from dirbs.config.list_generation import ListGenerationConfig
from dirbs.config.report_generation import ReportGenerationConfig
from dirbs.config.multiprocessing import MultiprocessingConfig
from dirbs.config.statsd import StatsdConfig
from dirbs.config.redis import RedisConfig
from dirbs.config.catalog import CatalogConfig
from dirbs.config.amnesty import AmnestyConfig
from dirbs.config.broker import BrokerConfig
from dirbs.config.operational import OperationalConfig
from dirbs.config.thresholds import OperatorThresholdConfig, GSMAThresholdConfig, PairingListThresholdConfig, \
    SubscribersListThresholdConfig, StolenListThresholdConfig, RegistrationListThresholdConfig, \
    GoldenListThresholdConfig, BarredListThresholdConfig, BarredTacListThresholdConfig, \
    DeviceAssociationListThresholdConfig, MonitoringListThresholdConfig

_DEFAULT_SEARCH_PATHS = [os.path.expanduser('~/.dirbs.yml'), '/opt/dirbs/etc/config.yml']
_logger = logging.getLogger('dirbs.config')


class ConfigParser:
    """Class to parse the DIRBS YAML config and parses it into Python config object."""

    def parse_config(self, *, ignore_env, config_paths=None):
        """Helper method to parse the config file from disk."""
        if config_paths is None:
            env_config_file = os.environ.get('DIRBS_CONFIG_FILE', None)
            if env_config_file is not None:
                config_paths = [env_config_file]
            else:
                config_paths = _DEFAULT_SEARCH_PATHS  # pragma: no cover

        for p in config_paths:
            _logger.debug('Looking for DIRBS config file in {0}...'.format(p))
            try:
                cfg = yaml.safe_load(open(p))
                if cfg is None:
                    _logger.error('Invalid DIRBS Config file found at {0}!'.format(p))
                    raise ConfigParseException('Invalid DIRBS Config file found at {0}'.format(p))
                _logger.debug('Successfully parsed {0} as YAML...'.format(p))
                return AppConfig(ignore_env=ignore_env, **cfg)
            except yaml.YAMLError as ex:
                _logger.error('Invalid DIRBS Config file found at {0}!'.format(p))
                msg = str(ex)
                _logger.error(str(ex))
                raise ConfigParseException(msg)
            except IOError:
                _logger.debug('{0} did not exist, skipping...'.format(p))
                continue

        msg = 'Missing config file - please create a config file for DIRBS'
        _logger.error(msg)
        raise ConfigParseException(msg)


class AppConfig:
    """DIRBS root application config object."""

    def __init__(self, *, ignore_env, **yaml_config):
        """Constructor performing common section parsing for config sections."""
        self.db_config = DBConfig(ignore_env=ignore_env, **(yaml_config.get('postgresql', {}) or {}))
        self.region_config = RegionConfig(ignore_env=ignore_env, **(yaml_config.get('region', {}) or {}))
        self.log_config = LoggingConfig(ignore_env=ignore_env, **(yaml_config.get('logging', {}) or {}))
        self.import_config = ImporterConfig(ignore_env=ignore_env, **(yaml_config.get('import', {}) or {}))
        self.conditions = [ConditionConfig(ignore_env=ignore_env, **c) for c in yaml_config.get('conditions', [])]
        cond_names = [c.label.lower() for c in self.conditions]

        # Check that condition names are unique and case-insensitive
        dupl_cond_name_found_error_message = 'Duplicate condition names {0} found in config. ' \
                                             'Condition names are case insensitive!'
        check_for_duplicates(cond_names, dupl_cond_name_found_error_message)

        self.operator_threshold_config = OperatorThresholdConfig(ignore_env=ignore_env,
                                                                 **(yaml_config.get('operator_threshold', {}) or {}))
        self.gsma_threshold_config = GSMAThresholdConfig(ignore_env=ignore_env,
                                                         **(yaml_config.get('gsma_threshold', {}) or {}))
        self.pairing_threshold_config = PairingListThresholdConfig(ignore_env=ignore_env,
                                                                   **(yaml_config.get('pairing_list_threshold',
                                                                                      {}) or {}))
        self.subscribers_threshold_config = SubscribersListThresholdConfig(ignore_env=ignore_env,
                                                                           **(yaml_config.get(
                                                                               'subscribers_list_threshold',
                                                                               {}) or {}))
        self.stolen_threshold_config = StolenListThresholdConfig(ignore_env=ignore_env,
                                                                 **(yaml_config.get('stolen_list_threshold',
                                                                                    {}) or {}))
        self.import_threshold_config = \
            RegistrationListThresholdConfig(ignore_env=ignore_env,
                                            **(yaml_config.get('registration_list_threshold', {}) or {}))
        self.golden_threshold_config = GoldenListThresholdConfig(ignore_env=ignore_env,
                                                                 **(yaml_config.get('golden_list_threshold',
                                                                                    {}) or {}))
        self.barred_threshold_config = BarredListThresholdConfig(ignore_env=ignore_env,
                                                                 **(yaml_config.get('barred_list_threshold',
                                                                                    {}) or {}))
        self.monitoring_threshold_config = MonitoringListThresholdConfig(ignore_env=ignore_env,
                                                                         **(yaml_config.get(
                                                                             'monitoring_list_threshold',
                                                                            {}) or {}))
        self.barred_tac_threshold_config = BarredTacListThresholdConfig(ignore_env=ignore_env,
                                                                        **(yaml_config.get('barred_tac_list_threshold',
                                                                                           {}) or {}))
        self.associations_threshold_config = DeviceAssociationListThresholdConfig(ignore_env=ignore_env,
                                                                                  **(yaml_config.get(
                                                                                      'association_list_threshold',
                                                                                      {}) or {}))
        self.retention_config = RetentionConfig(ignore_env=ignore_env, **(yaml_config.get('data_retention', {}) or {}))
        self.listgen_config = ListGenerationConfig(ignore_env=ignore_env,
                                                   **(yaml_config.get('list_generation', {}) or {}))
        self.report_config = ReportGenerationConfig(ignore_env=ignore_env,
                                                    **(yaml_config.get('report_generation', {}) or {}))
        self.multiprocessing_config = MultiprocessingConfig(ignore_env=ignore_env,
                                                            **(yaml_config.get('multiprocessing', {}) or {}))
        self.statsd_config = StatsdConfig(ignore_env=ignore_env, **(yaml_config.get('statsd', {}) or {}))
        self.catalog_config = CatalogConfig(ignore_env=ignore_env, **(yaml_config.get('catalog', {}) or {}))
        self.amnesty_config = AmnestyConfig(ignore_env=ignore_env, **(yaml_config.get('amnesty', {}) or {}))
        self.operational_config = OperationalConfig(ignore_env=ignore_env,
                                                    **(yaml_config.get('operational', {}) or {}))

        self.broker_config = BrokerConfig(
            ignore_env=ignore_env,
            **(yaml_config.get('broker', {}) or {})) if self.operational_config.activate_whitelist else None

        self.redis_config = RedisConfig(ignore_env=ignore_env, **(yaml_config.get('redis', {}) or {}))
        check_redis_status(redis_config=self.redis_config)
