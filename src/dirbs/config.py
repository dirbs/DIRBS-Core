"""
DIRBS configuration file parser.

Copyright (c) 2018 Qualcomm Technologies, Inc.

 All rights reserved.



 Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
 limitations in the disclaimer below) provided that the following conditions are met:


 * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 disclaimer.

 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 disclaimer in the documentation and/or other materials provided with the distribution.

 * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
 products derived from this software without specific prior written permission.

 NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
 THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 POSSIBILITY OF SUCH DAMAGE.
"""

import importlib
import logging
import collections
import os
import multiprocessing
import math
import re
import codecs
from datetime import datetime

import yaml


from dirbs.logging import DEFAULT_FORMAT


_DEFAULT_SEARCH_PATHS = [os.path.expanduser('~/.dirbs.yml'), '/opt/dirbs/etc/config.yml']
_logger = logging.getLogger('dirbs.config')


def parse_alphanum(string, bad_symbol_error_message):
    """Check that string contains only letters, underscores and digits(0-9)."""
    if not re.match('^\w*$', string):
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


class ConfigParseException(Exception):
    """Indicates that there was an exception encountered when parsing the DIRBS config file."""

    pass


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
                                                                   **(yaml_config.get('pairing_list_threshold', {}) or
                                                                      {}))
        self.stolen_threshold_config = StolenListThresholdConfig(ignore_env=ignore_env,
                                                                 **(yaml_config.get('stolen_list_threshold',
                                                                                    {}) or {}))
        self.import_threshold_config = \
            RegistrationListThresholdConfig(ignore_env=ignore_env,
                                            **(yaml_config.get('registration_list_threshold', {}) or {}))
        self.golden_threshold_config = GoldenListThresholdConfig(ignore_env=ignore_env,
                                                                 **(yaml_config.get('golden_list_threshold',
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
        except ValueError as ex:
            msg = '{0}: {1} value "{2}" must be an integer value'\
                  .format(self.section_name, propname, self.raw_config[propname])
            _logger.error((msg))
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
            msg = 'Invalid use of reserved operator name \'__all__\' in config!'
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


class ImporterConfig(ConfigSection):
    """Class representing the 'import' section of the config."""

    def __init__(self, **import_config):
        """Constructor which parses the importer config."""
        super(ImporterConfig, self).__init__(**import_config)
        self.batch_size = self._parse_positive_int('batch_size', allow_zero=False)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'ImporterConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'batch_size': 1000000,
        }


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
            msg_error = 'Could not create dimension \'{0}\' with supplied parameters'.format(self.module)
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
            'historic_msisdn_threshold': 0.9
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


class RetentionConfig(ConfigSection):
    """Class representing the configuration of the data retention window used to determine what data to prune."""

    def __init__(self, **retention_config):
        """Constructor which parses the data retention config."""
        super(RetentionConfig, self).__init__(**retention_config)
        self.months_retention = self._parse_positive_int('months_retention')

    @property
    def section_name(self):
        """Property for the section name."""
        return 'RetentionConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'months_retention': 3,
        }


class ListGenerationConfig(ConfigSection):
    """Class representing the configuration of the lookback window used in the list generation process."""

    def __init__(self, **listgen_config):
        """Constructor which parses the list generation config."""
        super(ListGenerationConfig, self).__init__(**listgen_config)
        self.lookback_days = self._parse_positive_int('lookback_days')
        self.restrict_exceptions_list = self._parse_bool('restrict_exceptions_list_to_blacklisted_imeis')
        self.generate_check_digit = self._parse_bool('generate_check_digit')
        self.output_invalid_imeis = self._parse_bool('output_invalid_imeis')

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
            'output_invalid_imeis': True
        }


class ReportGenerationConfig(ConfigSection):
    """Class representing the configuration of the lookback window used in the report generation process."""

    def __init__(self, **repgen_config):
        """Constructor which parses the report generation config."""
        super(ReportGenerationConfig, self).__init__(**repgen_config)
        self.blacklist_violations_grace_period_days = \
            self._parse_positive_int('blacklist_violations_grace_period_days')

    @property
    def section_name(self):
        """Property for the section name."""
        return 'ReportGenerationConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'blacklist_violations_grace_period_days': 2
        }


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


class StatsdConfig(ConfigSection):
    """Class representing the configuration of the StatsD daemon used to aggregate and forward application metrics."""

    def __init__(self, **statsd_config):
        """Constructor which parses the StatsD config."""
        super(StatsdConfig, self).__init__(**statsd_config)
        self.hostname = self._parse_string('hostname')
        self.port = self._parse_positive_int('port')
        self.prefix = self._parse_string('prefix', optional=True)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'StatsdConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'hostname': 'localhost',
            'port': 8125,
            'prefix': None
        }

    @property
    def env_overrides(self):
        """Property describing a key->envvar mapping for overriding config valies."""
        return {
            'hostname': 'DIRBS_STATSD_HOST',
            'port': 'DIRBS_STATSD_PORT',
            'prefix': 'DIRBS_ENV'
        }


class CatalogConfig(ConfigSection):
    """Class representing the configuration of the catalog process."""

    def __init__(self, **catalog_config):
        """Constructor which parses the catalog config."""
        super(CatalogConfig, self).__init__(**catalog_config)
        self.perform_prevalidation = self._parse_bool('perform_prevalidation')
        self.prospectors = [{'file_type': str(x['file_type']), 'paths': list(x['paths']),
                             'schema': x['schema_filename']}
                            for x in self.raw_config['prospectors']]

        path_list = [path for x in self.prospectors for path in x['paths']]
        if len(path_list) != len(set(path_list)):
            msg = 'The paths specified in the catalog config are not globally unique!'
            _logger.error(msg)
            raise ConfigParseException(msg)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'CatalogConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'perform_prevalidation': False,
            'prospectors': []
        }


class AmnestyConfig(ConfigSection):
    """Class representing the configutation for the amnesty feature."""

    def __init__(self, **amnesty_config):
        """Constructor which parses the amnesty config."""
        super(AmnestyConfig, self).__init__(**amnesty_config)
        self.amnesty_enabled = self._parse_bool('amnesty_enabled')
        self.evaluation_period_end_date = self._parse_date('evaluation_period_end_date', '%Y%m%d', 'YYYYMMDD')
        self.amnesty_period_end_date = self._parse_date('amnesty_period_end_date', '%Y%m%d', 'YYYYMMDD')
        if self.amnesty_period_end_date <= self.evaluation_period_end_date:
            msg = 'The \'amnesty_period_end_date\' must be greater than the \'evaluation_period_end_date\'!'
            _logger.error(msg)
            raise ConfigParseException(msg)

    @property
    def section_name(self):
        """Property for the section name."""
        return 'AmnestyConfig'

    @property
    def defaults(self):
        """Property describing defaults for config values."""
        return {
            'amnesty_enabled': False,
            'evaluation_period_end_date': '19700101',
            'amnesty_period_end_date': '19700102'
        }
