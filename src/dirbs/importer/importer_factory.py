"""
Factory for generating the data importer classes.

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

import logging

from dirbs.importer.operator_data_importer import OperatorDataImporter
from dirbs.importer.gsma_data_importer import GSMADataImporter
from dirbs.importer.stolen_list_importer import StolenListImporter
from dirbs.importer.pairing_list_importer import PairingListImporter
from dirbs.importer.registration_list_importer import RegistrationListImporter
from dirbs.importer.golden_list_importer import GoldenListImporter
from dirbs.importer.barred_list_importer import BarredListImporter
from dirbs.importer.barred_tac_list_importer import BarredTacListImporter
from dirbs.importer.subscriber_reg_list_importer import SubscribersListImporter
from dirbs.importer.device_association_list_importer import DeviceAssociationListImporter
from dirbs.importer.monitoring_list_importer import MonitoringListImporter


def make_data_importer(import_type, input_file, config, statsd, conn, metadata_conn,
                       run_id, metrics_root, metrics_run_root, **kwargs):
    """Factory function for creating the appropriate data importer class."""
    if import_type == 'operator':
        return make_operator_data_importer(input_file, config, statsd, conn, metadata_conn,
                                           run_id, metrics_root, metrics_run_root, **kwargs)
    elif import_type == 'gsma_tac':
        return make_gsma_data_importer(input_file, config, statsd, conn, metadata_conn,
                                       run_id, metrics_root, metrics_run_root, **kwargs)
    elif import_type == 'stolen_list':
        return make_stolen_list_importer(input_file, config, statsd, conn, metadata_conn,
                                         run_id, metrics_root, metrics_run_root, **kwargs)
    elif import_type == 'pairing_list':
        return make_pairing_list_importer(input_file, config, statsd, conn, metadata_conn,
                                          run_id, metrics_root, metrics_run_root, **kwargs)
    elif import_type == 'registration_list':
        return make_registration_list_importer(input_file, config, statsd, conn, metadata_conn,
                                               run_id, metrics_root, metrics_run_root, **kwargs)
    elif import_type == 'golden_list':
        return make_golden_list_importer(input_file, config, statsd, conn, metadata_conn,
                                         run_id, metrics_root, metrics_run_root, **kwargs)
    elif import_type == 'barred_list':
        return make_barred_list_importer(input_file, config, statsd, conn, metadata_conn,
                                         run_id, metrics_root, metrics_run_root, **kwargs)
    elif import_type == 'barred_tac_list':
        return make_barred_tac_list_importer(input_file, config, statsd, conn, metadata_conn,
                                             run_id, metrics_root, metrics_run_root, **kwargs)
    elif import_type == 'subscribers_registration_list':
        return make_subscribers_registration_list_importer(input_file, config, statsd, conn, metadata_conn,
                                                           run_id, metrics_root, metrics_run_root, **kwargs)
    elif import_type == 'device_association_list':
        return make_device_association_list_importer(input_file, config, statsd, conn, metadata_conn,
                                                     run_id, metrics_root, metrics_run_root, **kwargs)
    elif import_type == 'monitoring_list':
        return make_monitoring_list_importer(input_file, config, statsd, conn, metadata_conn,
                                             run_id, metrics_root, metrics_run_root, **kwargs)
    else:
        raise NameError('No importer found for file type: {0}'.format(import_type))


def _common_config_params(config):
    """Dictionary containing importer parameters defined in the config file."""
    return {'batch_size': config.import_config.batch_size,
            'max_local_cpus': config.multiprocessing_config.max_local_cpus,
            'max_db_connections': config.multiprocessing_config.max_db_connections}


def make_operator_data_importer(input_file, config, statsd, conn, metadata_conn,
                                run_id, metrics_root, metrics_run_root, operator_id, **kwargs):
    """Create an instance of OperatorDataImporter."""
    common_params = _common_config_params(config)
    operators = config.region_config.operators
    matching_operators = [o for o in operators if operator_id == o.id]
    assert len(matching_operators) == 1
    matching_operator = matching_operators[0]
    return OperatorDataImporter(operator_id, matching_operator.mcc_mnc_pairs, config.region_config.country_codes,
                                conn, metadata_conn, run_id, metrics_root, metrics_run_root, config.db_config,
                                input_file, logging.getLogger('dirbs.import'), statsd, **common_params, **kwargs)


def make_gsma_data_importer(input_file, config, statsd, conn, metadata_conn,
                            run_id, metrics_root, metrics_run_root, **kwargs):
    """Create an instance of GSMADataImporter."""
    common_params = _common_config_params(config)
    return GSMADataImporter(conn, metadata_conn, run_id, metrics_root, metrics_run_root, config.db_config,
                            input_file, logging.getLogger('dirbs.import'), statsd, **common_params, **kwargs)


def make_stolen_list_importer(input_file, config, statsd, conn, metadata_conn,
                              run_id, metrics_root, metrics_run_root, **kwargs):
    """Create an instance of StolenListImporter."""
    common_params = _common_config_params(config)
    return StolenListImporter(conn, metadata_conn, run_id, metrics_root, metrics_run_root, config.db_config,
                              input_file, logging.getLogger('dirbs.import'), statsd, **common_params, **kwargs)


def make_pairing_list_importer(input_file, config, statsd, conn, metadata_conn,
                               run_id, metrics_root, metrics_run_root, **kwargs):
    """Create an instance of PairingListImporter."""
    common_params = _common_config_params(config)
    return PairingListImporter(conn, metadata_conn, run_id, metrics_root, metrics_run_root, config.db_config,
                               input_file, logging.getLogger('dirbs.import'), statsd, **common_params, **kwargs)


def make_subscribers_registration_list_importer(input_file, config, statsd, conn, metadata_conn,
                                                run_id, metrics_root, metrics_run_root, **kwargs):
    """Create an instance of SubscribersListImporter."""
    common_params = _common_config_params(config)
    return SubscribersListImporter(conn, metadata_conn, run_id, metrics_root, metrics_run_root, config.db_config,
                                   input_file, logging.getLogger('dirbs.import'), statsd, **common_params, **kwargs)


def make_device_association_list_importer(input_file, config, statsd, conn, metadata_conn,
                                          run_id, metrics_root, metrics_run_root, **kwargs):
    """Create an instance of DeviceAssociationListImporter."""
    common_params = _common_config_params(config)
    return DeviceAssociationListImporter(conn, metadata_conn, run_id, metrics_root, metrics_run_root,
                                         config.db_config, input_file, logging.getLogger('dirbs.import'),
                                         statsd, **common_params, **kwargs)


def make_registration_list_importer(input_file, config, statsd, conn, metadata_conn,
                                    run_id, metrics_root, metrics_run_root, **kwargs):
    """Create an instance of RegistrationListImporter."""
    common_params = _common_config_params(config)
    return RegistrationListImporter(conn, metadata_conn, run_id, metrics_root, metrics_run_root, config.db_config,
                                    input_file, logging.getLogger('dirbs.import'), statsd, **common_params, **kwargs)


def make_golden_list_importer(input_file, config, statsd, conn, metadata_conn,
                              run_id, metrics_root, metrics_run_root, **kwargs):
    """Create an instance of GoldenListImporter."""
    common_params = _common_config_params(config)
    return GoldenListImporter(conn, metadata_conn, run_id, metrics_root, metrics_run_root, config.db_config,
                              input_file, logging.getLogger('dirbs.import'), statsd, **common_params, **kwargs)


def make_barred_list_importer(input_file, config, statsd, conn, metadata_conn,
                              run_id, metrics_root, metrics_run_root, **kwargs):
    """Create an instance of BarredListImporter."""
    common_params = _common_config_params(config)
    return BarredListImporter(conn, metadata_conn, run_id, metrics_root, metrics_run_root, config.db_config,
                              input_file, logging.getLogger('dirbs.import'), statsd, **common_params, **kwargs)


def make_monitoring_list_importer(input_file, config, statsd, conn, metadata_conn,
                                  run_id, metrics_root, metrics_run_root, **kwargs):
    """Create an instance of MonitoringListImporter."""
    common_params = _common_config_params(config)
    return MonitoringListImporter(conn, metadata_conn, run_id, metrics_root, metrics_run_root, config.db_config,
                                  input_file, logging.getLogger('dirbs.import'), statsd, **common_params, **kwargs)


def make_barred_tac_list_importer(input_file, config, statsd, conn, metadata_conn,
                                  run_id, metrics_root, metrics_run_root, **kwargs):
    """Create an instance of BarredListImporter."""
    common_params = _common_config_params(config)
    return BarredTacListImporter(conn, metadata_conn, run_id, metrics_root, metrics_run_root, config.db_config,
                                 input_file, logging.getLogger('dirbs.import'), statsd, **common_params, **kwargs)
