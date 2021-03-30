"""
Config parsing unit tests.

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
import datetime

import pytest

from dirbs.importer.gsma_data_importer import GSMADataImporter
from dirbs.importer.pairing_list_importer import PairingListImporter
from dirbs.importer.stolen_list_importer import StolenListImporter
from dirbs.importer.registration_list_importer import RegistrationListImporter
from dirbs.dimensions.duplicate_threshold import DuplicateThreshold
from dirbs.dimensions.duplicate_daily_avg import DuplicateAverageThreshold
from dirbs.condition import Condition
from dirbs.config import GSMAThresholdConfig, PairingListThresholdConfig, StolenListThresholdConfig, \
    RegistrationListThresholdConfig, AppConfig, ConfigParseException, ConfigParser, ConditionConfig
from _fixtures import *  # noqa: F403, F401
from _importer_params import GSMADataParams, PairListParams, StolenListParams, RegistrationListParams
from _helpers import get_importer, data_file_to_test, expect_success, expect_failure


def _expect_app_config_failure(*, config, expected_message):
    with pytest.raises(ConfigParseException) as ex:
        AppConfig(**config, ignore_env=True)

    assert expected_message in str(ex)


def historic_threshold_check_function_success(first_file_size, second_file_size,
                                              db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd,
                                              importer_class, importer_params,
                                              threshold_config, imei_imsi=False, imei_imsi_msisdn=False,
                                              imei_custom_header='imei,reporting_date,status',
                                              import_data_file_by_path=False,
                                              import_first_data_file_path=None,
                                              import_second_data_file_path=None):
    """Helper function to test if historic thresholds are configurable.

    This function imports a first file with large size and then a smaller file succeeding
    the historic check.
    """
    # To import file from path set import_data_file_by_path to True and set both import_first_data_file_path
    # and import_second_data_file_path with the file paths.
    first_file_to_import = import_first_data_file_path if import_data_file_by_path \
        else data_file_to_test(first_file_size, imei_imsi, imei_custom_header, imei_imsi_msisdn=imei_imsi_msisdn)

    with get_importer(importer_class,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      importer_params(filename=first_file_to_import,
                                      import_size_variation_percent=threshold_config.import_size_variation_percent,
                                      import_size_variation_absolute=threshold_config.import_size_variation_absolute,
                                      extract=False)) as large_importer:
        expect_success(large_importer, first_file_size, db_conn, logger)

    second_file_to_import = import_second_data_file_path if import_data_file_by_path \
        else data_file_to_test(second_file_size, imei_imsi, imei_custom_header, imei_imsi_msisdn=imei_imsi_msisdn)

    with get_importer(importer_class,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      importer_params(filename=second_file_to_import,
                                      import_size_variation_percent=threshold_config.import_size_variation_percent,
                                      import_size_variation_absolute=threshold_config.import_size_variation_absolute,
                                      extract=False)) as small_importer:
        expect_success(small_importer, second_file_size, db_conn, logger)


def historic_threshold_check_function_fails(first_file_size, second_file_size, db_conn, metadata_db_conn,
                                            mocked_config, tmpdir, logger,
                                            mocked_statsd, importer_class, importer_params, threshold_config,
                                            imei_imsi=False, imei_imsi_msisdn=False,
                                            imei_custom_header='imei,reporting_date,status',
                                            exc_message='', import_data_file_by_path=False,
                                            import_first_data_file_path=None, import_second_data_file_path=None):
    """Helper function to test if historic thresholds are configurable.

    This function imports a first file with large size and then a smaller file failing
    the historic check.
    """
    # To import file from path set import_data_file_by_path to True and set both import_first_data_file_path
    # and import_second_data_file_path with the file paths.
    first_file_to_import = import_first_data_file_path if import_data_file_by_path \
        else data_file_to_test(first_file_size, imei_imsi, imei_custom_header, imei_imsi_msisdn=imei_imsi_msisdn)

    with get_importer(importer_class,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      importer_params(filename=first_file_to_import,
                                      import_size_variation_percent=threshold_config.import_size_variation_percent,
                                      import_size_variation_absolute=threshold_config.import_size_variation_absolute,
                                      extract=False)) as large_importer:
        expect_success(large_importer, first_file_size, db_conn, logger)

    second_file_to_import = import_second_data_file_path if import_data_file_by_path \
        else data_file_to_test(second_file_size, imei_imsi, imei_custom_header, imei_imsi_msisdn=imei_imsi_msisdn)

    with get_importer(importer_class,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      importer_params(filename=second_file_to_import,
                                      import_size_variation_percent=threshold_config.import_size_variation_percent,
                                      import_size_variation_absolute=threshold_config.import_size_variation_absolute,
                                      extract=False)) as small_importer:
        expect_failure(small_importer, exc_message=exc_message)


# Pairing_list historic thresholds tests
def test_pairing_list_historical_thresholds_failure(logger, mocked_statsd, db_conn, metadata_db_conn,
                                                    tmpdir, mocked_config, monkeypatch):
    """Test Depot ID not known yet.

    Verify that it is possible to configure historical_thresholds from yaml file for pairing importer.
    Threshold values set from yaml file: import_size_variation_absolute: -1; import_size_variation_percent: 0.5.
    Expect to fail to import a file with size 100 followed by a smaller file of size 49 because
    import_size_variation_percent allows to import at least 50 rows.
    """
    pairing_list_instance = PairingListThresholdConfig(ignore_env=True,
                                                       import_size_variation_absolute=-1,
                                                       import_size_variation_percent=0.5)

    historic_threshold_check_function_fails(100, 49, db_conn, metadata_db_conn, mocked_config, tmpdir,
                                            logger, mocked_statsd,
                                            PairingListImporter, PairListParams,
                                            pairing_list_instance, imei_imsi_msisdn=True,
                                            exc_message='Failed import size historic check, historic '
                                                        'value is: 100.00, imported data has: 49.00 and '
                                                        'minimum required is 50.00')


def test_pairing_list_historical_thresholds_success(logger, mocked_statsd, db_conn, metadata_db_conn,
                                                    tmpdir, mocked_config):
    """Test Depot ID not known yet.

    Verify that it is possible to configure historical_thresholds from yaml file for pairing importer.
    Threshold values set from yaml file: import_size_variation_absolute: -1; import_size_variation_percent: 0.5.
    Expect to succeed to import a file with size 100 followed by a smaller file of size 51 because
    import_size_variation_percent allows to import at least 50 rows.
    """
    # import_size_variation_absolute: -1
    # import_size_variation_percent: 0.5
    # import size 100 then 51
    pairing_list_instance = PairingListThresholdConfig(ignore_env=True,
                                                       import_size_variation_absolute=-1,
                                                       import_size_variation_percent=0.5)

    historic_threshold_check_function_success(100, 51, db_conn, metadata_db_conn, mocked_config, tmpdir,
                                              logger, mocked_statsd,
                                              PairingListImporter, PairListParams, pairing_list_instance,
                                              imei_imsi_msisdn=True)


def test_pairing_list_historical_thresholds_success_with_abs_threshold(logger, mocked_statsd, db_conn,
                                                                       metadata_db_conn, tmpdir, mocked_config):
    """Test Depot ID not known yet.

    Verify that it is possible to configure historical_thresholds from yaml file for pairing importer.
    Threshold values from yaml file: import_size_variation_absolute: 48; import_size_variation_percent: 0.5.
    Expect to fail despite the fact that the percentage threshold allows to import at least 50 rows as in this case
    (we import a file with size 100 followed by a smaller file of size 51).
    In fact the test fails because import_size_variation_absolute set 48 allows at least 52 rows to import.
    """
    # import_size_variation_absolute: 48
    # import_size_variation_percent: 0.5
    # import size 100 then 51
    pairing_list_instance = PairingListThresholdConfig(ignore_env=True,
                                                       import_size_variation_absolute=48,
                                                       import_size_variation_percent=0.5)

    historic_threshold_check_function_fails(100, 51, db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                                            mocked_statsd, PairingListImporter, PairListParams,
                                            pairing_list_instance, imei_imsi_msisdn=True,
                                            exc_message='Failed import size historic check, '
                                                        'historic value is: 100.00, imported data has: '
                                                        '51.00 and minimum required is 52.00')


# Stolen_list historic thresholds tests
def test_stolen_list_historical_thresholds_failure(logger, mocked_statsd, db_conn, metadata_db_conn,
                                                   tmpdir, mocked_config):
    """Test Depot ID not known yet.

    Verify that it is possible to configure historical_thresholds from yaml file for stolen importer.
    Threshold values set from yaml file: import_size_variation_absolute: -1; import_size_variation_percent: 0.5.
    Expect to fail to import a file with size 100 followed by a smaller file of size 49 because
    import_size_variation_percent allows to import at least 50 rows.
    """
    # import_size_variation_absolute: -1
    # import_size_variation_percent: 0.5
    # import size 100 then 49
    stolen_list_instance = StolenListThresholdConfig(ignore_env=True,
                                                     import_size_variation_absolute=-1,
                                                     import_size_variation_percent=0.5)

    historic_threshold_check_function_fails(100, 49, db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                                            mocked_statsd, StolenListImporter, StolenListParams,
                                            stolen_list_instance,
                                            exc_message='Failed import size historic check, historic '
                                                        'value is: 100.00, imported data has: 49.00 and '
                                                        'minimum required is 50.00')


def test_stolen_list_historical_thresholds_success(logger, mocked_statsd, db_conn, metadata_db_conn,
                                                   tmpdir, mocked_config):
    """Test Depot ID not known yet.

    Verify that it is possible to configure historical_thresholds from yaml file for stolen importer.
    Threshold values set from yaml file: import_size_variation_absolute: -1; import_size_variation_percent: 0.5.
    Expect to succeed to import a file with size 100 followed by a smaller file of size 51 because
    import_size_variation_percent allows to import at least 50 rows.
    """
    # import_size_variation_absolute: -1
    # import_size_variation_percent: 0.5
    # import size 100 then 51
    stolen_list_instance = StolenListThresholdConfig(ignore_env=True,
                                                     import_size_variation_absolute=-1,
                                                     import_size_variation_percent=0.5)

    historic_threshold_check_function_success(100, 51, db_conn, metadata_db_conn, mocked_config, tmpdir, logger,
                                              mocked_statsd, StolenListImporter, StolenListParams,
                                              stolen_list_instance)


def test_stolen_list_historical_thresholds_success_with_abs_threshold(logger, mocked_statsd, db_conn, metadata_db_conn,
                                                                      tmpdir, mocked_config):
    """Test Depot ID not known yet.

    Verify that it is possible to configure historical_thresholds from yaml file for stolen importer.
    Threshold values from yaml file: import_size_variation_absolute: 48; import_size_variation_percent: 0.5.
    Expect to fail despite the fact that the percentage threshold allows to import at least 50 rows as in this case
    (we import a file with size 100 followed by a smaller file of size 51).
    In fact the test fails because import_size_variation_absolute set 48 allows at least 52 rows to import.
    """
    # import_size_variation_absolute: 48
    # import_size_variation_percent: 0.5
    # import size 100 then 51
    stolen_list_instance = StolenListThresholdConfig(ignore_env=True,
                                                     import_size_variation_absolute=48,
                                                     import_size_variation_percent=0.5)
    historic_threshold_check_function_fails(100, 51, db_conn, metadata_db_conn, mocked_config, tmpdir,
                                            logger, mocked_statsd,
                                            StolenListImporter, StolenListParams,
                                            stolen_list_instance,
                                            exc_message='Failed import size historic check, '
                                                        'historic value is: 100.00, imported data has: '
                                                        '51.00 and minimum required is 52.00')


# Registration_list historic thresholds tests
def test_registration_list_historical_thresholds_failure(logger, mocked_statsd, db_conn, metadata_db_conn,
                                                         tmpdir, mocked_config):
    """Test Depot ID not known yet.

    Verify that it is possible to configure historical_thresholds from yaml file for registration_list importer.
    Threshold values set from yaml file: import_size_variation_absolute: -1; import_size_variation_percent: 0.5.
    Expect to fail to import a file with size 100 followed by a smaller file of size 49 because
    import_size_variation_percent allows to import at least 50 rows.
    """
    # import_size_variation_absolute: -1
    # import_size_variation_percent: 0.5
    # import size 100 then 49
    import_list_instance = RegistrationListThresholdConfig(ignore_env=True,
                                                           import_size_variation_absolute=-1,
                                                           import_size_variation_percent=0.5)

    historic_threshold_check_function_fails(100, 49, db_conn, metadata_db_conn, mocked_config, tmpdir,
                                            logger, mocked_statsd,
                                            RegistrationListImporter, RegistrationListParams,
                                            import_list_instance,
                                            imei_custom_header='approved_imei,make,model,status,'
                                                               'model_number,brand_name,device_type,'
                                                               'radio_interface,device_id',
                                            exc_message='Failed import size historic check, historic '
                                                        'value is: 100.00, imported data has: 49.00 and '
                                                        'minimum required is 50.00')


def test_registration_list_historical_thresholds_success(logger, mocked_statsd, db_conn, metadata_db_conn,
                                                         tmpdir, mocked_config):
    """Test Depot ID not known yet.

    Verify that it is possible to configure historical_thresholds from yaml file for registration_list importer.
    Threshold values set from yaml file: import_size_variation_absolute: -1; import_size_variation_percent: 0.5.
    Expect to succeed to import a file with size 100 followed by a smaller file of size 51 because
    import_size_variation_percent allows to import at least 50 rows.
    """
    # import_size_variation_absolute: -1
    # import_size_variation_percent: 0.5
    # import size 100 then 51
    import_list_instance = RegistrationListThresholdConfig(ignore_env=True,
                                                           import_size_variation_absolute=-1,
                                                           import_size_variation_percent=0.5)

    historic_threshold_check_function_success(100, 51, db_conn, metadata_db_conn, mocked_config, tmpdir,
                                              logger, mocked_statsd,
                                              RegistrationListImporter, RegistrationListParams,
                                              import_list_instance, imei_custom_header='approved_imei,make,model,'
                                                                                       'status,model_number,'
                                                                                       'brand_name,device_type,'
                                                                                       'radio_interface,device_id'
                                              )


def test_registration_list_historical_thresholds_success_with_abs_threshold(logger, mocked_statsd, db_conn,
                                                                            metadata_db_conn, tmpdir,
                                                                            mocked_config):
    """Test Depot ID not known yet.

    Verify that it is possible to configure historical_thresholds from yaml file for registration_list importer.
    Threshold values from yaml file: import_size_variation_absolute: 48; import_size_variation_percent: 0.5.
    Expect to fail despite the fact that the percentage threshold allows to import at least 50 rows as in this case
    (we import a file with size 100 followed by a smaller file of size 51).
    In fact the test fails because import_size_variation_absolute set 48 allows at least 52 rows to import.
    """
    # import_size_variation_absolute: 48
    # import_size_variation_percent: 0.5
    # import size 100 then 51
    import_list_instance = RegistrationListThresholdConfig(ignore_env=True,
                                                           import_size_variation_absolute=48,
                                                           import_size_variation_percent=0.5)

    historic_threshold_check_function_fails(100, 51, db_conn, metadata_db_conn, mocked_config, tmpdir,
                                            logger, mocked_statsd,
                                            RegistrationListImporter, RegistrationListParams,
                                            import_list_instance,
                                            imei_custom_header='approved_imei,make,model,status,model_number,'
                                                               'brand_name,device_type,radio_interface,device_id',
                                            exc_message='Failed import size historic check, '
                                                        'historic value is: 100.00, imported data has: '
                                                        '51.00 and minimum required is 52.00')


# GSMA historic thresholds tests
def test_gsma_historical_thresholds_failure(logger, mocked_statsd, db_conn, metadata_db_conn, tmpdir, mocked_config):
    """Test Depot ID not known yet.

    Verify that it is possible to configure historical_thresholds from yaml file for GSMA importer.
    Threshold values set from yaml file: import_size_variation_absolute: -1; import_size_variation_percent: 0.5.
    Expect to fail to import a file with size 100 followed by a smaller file of size 49 because
    import_size_variation_percent allows to import at least 50 rows.
    """
    # import_size_variation_absolute: -1
    # import_size_variation_percent: 0.5
    # import size 100 then 49
    gsma_instance = GSMAThresholdConfig(ignore_env=True,
                                        import_size_variation_absolute=-1,
                                        import_size_variation_percent=0.5)

    historic_threshold_check_function_fails(100, 49, db_conn, metadata_db_conn, mocked_config, tmpdir,
                                            logger, mocked_statsd,
                                            GSMADataImporter, GSMADataParams,
                                            gsma_instance,
                                            exc_message='Failed import size historic check, historic '
                                                        'value is: 100.00, imported data has: 49.00 and '
                                                        'minimum required is 50.00',
                                            import_data_file_by_path=True,
                                            import_first_data_file_path='gsma_100_valid_rows_import_with_dupl.txt',
                                            import_second_data_file_path='gsma_49_valid_rows_import_with_dupl.txt')


def test_gsma_historical_thresholds_success(logger, mocked_statsd, db_conn, metadata_db_conn, tmpdir, mocked_config):
    """Test Depot ID not known yet.

    Verify that it is possible to configure historical_thresholds from yaml file for GSMA importer.
    Threshold values set from yaml file: import_size_variation_absolute: -1; import_size_variation_percent: 0.5.
    Expect to succeed to import a file with size 100 followed by a smaller file of size 51 because
    import_size_variation_percent allows to import at least 50 rows.
    """
    # import_size_variation_absolute: -1
    # import_size_variation_percent: 0.5
    # import size 100 then 51
    gsma_instance = GSMAThresholdConfig(ignore_env=True,
                                        import_size_variation_absolute=-1,
                                        import_size_variation_percent=0.5)

    historic_threshold_check_function_success(100, 51, db_conn, metadata_db_conn, mocked_config, tmpdir,
                                              logger, mocked_statsd,
                                              GSMADataImporter, GSMADataParams, gsma_instance,
                                              import_data_file_by_path=True,
                                              import_first_data_file_path='gsma_100_valid_rows_import_with_dupl.txt',
                                              import_second_data_file_path='gsma_51_valid_rows_import_with_dupl.txt')


def test_gsma_historical_thresholds_success_with_abs_threshold(logger, mocked_statsd, db_conn, metadata_db_conn,
                                                               tmpdir, mocked_config):
    """Test Depot ID not known yet.

    Verify that it is possible to configure historical_thresholds from yaml file for GSMA importer.
    Threshold values from yaml file: import_size_variation_absolute: 48; import_size_variation_percent: 0.5.
    Expect to fail despite the fact that the percentage threshold allows to import at least 50 rows as in this case
    (we import a file with size 100 followed by a smaller file of size 51).
    In fact the test fails because import_size_variation_absolute set 48 allows at least 52 rows to import.
    """
    # import_size_variation_absolute: 48
    # import_size_variation_percent: 0.5
    # import size 100 then 51
    gsma_instance = GSMAThresholdConfig(ignore_env=True,
                                        import_size_variation_absolute=48,
                                        import_size_variation_percent=0.5)

    historic_threshold_check_function_fails(100, 51, db_conn, metadata_db_conn, mocked_config, tmpdir,
                                            logger, mocked_statsd,
                                            GSMADataImporter, GSMADataParams,
                                            gsma_instance,
                                            exc_message='Failed import size historic check, '
                                                        'historic value is: 100.00, imported data has: '
                                                        '51.00 and minimum required is 52.00',
                                            import_data_file_by_path=True,
                                            import_first_data_file_path='gsma_100_valid_rows_import_with_dupl.txt',
                                            import_second_data_file_path='gsma_51_valid_rows_import_with_dupl.txt')


def test_config_symbols_and_case_sensitivity():
    """Test Depot not known yet.

    Verify that config parser allows only letters, underscores, digits (0-9) for operator_id and condition labels.
    Verify that operator_id and condition labels are converted to lower case by the config parser and not
    allowed to be duplicated.
    """
    # Step 1: Verify that config parser allows only letters, underscores, digits (0-9) for operator_id and condition
    # labels.
    # Step 2: Verify that operator_id and condition labels are converted to lower case by the config parser and not
    # allowed to be duplicated.
    # Step 1 for condition names:
    # bad symbol '*'
    conditions_config = [{'label': 'g*sma_not_found',
                          'reason': 'TAC not found in GSMA TAC database',
                          'dimensions': [{'module': 'gsma_not_found'}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = 'Condition label g*sma_not_found must contain only letters, underscores or digits(0-9)!'
    _expect_app_config_failure(config=cfg, expected_message=msg)

    # white space
    conditions_config = [{'label': 'g sma_not_found',
                          'reason': 'TAC not found in GSMA TAC database',
                          'dimensions': [{'module': 'gsma_not_found'}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = 'Condition label g sma_not_found must contain only letters, underscores or digits(0-9)!'
    _expect_app_config_failure(config=cfg, expected_message=msg)

    # Step 1 for operator_ids:
    # bad symbol '@'
    cfg = {'region': {'name': 'Country1',
                      'country_codes': '22',
                      'operators': [{'name': 'First Operator',
                                     'id': 'Operator@1',
                                     'mcc_mnc_pairs': [{'mnc': '01', 'mcc': '111'}]}]}}

    msg = 'Operator_id operator@1 must contain only letters, underscores or digits(0-9)!'
    _expect_app_config_failure(config=cfg, expected_message=msg)

    # white space
    cfg = {'region': {'name': 'Country1',
                      'country_codes': '22',
                      'operators': [{'name': 'First Operator',
                                     'id': 'operator 1',
                                     'mcc_mnc_pairs': [{'mnc': '01', 'mcc': '111'}]}]}}

    msg = 'Operator_id operator 1 must contain only letters, underscores or digits(0-9)!'
    _expect_app_config_failure(config=cfg, expected_message=msg)

    # Step 2 for condition names:
    conditions_config = [{'label': 'GSMA_NOT_FOUND',
                          'reason': 'TAC not found in GSMA TAC database',
                          'dimensions': [{'module': 'gsma_not_found'}]},
                         {'label': 'gsma_not_found',
                          'reason': 'TAC not found in GSMA TAC database',
                          'dimensions': [{'module': 'gsma_not_found'}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = 'Duplicate condition names gsma_not_found found in config. Condition names are case insensitive!'
    _expect_app_config_failure(config=cfg, expected_message=msg)

    # Step 2 for operator_ids:
    cfg = {'region': {'name': 'Country1',
                      'country_codes': '22',
                      'operators': [{'name': 'First Operator',
                                     'id': 'OPERATOR1',
                                     'mcc_mnc_pairs': [{'mnc': '01', 'mcc': '111'}]},
                                    {'name': 'First Operator',
                                     'id': 'operator1',
                                     'mcc_mnc_pairs': [{'mnc': '01', 'mcc': '111'}]}]}}

    msg = 'Duplicate operator_ids operator1 found in config. Operator_ids are case insensitive!'
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_valid_config_to_import(mocked_config):
    """Test Depot not known yet.

    Verify that config parser allows valid .yml file using mocked_config fixture.
    """
    assert mocked_config.db_config.user == 'postgres'
    operator_id = 'operator1'
    operators = mocked_config.region_config.operators
    operator = [o for o in operators if operator_id == o.id]
    assert operator[0].mcc_mnc_pairs == [{'mcc': '111', 'mnc': '01'}]
    assert mocked_config.log_config.level == 'info'
    assert mocked_config.conditions[0].label == 'gsma_not_found'
    assert mocked_config.operator_threshold_config.unclean_threshold == 0.05
    assert mocked_config.listgen_config.lookback_days == 180


def test_empty_config_file(tmpdir):
    """Test Depot not known yet.

    Verify that config parser doesn't allow empty config files.
    """
    file_name = 'config_empty_config_file.yml'
    config_path = os.path.join(str(tmpdir), file_name)
    with pytest.raises(ConfigParseException) as ex, open(config_path, 'w') as f:
        cp = ConfigParser()
        cp.parse_config(config_paths=[f.name], ignore_env=True)

    assert 'Invalid DIRBS Config file' in str(ex)


def test_null_config_file():
    """Test Depot not known yet.

    Verify that config parser doesn't allow missing config files.
    """
    config_path = '/config_file_missing.yml'
    with pytest.raises(ConfigParseException) as ex:
        cp = ConfigParser()
        cp.parse_config(config_paths=[config_path], ignore_env=True)

    assert 'Missing config file' in str(ex)


def test_config_malformed_propname():
    """Test Depot not known yet.

    Verify that config parser doesn't allow malformed attribute name for Region Config.
    """
    cfg = {'rDegion': {'name': 'Country1', 'country_codes': '22'}}
    msg = 'Missing attribute name in config'
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_non_numeric_value_country_code():
    """Test Depot not known yet.

    Verify that config parser doesn't allow non numeric value for country code.
    """
    cfg = {'region': {'name': 'Country1', 'country_codes': '-$'}}
    msg = 'non-numeric value for country code'
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_missing_mcc_mnc_pairs():
    """Test Depot not known yet.

    Verify that config parser doesn't allow missing mcc_mnc_pairs.
    """
    cfg = {'region': {'name': 'Country1',
                      'country_codes': '22',
                      'operators': [{'name': 'First Operator',
                                     'id': 'OPERATOR1'}]}}

    msg = 'Missing (or non-list) mcc_mnc_pairs in config for operator ID operator1'
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_non_integer_mcc():
    """Test Depot not known yet.

    Verify that config parser doesn't allow non integer mcc.
    """
    cfg = {'region': {'name': 'Country1',
                      'country_codes': '22',
                      'operators': [{'name': 'First Operator',
                                     'id': 'OPERATOR1',
                                     'mcc_mnc_pairs': [{'mnc': '01', 'mcc': '$$'}]}]}}

    msg = 'Non-existent or non integer mcc in config for operator ID operator1'
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_duplicated_mcc_mnc():
    """Test Depot not known yet.

    Verify that config parser doesn't allow duplicated MCC-MNC pairs
    """
    msg = 'Duplicate MCC-MNC pairs 0101 found in config'
    cfg = {'region': {'name': 'Country1',
                      'country_codes': '22',
                      'operators': [{'name': 'First Operator',
                                     'id': 'OPERATOR1',
                                     'mcc_mnc_pairs': [{'mcc': '01', 'mnc': '01'}, {'mcc': '01', 'mnc': '01'}]}]}}
    _expect_app_config_failure(config=cfg, expected_message=msg)

    cfg = {'region': {'name': 'Country1',
                      'country_codes': '22',
                      'operators': [{'name': 'First Operator',
                                     'id': 'OPERATOR1',
                                     'mcc_mnc_pairs': [{'mcc': '01', 'mnc': '01'}]},
                                    {'name': 'Second Operator',
                                     'id': 'OPERATOR2',
                                     'mcc_mnc_pairs': [{'mcc': '01', 'mnc': '01'}]}]}}
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_overlapping_mcc_mncs():
    """Test Depot not known yet.

    Verify that config parser doesn't allow MCC-MNC values that "overlap" (where one MCC-MNC pair is prefixed by
    another)
    """
    msg = 'MCC-MNC pair 0101 found which starts with another configured MCC-MNC pair 010'
    cfg = {'region': {'name': 'Country1',
                      'country_codes': '22',
                      'operators': [{'name': 'First Operator',
                                     'id': 'OPERATOR1',
                                     'mcc_mnc_pairs': [{'mcc': '01', 'mnc': '0'}, {'mcc': '01', 'mnc': '01'}]}]}}
    _expect_app_config_failure(config=cfg, expected_message=msg)

    cfg = {'region': {'name': 'Country1',
                      'country_codes': '22',
                      'operators': [{'name': 'First Operator',
                                     'id': 'OPERATOR1',
                                     'mcc_mnc_pairs': [{'mcc': '01', 'mnc': '0'}]},
                                    {'name': 'Second Operator',
                                     'id': 'OPERATOR2',
                                     'mcc_mnc_pairs': [{'mcc': '01', 'mnc': '01'}]}]}}
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_not_list_dimension():
    """Test Depot not known yet.

    Verify that config parser doesn't allow dimensions to be a non list type.
    """
    conditions_config = [{'label': 'GSMA_NOT_FOUND',
                          'reason': 'TAC not found in GSMA TAC database',
                          'dimensions': ''}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = 'Dimensions not a list type'
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_bad_db_port():
    """Test Depot not known yet.

    Verify that config parser doesn't allow as potgress port a non integer.
    """
    cfg = {'postgresql': {'port': 'ss'}, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = 'port value "ss" must be an integer value'
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_dupl_cond():
    """Test Depot not known yet.

    Verify that config parser doesn't allow duplicate conditions.
    """
    conditions_config = [{'label': 'local_stolen',
                          'reason': 'IMEI found on local stolen list',
                          'dimensions': [{'module': 'stolen_list'}]},
                         {'label': 'local_stolen',
                          'reason': 'IMEI found on local stolen list',
                          'dimensions': [{'module': 'stolen_list'}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = 'Duplicate condition names local_stolen found in config. Condition names are case insensitive!'
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_cond_with_comma():
    """Test Depot not known yet.

    Verify that config parser doesn't allow conditions with commas.
    """
    conditions_config = [{'label': 'gsma_not_,found',
                          'reason': 'TAC not found in GSMA TAC database',
                          'dimensions': [{'module': 'gsma_not_found'}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = 'Condition label gsma_not_,found must contain only letters, underscores or digits(0-9)!'
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_invalid_settings():
    """Test Depot not known yet.

    Verify that config parser doesn't allow invalid_settings.
    """
    cfg = {'region': {'naame': 'Country1'}}
    msg = 'Missing attribute name in config'
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_parse_positive_int():
    """Test Depot not known yet.

    Verify config parser function for parsing positive int.
    """
    conditions_config = [{'label': 'malformed_imei',
                          'grace_period_days': '2,2',
                          'reason': 'Invalid characters detected in IMEI',
                          'dimensions': [{'module': 'malformed_imei'}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    _expect_app_config_failure(config=cfg, expected_message='value "2,2" must be an integer value')


def test_config_parse_positive_int_negative_param():
    """Test Depot not known yet.

    Verify that config parser function for parsing positive int doesn't allow negative values.
    """
    conditions_config = [{'label': 'malformed_imei',
                          'grace_period_days': '-2',
                          'reason': 'Invalid characters detected in IMEI',
                          'dimensions': [{'module': 'malformed_imei'}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    _expect_app_config_failure(config=cfg, expected_message='value "-2" must be greater than or equal to 0')


def test_parse_float_ratio_non_num_value():
    """Test Depot not known yet.

    Verify config parser function for parsing float value in case of non numeric value.
    """
    cfg = {'operator_threshold': {'null_imei_threshold': 'a'}, 'region': {'name': 'Country1', 'country_codes': '22'}}
    _expect_app_config_failure(config=cfg, expected_message='null_imei_threshold value "a" is non-numeric')


def test_parse_float_ratio_not_in_range_zero_one():
    """Test Depot not known yet.

    Verify config parser function for parsing float value in case of null_imei_threshold value not between 0 and 1.
    """
    cfg = {'operator_threshold': {'null_imei_threshold': '-1'}, 'region': {'name': 'Country1', 'country_codes': '22'}}
    _expect_app_config_failure(config=cfg, expected_message='null_imei_threshold value "-1.0" not between 0 and 1')


def test_parse_string_max_len():
    """Test Depot not known yet.

    Verify config parser function for parsing string properties with max length value.
    """
    conditions_config = [{'label': 'gsma_not_found_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                          'reason': 'TAC not found in GSMA TAC database',
                          'dimensions': [{'module': 'gsma_not_found'}]}]

    msg = 'ConditionConfig: label value "gsma_not_found_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ' \
          'is limited to 64 characters and has 71'
    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_none_properties():
    """Test Depot not known yet.

    Verify that config parser doesn't allow none properties.
    """
    cfg = {'region': {'country_codes': '22'}}
    _expect_app_config_failure(config=cfg, expected_message='Missing attribute name in config')


def test_parse_boolean():
    """Test Depot not known yet.

    Verify config parser function for parsing boolean values.
    """
    conditions_config = [{'label': 'local_stolen',
                          'reason': 'IMEI found on local stolen list',
                          'blocking': '11',
                          'dimensions': [{'module': 'stolen_list'}]}]

    msg = 'blocking value "11" is not a valid boolean value'
    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_invalid_use_reserved_operator():
    """Test Depot not known yet.

    Verify that config parser doesn't allow invalid use of reserved operator
    name __all__ in config.
    """
    cfg = {'region': {'name': 'Country1',
                      'country_codes': '22',
                      'operators': [{'name': 'invalidname',
                                     'id': '__all__',
                                     'mcc_mnc_pairs': [{'mnc': '01', 'mcc': '$$'}]}]}}

    msg = "Invalid use of reserved operator name \'__all__\' in config!"
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_no_module_specified():
    """Test Depot not known yet.

    Verify that config parser doesn't allow modules not to be specified.
    """
    conditions_config = [{'label': 'gsma_not_found',
                          'reason': 'TAC not found in GSMA TAC database',
                          'dimensions': [{'aa': 'aaa'}]}]

    msg = 'No module specified!'
    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_module_cannot_be_imported():
    """Test Depot not known yet.

    Verify that config parser checks for modules that cannot be imported.
    """
    conditions_config = [{'label': 'gsma_not_found',
                          'reason': 'TAC not found in GSMA TAC database',
                          'dimensions': [{'module': 'aaa'}]}]

    msg = 'module aaa can not be imported'
    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_dim_no_int_threshold_value():
    """Test Depot not known yet.

    Verify that config parser only allows valid dimension params.
    """
    conditions_config = [{'label': 'duplicate_mk1',
                          'reason': 'Duplicate IMEI detected',
                          'dimensions': [{'module': 'duplicate_threshold',
                                          'parameters': {'threshold': 'aaa', 'period_days': '120'}}]}]

    msg = "Could not create dimension \'duplicate_threshold\' with supplied parameters. Cause: " \
          "\'threshold\' parameter must be an integer, got \'aaa\' instead"
    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_dim_no_int_lookback_days_local_non_dirbs_roamer_dim():
    """Test Depot not known yet.

    Verify that config parser only int lookback_days for used_by_local_non_dirbs dimension.
    """
    conditions_config = [{'label': 'used_by_local_non_dirbs_roamer',
                          'reason': 'IMEI found for non DIRBS roamer',
                          'dimensions': [{'module': 'used_by_local_non_dirbs_roamer',
                                          'parameters': {'lookback_days': 'AAA'}}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = "Could not create dimension \'used_by_local_non_dirbs_roamer\' with supplied parameters. Cause: " \
          "\'lookback_days\' parameter must be an integer, got \'AAA\' instead"
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_dim_no_int_lookback_days_international_roamers_dim():
    """Test Depot not known yet.

    Verify that config parser only int lookback_days for international_roamer dimension.
    """
    conditions_config = [{'label': 'used_by_international_roamer',
                          'reason': 'IMEI found for local non DIRBS roamer',
                          'dimensions': [{'module': 'used_by_international_roamer',
                                          'parameters': {'lookback_days': 'BBB'}}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = "Could not create dimension \'used_by_international_roamer\' with supplied parameters. " \
          "Cause: \'lookback_days\' parameter must be an integer, got \'BBB\' instead"
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_dim_no_int_lookback_days_dirbs_subscriber_dim():
    """Test Depot not known yet.

    Verify that config parser only int lookback_days for dirbs_subscriber dimension.
    """
    conditions_config = [{'label': 'used_by_dirbs_subscriber',
                          'reason': 'IMEI found for DIRBS subscriber',
                          'dimensions': [{'module': 'used_by_dirbs_subscriber',
                                          'parameters': {'lookback_days': 'CCC'}}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = "Could not create dimension \'used_by_dirbs_subscriber\' with supplied parameters. " \
          "Cause: \'lookback_days\' parameter must be an integer, got \'CCC\' instead"
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_duplicate_daily_avg_no_int_min_seen_days():
    """Test Depot not known yet.

    Verify that config parser only int _min_seen_days for duplicate_daily_avg dimension.
    """
    conditions_config = [{'label': 'duplicate_mk1',
                          'reason': 'Duplicate IMEI detected',
                          'dimensions': [{'module': 'duplicate_daily_avg',
                                          'parameters': {'min_seen_days': 'ABC', 'threshold': '4.0',
                                                         'period_days': '30'}}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = "Could not create dimension \'duplicate_daily_avg\' with supplied parameters. " \
          "Cause: \'min_seen_days\' parameter must be an integer, got \'ABC\' instead"
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_duplicate_daily_avg_no_float_threshold():
    """Test Depot not known yet.

    Verify that config parser only float threshold for duplicate_daily_avg dimension.
    """
    conditions_config = [{'label': 'duplicate_mk1',
                          'reason': 'Duplicate IMEI detected',
                          'dimensions': [{'module': 'duplicate_daily_avg',
                                          'parameters': {'min_seen_days': '20', 'threshold': 'AA',
                                                         'period_days': '30'}}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = "Could not create dimension \'duplicate_daily_avg\' with supplied parameters. Cause: " \
          "\'threshold\' parameter must be a float, got \'AA\' instead"
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_dupl_threshold_dim_null_param_values():
    """Test Depot not known yet.

    Verify that config parser only allows not null dimension param values.
    """
    conditions_config = [{'label': 'duplicate_mk1',
                          'reason': 'Duplicate IMEI detected',
                          'dimensions': [{'module': 'duplicate_threshold',
                                          'parameters': {'threshold': None,
                                                         'period_days': '30'}}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = "Could not create dimension \'duplicate_threshold\' with supplied parameters. Cause: " \
          "\'threshold\' parameter must be an integer, got \'None\' instead"
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_dim_invalid_param():
    """Test Depot not known yet.

    Verify that config parser doesn't allows invalid dimension params.
    """
    conditions_config = [{'label': 'duplicate_mk1',
                          'reason': 'Duplicate IMEI detected',
                          'dimensions': [{'module': 'duplicate_threshold',
                                          'parameters': {'threshold': '100',
                                                         'threshold2': '13',
                                                         'period_days': '120'}}]}]

    cfg = {'conditions': conditions_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    msg = "Could not create dimension \'duplicate_threshold\' with supplied parameters. Cause: " \
          "__init__() got an unexpected keyword argument \'threshold2\'"
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_config_duplicate_dim_period(logger):
    """Test Depot not known yet.

    1. Verify that config parser doesn't allows simultaneous non-null period_days and period_months.
    2. Verify that config parser doesn't allows simultaneous null period_days and period_months.
    3. Verify that config parser doesn't allows non-integer period_months (or period_days).
    4. Verify that config parser doesn't allow negative period_months (or period_days).
    5. Verify that config parser allows allows a valid integer period for period_months and period_days.
    6. Verify that config parser doesn't allow min_seen_days greater than period_days for duplicate_daily_avg
    """
    # DuplicateAverageThreshold and DuplicateThreshold constuctor do not take same mandatory keyword params
    # e.g. min_seen_days
    common_param_dict = {'threshold': 100}
    min_days_param_dict = {'threshold': 100, 'min_seen_days': 5}

    for cls in [DuplicateAverageThreshold, DuplicateThreshold]:
        param_dict = common_param_dict if cls is DuplicateThreshold else min_days_param_dict

        # Case 1
        with pytest.raises(ValueError):
            param_dict['period_days'] = 1
            param_dict['period_months'] = 2
            cls(**param_dict)
        # Case 2
        with pytest.raises(ValueError):
            param_dict['period_days'] = None
            param_dict['period_months'] = None
            cls(**param_dict)
        # Case 3
        with pytest.raises(ValueError):
            param_dict['period_days'] = 'a'
            param_dict['period_months'] = None
            cls(**param_dict)
        with pytest.raises(ValueError):
            param_dict['period_days'] = None
            param_dict['period_months'] = 'a'
            cls(**param_dict)
        # Case 4
        with pytest.raises(ValueError):
            param_dict['period_days'] = None
            param_dict['period_months'] = -1
            cls(**param_dict)
        with pytest.raises(ValueError):
            param_dict['period_days'] = -1
            param_dict['period_months'] = None
            cls(**param_dict)

        # Case 5
        param_dict['period_days'] = 6
        param_dict['period_months'] = None
        dim = cls(**param_dict)
        assert dim._period_days == 6
        assert dim._period_months is None
        param_dict['period_days'] = None
        param_dict['period_months'] = 6
        dim = cls(**param_dict)
        assert dim._period_days is None
        assert dim._period_months == 6

    # Case 6
    with pytest.raises(ValueError):
        DuplicateAverageThreshold(period_days=2, min_seen_days=5, threshold=100)


def test_config_missing_param(logger, db_conn):
    """Test Depot not known yet.

    Verify that config parser allows missing dimension params and replaces their values with the default ones.
    """
    cond_config = {
        'label': 'dummy_test_condition',
        'grace_period_days': 0,
        'blocking': False,
        'sticky': False,
        'reason': 'Some default reason',
        'dimensions': [{'module': 'gsma_not_found'}]
    }

    # Most dimensions no longer allow default parameters, gsma_not_found is an example of one that does
    # (ignore_rbi_delays). The below is basically testing that final_rbi_delays is computed as if ignore_rbi_delays
    # is False if nothing is passed in and that invert is also set to False
    cond_config = ConditionConfig(ignore_env=True, **cond_config)
    c = Condition(cond_config)
    assert len(c.dimensions) == 1
    assert c.dimensions[0].final_rbi_delays != {}
    assert c.dimensions[0].invert is False


def test_config_max_local_cpus():
    """Test Depot not known yet.

    Verify that config parser doesn't allow local cpu values higher than threshold.
    """
    cfg = {'region': {'name': 'Country1', 'country_codes': '22'}, 'multiprocessing': {'max_local_cpus': '100'}}
    _expect_app_config_failure(config=cfg, expected_message='max_local_cpus must be at least 1 and can not be '
                                                            'set higher than CPUs present in the system minus one!')


def test_config_max_db_connections():
    """Test Depot not known yet.

    Verify that config parser doesn't allow db connections higher than threshold.
    """
    cfg = {'region': {'name': 'Country1', 'country_codes': '22'}, 'multiprocessing': {'max_db_connections': '1000'}}
    msg = 'max_db_connections must be at least 1 and can not be set higher than 32!'
    _expect_app_config_failure(config=cfg, expected_message=msg)


def test_parse_date():
    """Test Depot not known yet.

    Verify config parser function for parsing date values.
    """
    broker_config = {'kafka': {'hostname': 'kafka', 'port': 9092, 'topic': 'dirbs'}}
    amnesty_config = {'amnesty_enabled': True,
                      'evaluation_period_end_date': 20180101,
                      'amnesty_period_end_date': 20180202}

    cfg = {'amnesty': amnesty_config, 'region': {'name': 'Country1', 'country_codes': '22'}, 'broker': broker_config}
    app_cfg = AppConfig(**cfg, ignore_env=True)
    assert app_cfg.amnesty_config.amnesty_enabled
    assert app_cfg.amnesty_config.evaluation_period_end_date == datetime.date(2018, 1, 1)
    assert app_cfg.amnesty_config.amnesty_period_end_date == datetime.date(2018, 2, 2)


def test_amnesty_config():
    """Test Depot not known yet.

    Verify amnesty config is parsed correctly.
    """
    amnesty_config = {'amnesty_enabled': True,
                      'evaluation_period_end_date': 20180101,
                      'amnesty_period_end_date': 20180101}

    msg = "The \'amnesty_period_end_date\' must be greater than the \'evaluation_period_end_date\'!"
    cfg = {'amnesty': amnesty_config, 'region': {'name': 'Country1', 'country_codes': '22'}}
    _expect_app_config_failure(config=cfg, expected_message=msg)
