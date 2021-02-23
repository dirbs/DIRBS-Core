"""
Privilege separation unit tests.

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
import json
import copy

from flask import url_for
import pytest
from click.testing import CliRunner

from dirbs.config.db import DBConfig
from dirbs.config.catalog import CatalogConfig
from dirbs.cli.importer import cli as dirbs_import_cli
from dirbs.cli.listgen import cli as dirbs_listgen_cli
from dirbs.cli.classify import cli as dirbs_classify_cli
from dirbs.cli.report import cli as dirbs_report_cli
from dirbs.cli.catalog import cli as dirbs_catalog_cli
from dirbs.cli.prune import cli as dirbs_prune_cli
from dirbs.cli.db import cli as dirbs_db_cli
from dirbs.utils import create_db_connection, DatabaseRoleCheckException
from dirbs.importer.gsma_data_importer import GSMADataImporter
from dirbs.importer.operator_data_importer import OperatorDataImporter
from dirbs.importer.pairing_list_importer import PairingListImporter
from dirbs.importer.registration_list_importer import RegistrationListImporter
from _importer_params import OperatorDataParams, PairListParams, GSMADataParams, RegistrationListParams
from _fixtures import *  # noqa: F403, F401
from _helpers import zip_files_to_tmpdir, get_importer


@pytest.mark.parametrize('db_user', ['dirbs_poweruser_login', 'dirbs_import_operator_user', 'unknown_user'])
def test_db(per_test_postgres, db_user, mocked_config, monkeypatch):
    """Test db commands work with the poweruser security role."""
    monkeypatch.setattr(mocked_config.db_config, 'user', db_user)
    runner = CliRunner()
    if db_user in ['dirbs_poweruser_login', 'dirbs_import_operator_user']:
        result = runner.invoke(dirbs_db_cli, ['check'], obj={'APP_CONFIG': mocked_config})
        # Test whether dirbs-db check passes after schema install
        assert result.exit_code == 0
    else:
        result = runner.invoke(dirbs_db_cli, ['check'], obj={'APP_CONFIG': mocked_config})
        assert result.exit_code != 0


@pytest.mark.parametrize('db_user', ['dirbs_poweruser_login', 'dirbs_import_operator_user'])
def test_prune(per_test_postgres, tmpdir, logger, mocked_statsd, db_user, mocked_config, monkeypatch):
    """Test prune works with the poweruser security role."""
    dsn = per_test_postgres.dsn()
    db_config = DBConfig(ignore_env=True, **dsn)
    with create_db_connection(db_config) as conn, create_db_connection(db_config, autocommit=True) as metadata_conn:
        with get_importer(OperatorDataImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          OperatorDataParams(
                              filename='testData1-operator-operator4-anonymized_20161101_20161130.csv',
                              operator='1',
                              extract=False,
                              perform_leading_zero_check=False,
                              mcc_mnc_pairs=[{'mcc': '111', 'mnc': '04'}],
                              perform_unclean_checks=False,
                              perform_file_daterange_check=False)) as imp:
            imp.import_data()
            conn.commit()

    runner = CliRunner()
    monkeypatch.setattr(mocked_config.db_config, 'user', db_user)
    result = runner.invoke(dirbs_prune_cli, ['triplets'], obj={'APP_CONFIG': mocked_config})
    if db_user in ['dirbs_poweruser_login']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


@pytest.mark.parametrize('classification_data',
                         ['classification_state/listgen_privileges_class_state.csv'],
                         indirect=True)
@pytest.mark.parametrize('db_user', ['dirbs_poweruser_login', 'dirbs_listgen_user', 'dirbs_import_operator_user'])
def test_listgen(per_test_postgres, tmpdir, logger, mocked_statsd, db_user, mocked_config, monkeypatch,
                 classification_data):
    """Test that the dirbs-listgen instance runs without an error."""
    dsn = per_test_postgres.dsn()
    db_config = DBConfig(ignore_env=True, **dsn)
    with create_db_connection(db_config) as conn, create_db_connection(db_config, autocommit=True) as metadata_conn:
        with get_importer(OperatorDataImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          OperatorDataParams(
                              content='date,imei,imsi,msisdn\n'
                                      '20160203,811111013136460,111018001111111,223338000000\n'
                                      '20160203,359000000000000,111015113222222,223355000000\n'
                                      '20160203,357756065985824,111015113333333,223355111111',
                              cc=['22', '74'],
                              mcc_mnc_pairs=[{'mcc': '111', 'mnc': '01'}],
                              operator='operator1',
                              extract=False)) as imp:
            imp.import_data()

        with get_importer(PairingListImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          PairListParams(
                              content='imei,imsi,msisdn\n'
                                      '811111013136460,111018001111111,234555555555550\n'
                                      '359000000000000,111015113222222,234555555555551\n'
                                      '357756065985824,111015113333333,234555555555552')) as imp:
            imp.import_data()

    # Now run listgen as requested user
    runner = CliRunner()
    monkeypatch.setattr(mocked_config.db_config, 'user', db_user)
    output_dir = str(tmpdir)
    result = runner.invoke(dirbs_listgen_cli, [output_dir], obj={'APP_CONFIG': mocked_config})
    if db_user in ['dirbs_poweruser_login', 'dirbs_listgen_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


@pytest.mark.parametrize('db_user', ['dirbs_poweruser_login', 'dirbs_import_operator_user', 'dirbs_listgen_user'])
def test_operator_data_importer(per_test_postgres, tmpdir, db_user, mocked_config, monkeypatch):
    """Test operator import works with the security role created based on abstract role."""
    files_to_zip = ['unittest_data/operator/Foo_Wireless_20160101_20160331.csv']
    zip_files_to_tmpdir(files_to_zip, tmpdir)
    zipped_file_path = str(tmpdir.join('Foo_Wireless_20160101_20160331.zip'))

    # Run dirbs-import using db args from the temp postgres instance
    runner = CliRunner()
    monkeypatch.setattr(mocked_config.db_config, 'user', db_user)
    result = runner.invoke(dirbs_import_cli, ['operator', '--disable-clean-check', '--disable-rat-import',
                                              '--disable-home-check', '--disable-region-check',
                                              'operator1', zipped_file_path],
                           obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_import_operator_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0

    # Re-run to verify data is imported correctly
    result = runner.invoke(dirbs_import_cli, ['operator', '--disable-clean-check', '--disable-rat-import',
                                              '--disable-home-check', '--disable-region-check',
                                              'operator1', zipped_file_path],
                           obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_import_operator_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


@pytest.mark.parametrize('db_user', ['dirbs_poweruser_login', 'dirbs_import_pairing_list_user',
                                     'dirbs_import_operator_user'])
def test_pairing_list_importer(per_test_postgres, tmpdir, db_user, mocked_config, monkeypatch):
    """Test pairing list import works with the security role created based on abstract role."""
    files_to_zip = ['unittest_data/pairing_list/sample_pairinglist.csv']
    zip_files_to_tmpdir(files_to_zip, tmpdir)
    zipped_file_path = str(tmpdir.join('sample_pairinglist.zip'))

    # Run dirbs-import using db args from the temp postgres instance
    runner = CliRunner()
    monkeypatch.setattr(mocked_config.db_config, 'user', db_user)
    result = runner.invoke(dirbs_import_cli, ['pairing_list', zipped_file_path],
                           obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_import_pairing_list_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0

    # Re-run to verify data is imported correctly
    result = runner.invoke(dirbs_import_cli, ['pairing_list', zipped_file_path], obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_import_pairing_list_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


@pytest.mark.parametrize('db_user', ['dirbs_poweruser_login', 'dirbs_import_stolen_list_user',
                                     'dirbs_import_pairing_list_user'])
def test_stolen_list_importer(per_test_postgres, tmpdir, db_user, mocked_config, monkeypatch):
    """Test stolen list import works with the security role created based on abstract role."""
    files_to_zip = ['unittest_data/stolen_list/sample_stolen_list.csv']
    zip_files_to_tmpdir(files_to_zip, tmpdir)
    zipped_file_path = str(tmpdir.join('sample_stolen_list.zip'))

    # Run dirbs-import using db args from the temp postgres instance
    runner = CliRunner()
    monkeypatch.setattr(mocked_config.db_config, 'user', db_user)
    result = runner.invoke(dirbs_import_cli, ['stolen_list', zipped_file_path], obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_import_stolen_list_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0

    # Re-run to verify data is imported correctly
    result = runner.invoke(dirbs_import_cli, ['stolen_list', zipped_file_path], obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_import_stolen_list_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


@pytest.mark.parametrize('db_user', ['dirbs_poweruser_login', 'dirbs_import_registration_list_user',
                                     'dirbs_import_stolen_list_user'])
def test_registration_list_importer(per_test_postgres, tmpdir, db_user, mocked_config, monkeypatch):
    """Test registration list import works with the security role created based on abstract role."""
    files_to_zip = ['unittest_data/registration_list/sample_registration_list.csv']
    zip_files_to_tmpdir(files_to_zip, tmpdir)
    zipped_file_path = str(tmpdir.join('sample_registration_list.zip'))

    # Run dirbs-import using db args from the temp postgres instance
    runner = CliRunner()
    monkeypatch.setattr(mocked_config.db_config, 'user', db_user)
    result = runner.invoke(dirbs_import_cli, ['registration_list', zipped_file_path],
                           obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_import_registration_list_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0

    # Re-run to verify data is imported correctly
    result = runner.invoke(dirbs_import_cli, ['registration_list', zipped_file_path],
                           obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_import_registration_list_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


@pytest.mark.parametrize('db_user', ['dirbs_poweruser_login', 'dirbs_import_golden_list_user',
                                     'dirbs_import_registration_list_user'])
def test_golden_list_importer(per_test_postgres, tmpdir, db_user, mocked_config, monkeypatch):
    """Test golden list import works with the security role created based on abstract role."""
    files_to_zip = ['unittest_data/golden_list/sample_golden_list.csv']
    zip_files_to_tmpdir(files_to_zip, tmpdir)
    zipped_file_path = str(tmpdir.join('sample_golden_list.zip'))

    # Run dirbs-import using db args from the temp postgres instance
    runner = CliRunner()
    monkeypatch.setattr(mocked_config.db_config, 'user', db_user)
    result = runner.invoke(dirbs_import_cli, ['golden_list', zipped_file_path], obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_import_golden_list_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0

    # Re-run to verify data is imported correctly
    result = runner.invoke(dirbs_import_cli, ['golden_list', zipped_file_path], obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_import_golden_list_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


@pytest.mark.parametrize('db_user', ['dirbs_poweruser_login', 'dirbs_import_gsma_user',
                                     'dirbs_import_golden_list_user'])
def test_gsma_data_importer(per_test_postgres, tmpdir, db_user, monkeypatch, mocked_config):
    """Test gsma data import works with the security role created based on abstract role."""
    files_to_zip = ['unittest_data/gsma/sample_gsma_import_list_anonymized.txt']
    zip_files_to_tmpdir(files_to_zip, tmpdir)
    zipped_file_path = str(tmpdir.join('sample_gsma_import_list_anonymized.zip'))

    # Run dirbs-import using db args from the temp postgres instance
    runner = CliRunner()
    monkeypatch.setattr(mocked_config.db_config, 'user', db_user)
    result = runner.invoke(dirbs_import_cli, ['gsma_tac', zipped_file_path], obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_import_gsma_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0

    # Re-run to verify data is imported correctly
    result = runner.invoke(dirbs_import_cli, ['gsma_tac', zipped_file_path], obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_import_gsma_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


@pytest.mark.parametrize('db_user', ['dirbs_poweruser_login', 'dirbs_classify_user', 'dirbs_import_gsma_user'])
def test_classify(per_test_postgres, db_user, tmpdir, logger, mocked_statsd, monkeypatch, mocked_config):
    """Test classify works with the security role created based on abstract role."""
    dsn = per_test_postgres.dsn()
    db_config = DBConfig(ignore_env=True, **dsn)
    with create_db_connection(db_config) as conn, create_db_connection(db_config, autocommit=True) as metadata_conn:
        with get_importer(OperatorDataImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          OperatorDataParams(
                              content='date,imei,imsi,msisdn\n'
                                      '20110101,8888#888622222,123456789012345,123456789012345\n'
                                      '20110101,88888888622222,123456789012345,123456789012345\n'
                                      '20110101,8888888862222209,123456789012345,123456789012345\n'
                                      '20110101,88888862222209**,123456789012345,123456789012345',
                              extract=False,
                              perform_unclean_checks=False,
                              perform_region_checks=False,
                              perform_home_network_check=False,
                              operator='operator1')) as imp:
            imp.import_data()

        with get_importer(GSMADataImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          GSMADataParams(filename='gsma_not_found_anonymized.txt')) as imp:
            imp.import_data()

        with get_importer(RegistrationListImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          RegistrationListParams(filename='sample_registration_list.csv')) as imp:
            imp.import_data()

    # Run dirbs-classify using db args from the temp postgres instance
    runner = CliRunner()
    monkeypatch.setattr(mocked_config.db_config, 'user', db_user)
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check'], obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_classify_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


@pytest.mark.parametrize('db_user', ['dirbs_poweruser_login', 'dirbs_report_user', 'dirbs_classify_user'])
def test_report(per_test_postgres, tmpdir, db_user, logger, mocked_statsd, mocked_config, monkeypatch):
    """Test catalog works with the security role created based on abstract role."""
    dsn = per_test_postgres.dsn()
    db_config = DBConfig(ignore_env=True, **dsn)
    with create_db_connection(db_config) as conn, create_db_connection(db_config, autocommit=True) as metadata_conn:
        with get_importer(OperatorDataImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          OperatorDataParams(
                              filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                              operator='operator1',
                              perform_unclean_checks=False,
                              extract=False)) as imp:
            imp.import_data()

    runner = CliRunner()
    output_dir = str(tmpdir)
    monkeypatch.setattr(mocked_config.db_config, 'user', db_user)
    result = runner.invoke(dirbs_report_cli, ['standard', '--disable-retention-check', '--disable-data-check',
                                              '11', '2016', output_dir], obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_report_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


@pytest.mark.parametrize('db_user', ['dirbs_poweruser_login', 'dirbs_catalog_user', 'dirbs_report_user'])
def test_catalog(per_test_postgres, tmpdir, db_user, mocked_config, monkeypatch):
    """Test catalog works with the security role created based on abstract role."""
    files_to_zip = ['unittest_data/operator/operator1_with_rat_info_20160701_20160731.csv']
    zip_files_to_tmpdir(files_to_zip, tmpdir)

    catalog_config_dict = {
        'prospectors': [
            {
                'file_type': 'operator',
                'paths': [str(tmpdir.join('operator1_with_rat_info_20160701_20160731.zip'))],
                'schema_filename': 'OperatorImportSchema_v2.csvs'
            }
        ],
        'perform_prevalidation': False
    }

    catalog_config = CatalogConfig(ignore_env=True, **catalog_config_dict)
    monkeypatch.setattr(mocked_config, 'catalog_config', catalog_config)

    # Run dirbs-catalog using db args from the temp postgres instance
    runner = CliRunner()
    monkeypatch.setattr(mocked_config.db_config, 'user', db_user)
    result = runner.invoke(dirbs_catalog_cli, obj={'APP_CONFIG': mocked_config})

    if db_user in ['dirbs_poweruser_login', 'dirbs_catalog_user']:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0


@pytest.mark.parametrize('per_test_flask_app', ['dirbs_poweruser_login', 'dirbs_api_user', 'dirbs_catalog_user'],
                         indirect=True)
def test_imei_api(per_test_flask_app, per_test_postgres, logger, mocked_statsd, tmpdir, request, mocked_config,
                  api_version):
    """Test IMEI API call works with the security role created based on abstract role."""
    dsn = per_test_postgres.dsn()
    db_config = DBConfig(ignore_env=True, **dsn)
    with create_db_connection(db_config) as conn, \
            create_db_connection(db_config, autocommit=True) as metadata_conn:
        with get_importer(OperatorDataImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          OperatorDataParams(
                              filename='testData1-operator-operator1-anonymized_20161101_20161130.csv',
                              operator='operator1',
                              perform_unclean_checks=False,
                              extract=False)) as imp:
            imp.import_data()

    current_user = request.node.callspec.params['per_test_flask_app']

    if api_version == 'v1':
        if current_user in ['dirbs_poweruser_login', 'dirbs_api_user']:
            rv = per_test_flask_app.get(url_for('{0}.imei_api'.format(api_version),
                                                imei='388260336982806', include_seen_with=1))
            assert rv.status_code == 200
            assert json.loads(rv.data.decode('utf-8'))['seen_with'] == \
                                                      [{'imsi': '11101400135251', 'msisdn': '22300825684694'},
                                                       {'imsi': '11101400135252', 'msisdn': '22300825684692'}]
            assert json.loads(rv.data.decode('utf-8'))['realtime_checks']['ever_observed_on_network'] is True

        else:
            with pytest.raises(DatabaseRoleCheckException):
                per_test_flask_app.get(url_for('{0}.imei_api'.format(api_version),
                                               imei='388260336982806', include_seen_with=1))
    else:  # api version 2.0
        if current_user in ['dirbs_poweruser_login', 'dirbs_api_user']:
            rv = per_test_flask_app.get(url_for('{0}.imei_get_subscribers_api'.format(api_version),
                                                imei='388260336982806'))
            assert rv.status_code == 200
            data = json.loads(rv.data.decode('utf-8'))
            assert len(data['subscribers']) != 0
            assert data['subscribers'] == [
                {
                    'imsi': '11101400135251',
                    'last_seen': '2016-11-01',
                    'msisdn': '22300825684694'
                },
                {
                    'imsi': '11101400135252',
                    'last_seen': '2016-11-02',
                    'msisdn': '22300825684692'
                }]
        else:
            with pytest.raises(DatabaseRoleCheckException):
                per_test_flask_app.get(url_for('{0}.imei_get_subscribers_api'.format(api_version),
                                               imei='388260336982806'))


@pytest.mark.parametrize('per_test_flask_app', ['dirbs_api_user'],
                         indirect=True)
def test_imei_api_registration_list(per_test_flask_app, per_test_postgres, logger, mocked_statsd, tmpdir, request,
                                    mocked_config, api_version):
    """Test IMEI API call after registration list import."""
    dsn = per_test_postgres.dsn()
    db_config = DBConfig(ignore_env=True, **dsn)
    with create_db_connection(db_config) as conn, \
            create_db_connection(db_config, autocommit=True) as metadata_conn:
        with get_importer(GSMADataImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          GSMADataParams(filename='gsma_dump_small_july_2016.txt')) as imp:
            imp.import_data()

        with get_importer(RegistrationListImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          RegistrationListParams(content='APPROVED_IMEI,make,model,status,model_number,brand_name,'
                                                         'device_type,radio_interface,device_id\n'
                                                         '21260934000003,,,,,,,,1')) as imp:
            imp.import_data()

    if api_version == 'v1':
        rv = per_test_flask_app.get(url_for('{0}.imei_api'.format(api_version), imei='21260934000003'))
        assert rv.status_code == 200
    else:  # api version 2.0
        rv = per_test_flask_app.get(url_for('{0}.imei_get_api'.format(api_version), imei='21260934000003'))
        assert rv.status_code == 200


@pytest.mark.parametrize('per_test_flask_app', ['dirbs_api_user'],
                         indirect=True)
def test_imei_api_pairing_list(per_test_flask_app, per_test_postgres, logger, mocked_statsd, tmpdir, request,
                               mocked_config, api_version):
    """Test IMEI API call after pairing list import."""
    dsn = per_test_postgres.dsn()
    db_config = DBConfig(ignore_env=True, **dsn)
    with create_db_connection(db_config) as conn, \
            create_db_connection(db_config, autocommit=True) as metadata_conn:
        with get_importer(GSMADataImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          GSMADataParams(filename='gsma_dump_small_july_2016.txt')) as imp:
            imp.import_data()

        with get_importer(PairingListImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          PairListParams(
                              content='imei,imsi,msisdn\n'
                                      '811111013136460,111018001111111,222222222222222\n'
                                      '359000000000000,111015113222222,222222222222223\n'
                                      '357756065985824,111015113333333,222222222222224')) as imp:
            imp.import_data()

    if api_version == 'v1':
        rv = per_test_flask_app.get(url_for('{0}.imei_api'.format(api_version), imei='21260934000003'))
        assert rv.status_code == 200
    else:  # api version 2.0
        rv = per_test_flask_app.get(url_for('{0}.imei_get_pairings_api'.format(api_version), imei='21260934000003'))
        assert rv.status_code == 200


@pytest.mark.parametrize('per_test_flask_app', ['dirbs_poweruser_login', 'dirbs_api_user', 'dirbs_catalog_user'],
                         indirect=True)
def test_tac_api(per_test_flask_app, per_test_postgres, logger, mocked_statsd, tmpdir, request, mocked_config,
                 api_version):
    """Test TAC API call works with the security role created based on abstract role."""
    dsn = per_test_postgres.dsn()
    dsn['user'] = 'dirbs_import_gsma_user'
    db_config = DBConfig(ignore_env=True, **dsn)
    with create_db_connection(db_config) as conn, \
            create_db_connection(db_config, autocommit=True) as metadata_conn:
        with get_importer(GSMADataImporter,
                          conn,
                          metadata_conn,
                          db_config,
                          tmpdir,
                          logger,
                          mocked_statsd,
                          GSMADataParams(filename='sample_gsma_import_list_anonymized.txt')) as imp:
            imp.import_data()

    current_user = request.node.callspec.params['per_test_flask_app']

    if api_version == 'v1':
        if current_user in ['dirbs_poweruser_login', 'dirbs_api_user']:
            rv = per_test_flask_app.get(url_for('{0}.tac_api'.format(api_version), tac='01234404'))
            assert rv.status_code == 200
            results = json.loads(rv.data.decode('utf-8'))
            assert results['gsma'] is not None
        else:
            with pytest.raises(DatabaseRoleCheckException):
                per_test_flask_app.get(url_for('{0}.tac_api'.format(api_version), tac='01234404'))
    else:  # api version 2.0
        if current_user in ['dirbs_poweruser_login', 'dirbs_api_user']:
            rv = per_test_flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac='01234404'))
            data = json.loads(rv.data.decode('utf-8'))
            assert data['gsma'] is not None
        else:
            with pytest.raises(DatabaseRoleCheckException):
                per_test_flask_app.get(url_for('{0}.tac_get_api'.format(api_version), tac='01234404'))


@pytest.mark.parametrize('per_test_flask_app', ['dirbs_poweruser_login', 'dirbs_api_user', 'dirbs_catalog_user'],
                         indirect=True)
def test_catalog_api(per_test_flask_app, per_test_postgres, request, api_version):
    """Test catalog API call works with the security role created based on abstract role."""
    current_user = request.node.callspec.params['per_test_flask_app']
    if api_version == 'v1':
        if current_user in ['dirbs_poweruser_login', 'dirbs_api_user']:
            rv = per_test_flask_app.get(url_for('{0}.catalog_api'.format(api_version)))
            assert rv.status_code == 200
        else:
            with pytest.raises(DatabaseRoleCheckException):
                per_test_flask_app.get(url_for('{0}.catalog_api'.format(api_version)))
    else:  # api version 2.0
        if current_user in ['dirbs_poweruser_login', 'dirbs_api_user']:
            rv = per_test_flask_app.get(url_for('{0}.catalog_get_api'.format(api_version)))
            assert rv.status_code == 200
        else:
            with pytest.raises(DatabaseRoleCheckException):
                per_test_flask_app.get(url_for('{0}.catalog_get_api'.format(api_version)))


@pytest.mark.parametrize('per_test_flask_app', ['dirbs_poweruser_login', 'dirbs_api_user', 'dirbs_catalog_user'],
                         indirect=True)
def test_job_metadata_api(per_test_flask_app, per_test_postgres, request, api_version, mocked_config,
                          monkeypatch):
    """Test job_metadata API call works with the security role created based on abstract role."""
    # Run dirbs-classify to generate some metadata
    runner = CliRunner()
    config_copy = copy.deepcopy(mocked_config)
    config_copy.db_config.user = 'dirbs_classify_user'
    result = runner.invoke(dirbs_classify_cli, ['--no-safety-check'], catch_exceptions=False,
                           obj={'APP_CONFIG': config_copy})
    assert result.exit_code == 0
    current_user = request.node.callspec.params['per_test_flask_app']

    if api_version == 'v1':
        if current_user in ['dirbs_poweruser_login', 'dirbs_api_user']:
            rv = per_test_flask_app.get(url_for('{0}.job_metadata_api'.format(api_version)))
            assert rv.status_code == 200
            results = json.loads(rv.data.decode('utf-8'))
            assert results[0]['command'] == 'dirbs-classify'
        else:
            with pytest.raises(DatabaseRoleCheckException):
                per_test_flask_app.get(url_for('{0}.job_metadata_api'.format(api_version)))
    else:  # api version 2.0
        if current_user in ['dirbs_poweruser_login', 'dirbs_api_user']:
            rv = per_test_flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version)))
            assert rv.status_code == 200
        else:
            with pytest.raises(DatabaseRoleCheckException):
                per_test_flask_app.get(url_for('{0}.job_metadata_get_api'.format(api_version)))
