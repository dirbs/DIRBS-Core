"""
Reusable py.test fixtures for unit tests.

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

from os import path
import copy
import logging
from io import StringIO

import pytest
from psycopg2 import sql
import testing.postgresql
from click.testing import CliRunner

from dirbs.utils import create_db_connection
from dirbs.cli.db import cli as dirbs_db_cli
from dirbs.importer.gsma_data_importer import GSMADataImporter
from dirbs.importer.operator_data_importer import OperatorDataImporter
from dirbs.importer.pairing_list_importer import PairingListImporter
from dirbs.importer.stolen_list_importer import StolenListImporter
from dirbs.importer.registration_list_importer import RegistrationListImporter
from dirbs.importer.golden_list_importer import GoldenListImporter
from dirbs.importer.barred_list_importer import BarredListImporter
from dirbs.importer.barred_tac_list_importer import BarredTacListImporter
from dirbs.importer.subscriber_reg_list_importer import SubscribersListImporter
from dirbs.importer.device_association_list_importer import DeviceAssociationListImporter
from dirbs.importer.monitoring_list_importer import MonitoringListImporter
from dirbs.logging import StatsClient
import dirbs.logging
from _helpers import get_importer


@pytest.fixture(scope='session')
def mocked_config():
    """Fixture for mocking DIRBS .yml config so that tests do not depend on a user's config at all."""
    mocked_config_path = path.abspath(path.dirname(__file__) + '/unittest_data/config/config.yml')
    cp = dirbs.config.ConfigParser()
    mocked_config = cp.parse_config(ignore_env=True,
                                    config_paths=[mocked_config_path])
    yield mocked_config


@pytest.fixture(params=['v1', 'v2'])
def api_version(request):
    """Fixture for parameterizing API tests based on the API version."""
    yield request.param


def _flask_impl(mocked_config):
    """Implementation of fixture for injecting a Flask test client into a test function."""
    # Need to import this late as importing this module has sideeffcts on loging that
    from dirbs.api import app

    # We need to save the old URL map and view functions before adding out test_errors route below so
    # we can restore the state of the app during the fixture teardown
    old_url_map = copy.copy(app.url_map)
    old_view_functions = copy.copy(app.view_functions)

    @app.route('/test_errors')
    def test_errors():
        """Dummy route to test uncaught exception handling more easily."""
        raise ValueError('Testing unknown failures')

    app.testing = True
    app.debug = False
    # we need to copy the app config after setting debug(False) and testing(True) in order to have a copy of those
    # app configurations. (At the beginning of this fixture debug is set to true).
    # The fixture will then use this copy to restore those configuration after yielding the client,
    # enforcing debug to be set to False. If debug is set to True, it is possible to
    # get a setup function error.
    old_config = copy.copy(app.config)
    app.config['DIRBS_CONFIG'] = mocked_config
    client = app.test_client()
    ctx = app.test_request_context()
    ctx.push()
    yield client
    app.url_map = old_url_map
    app.view_functions = old_view_functions
    app.config = old_config
    # Reset the _got_first_request flag so that before_first_request funcs trigger every time
    app._got_first_request = False
    ctx.pop()


@pytest.fixture(params=['dirbs_poweruser_login'])
def per_test_flask_app(per_test_postgres, mocked_config, monkeypatch, request):
    """Fixture for injecting a Flask test client into a test function."""
    monkeypatch.setattr(mocked_config.db_config, 'user', request.param)
    yield from _flask_impl(mocked_config)


@pytest.fixture()
def flask_app(postgres, mocked_config):
    """Fixture for injecting a Flask test client into a test function."""
    yield from _flask_impl(mocked_config)


def _postgres_impl(mocked_config):
    """Implementation of fixture to initialise a temporary PostgreSQL instance with a clean DB schema."""
    # The system needs to be set to the C locale other than en_US.UTF8 to assume that,
    # in collation order uppercase will come before lowercase.
    postgresql = testing.postgresql.Postgresql(initdb_args='-U postgres -A trust --lc-collate=C.UTF-8 '
                                                           '--lc-ctype=C.UTF-8')
    dsn = postgresql.dsn()

    # Monkey-patch Postgres config to use temp postgres instance
    for setting in ['database', 'host', 'port', 'user', 'password']:
        setattr(mocked_config.db_config, setting, dsn.get(setting, None))

    # Run dirbs-db install_roles using db args from the temp postgres instance
    runner = CliRunner()
    result = runner.invoke(dirbs_db_cli, ['install_roles'], obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    with create_db_connection(mocked_config.db_config) as conn, conn.cursor() as cursor:
        cursor.execute('CREATE SCHEMA hll;')
        cursor.execute('GRANT USAGE ON SCHEMA hll TO dirbs_core_base;')
        cursor.execute('CREATE EXTENSION hll SCHEMA hll;')
        cursor.execute(sql.SQL('ALTER DATABASE {0} OWNER TO dirbs_core_power_user')
                       .format(sql.Identifier(dsn.get('database'))))

    # Run dirbs-db install using db args from the temp postgres instance
    result = runner.invoke(dirbs_db_cli, ['install'], catch_exceptions=False, obj={'APP_CONFIG': mocked_config})
    assert result.exit_code == 0

    # Create the necessary roles for security tests
    with create_db_connection(mocked_config.db_config) as conn, conn.cursor() as cursor:
        cursor.execute('CREATE ROLE dirbs_import_operator_user IN ROLE dirbs_core_import_operator LOGIN')
        cursor.execute('CREATE ROLE dirbs_import_gsma_user IN ROLE dirbs_core_import_gsma LOGIN')
        cursor.execute('CREATE ROLE dirbs_import_pairing_list_user IN ROLE dirbs_core_import_pairing_list LOGIN')
        cursor.execute('CREATE ROLE dirbs_import_stolen_list_user IN ROLE dirbs_core_import_stolen_list LOGIN')
        cursor.execute('CREATE ROLE dirbs_import_registration_list_user '
                       'IN ROLE dirbs_core_import_registration_list LOGIN')
        cursor.execute('CREATE ROLE dirbs_import_golden_list_user IN ROLE dirbs_core_import_golden_list LOGIN')
        cursor.execute('CREATE ROLE dirbs_import_barred_list_user IN ROLE dirbs_core_import_barred_list LOGIN')
        cursor.execute('CREATE ROLE dirbs_import_barred_tac_list_user IN ROLE dirbs_core_import_barred_tac_list LOGIN')
        cursor.execute('CREATE ROLE dirbs_core_import_subscribers_registration_list_user IN ROLE'
                       ' dirbs_core_import_subscribers_registration_list LOGIN')
        cursor.execute('CREATE ROLE dirbs_import_device_association_list_user IN ROLE '
                       'dirbs_core_import_device_association_list LOGIN')
        cursor.execute('CREATE ROLE dirbs_classify_user IN ROLE dirbs_core_classify LOGIN')
        cursor.execute('CREATE ROLE dirbs_listgen_user IN ROLE dirbs_core_listgen LOGIN')
        cursor.execute('CREATE ROLE dirbs_report_user IN ROLE dirbs_core_report LOGIN')
        cursor.execute('CREATE ROLE dirbs_api_user IN ROLE dirbs_core_api LOGIN')
        cursor.execute('CREATE ROLE dirbs_catalog_user IN ROLE dirbs_core_catalog LOGIN')
        cursor.execute('CREATE ROLE dirbs_poweruser_login IN ROLE dirbs_core_power_user LOGIN')
        cursor.execute('CREATE ROLE unknown_user LOGIN')

    yield postgresql
    postgresql.stop()


@pytest.fixture(scope='session')
def postgres(mocked_config):
    """Fixture to initialise a temporary PostgreSQL instance with a clean DB schema."""
    yield from _postgres_impl(mocked_config)


@pytest.fixture()
def per_test_postgres(mocked_config):
    """Fixture to initialise a temporary PostgreSQL instance with a clean DB schema."""
    yield from _postgres_impl(mocked_config)


@pytest.fixture(autouse=True)
def logger(monkeypatch, mocked_config):
    """Fixture to inject a simple stream logger so that we can assert that log messages are emitted."""
    logger = logging.getLogger('dirbs')
    old_propagate = logger.propagate
    logger.propagate = False

    # We assert that the logger does not have any handlers by this stage. If so, setup_initial_logging has
    # erroneously been called here or we imported a module which called setup_initial_logging at import time in the
    # tests (this is a problem for dirbs.api)
    #
    # Disabling this for now as it is not clear yet the tests are all passed at the moment
    # TODO: investigate and fix
    # assert not logger.hasHandlers()

    # Call setup_initial_logging()
    dirbs.logging.setup_initial_logging()

    # Call configure_logging
    dirbs.logging.configure_logging(mocked_config.log_config)

    # Add an initial handler for our tests and give it a name so we can find it
    stream = StringIO()
    hdlr = logging.StreamHandler(stream=stream)
    hdlr.name = 'dirbs.test'
    logger.addHandler(hdlr)
    # Ignore calls to setup_initial_logging. This means that CLI progras invoked via CliRunner.invoke
    # will not have output visible on result.output. Instead, use logger_stream_contents from _helpers.py
    # and pass in the logger yielded by this fixture to get the output of a CliRunner invocation
    monkeypatch.setattr(dirbs.logging, 'setup_initial_logging', lambda: None)
    # ignore setup_file_logging calls
    monkeypatch.setattr(dirbs.logging, 'setup_file_logging', lambda log_config, filename_root: None)
    # Ignore calls to configure_logging
    monkeypatch.setattr(dirbs.logging, 'configure_logging', lambda log_config: None)

    # Yield dirbslogger to test
    yield logger

    # Delete all handler and filters
    del logger.handlers[:]
    del logger.filters[:]
    logger.propagate = old_propagate


@pytest.fixture()
def mocked_statsd(mocker, mocked_config):
    """Mocked version of the StatsD class for unit test usage."""
    statsd = StatsClient(mocked_config.statsd_config)
    mocked_statsd = mocker.MagicMock(spec=statsd)
    yield mocked_statsd


@pytest.fixture()
def metadata_db_conn(postgres, mocked_config):
    """Fixture to inject a metadata DB connection as a fixture. Only cleans up the job_metadata table."""
    # Create db connection
    conn = create_db_connection(mocked_config.db_config, autocommit=True)
    yield conn
    with conn.cursor() as cursor:
        cursor.execute('TRUNCATE job_metadata')
        cursor.execute('ALTER SEQUENCE job_metadata_run_id_seq RESTART WITH 1')
    conn.close()


@pytest.fixture(params=['dirbs_poweruser_login'])
def db_conn(postgres, mocked_config, request):
    """Fixture to inject a DB connection into a fixture. Cleans up to make sure DB is clean after each test."""
    # Create db connection
    current_db_user = mocked_config.db_config.user
    mocked_config.db_config.user = request.param
    conn = create_db_connection(mocked_config.db_config)
    yield conn
    # Close connection and create new one as the db role might have changed for security tests
    conn.close()
    mocked_config.db_config.user = current_db_user
    conn = create_db_connection(mocked_config.db_config)
    with conn.cursor() as table_cursor, conn.cursor() as truncate_cursor:
        table_cursor.execute('SELECT tablename FROM pg_tables WHERE schemaname = current_schema() '
                             "AND tablename != \'schema_metadata\' AND tablename != \'radio_access_technology_map\'")
        for tblname in table_cursor:
            truncate_cursor.execute(sql.SQL('TRUNCATE {0} CASCADE').format(sql.Identifier(tblname[0])))

        table_cursor.execute('SELECT sequence_name FROM information_schema.sequences '
                             'WHERE sequence_schema = current_schema()')
        for seqname in table_cursor:
            truncate_cursor.execute(sql.SQL('ALTER SEQUENCE {0} RESTART WITH 1').format(sql.Identifier(seqname[0])))

        table_cursor.execute('SELECT matviewname FROM pg_matviews WHERE schemaname = current_schema()')
        for matviewname in table_cursor:
            truncate_cursor.execute(sql.SQL('REFRESH MATERIALIZED VIEW CONCURRENTLY {0}')
                                    .format(sql.Identifier(matviewname[0])))
    # Commit truncations
    conn.commit()
    conn.close()


@pytest.fixture()
def gsma_tac_db_importer(db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd, request):
    """GSMA TAC DB importer fixture. Parameters for importer come in via request.param."""
    gsma_tac_db_params = request.param
    with get_importer(GSMADataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      gsma_tac_db_params) as imp:
        yield imp


@pytest.fixture()
def pairing_list_importer(db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd, request):
    """Pairing list importer fixture. Parameters for importer come in via request.param."""
    pairing_list_imp_params = request.param
    with get_importer(PairingListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      pairing_list_imp_params) as imp:
        yield imp


@pytest.fixture()
def stolen_list_importer(db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd, request):
    """Stolen list importer fixture. Parameters for importer come in via request.param."""
    stolen_list_imp_params = request.param
    with get_importer(StolenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      stolen_list_imp_params) as imp:
        yield imp


@pytest.fixture()
def barred_list_importer(db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd, request):
    """Barred list importer fixture. Parameters for importer come in via request.param."""
    barred_list_imp_params = request.param
    with get_importer(BarredListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      barred_list_imp_params) as imp:
        yield imp


@pytest.fixture()
def monitoring_list_importer(db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd, request):
    """Monitoring list importer fixture. Parameters for importer come in via request.param."""
    monitoring_list_imp_params = request.param
    with get_importer(MonitoringListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      monitoring_list_imp_params) as imp:
        yield imp


@pytest.fixture()
def barred_tac_list_importer(db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd, request):
    """Barred tac list importer fixture. Parameters for importer come in via request.param."""
    barred_tac_list_imp_params = request.param
    with get_importer(BarredTacListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      barred_tac_list_imp_params) as imp:
        yield imp


@pytest.fixture()
def subscribers_list_importer(db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd, request):
    """Subscribers list importer fixture. Parameters for importer come in via request.param."""
    subscribers_list_params = request.param
    with get_importer(SubscribersListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      subscribers_list_params) as imp:
        yield imp


@pytest.fixture()
def device_association_list_importer(db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd, request):
    """Device association list importer fixture. Params for importer come in via request.param."""
    association_list_params = request.param
    with get_importer(DeviceAssociationListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      association_list_params) as imp:
        yield imp


@pytest.fixture()
def registration_list_importer(db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd, request):
    """Registration list importer fixture. Parameters for importer come in via request.param."""
    registration_list_imp_params = request.param
    with get_importer(RegistrationListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      registration_list_imp_params) as imp:
        yield imp


@pytest.fixture()
def golden_list_importer(db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd, request):
    """Golden list importer fixture. Parameters for importer come in via request.param."""
    golden_list_imp_params = request.param
    with get_importer(GoldenListImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      golden_list_imp_params) as imp:
        yield imp


@pytest.fixture()
def operator_data_importer(db_conn, metadata_db_conn, mocked_config, tmpdir, logger, mocked_statsd, request):
    """Operator data importer fixture. Parameters for importer come in via request.param."""
    op_data_params = request.param
    with get_importer(OperatorDataImporter,
                      db_conn,
                      metadata_db_conn,
                      mocked_config.db_config,
                      tmpdir,
                      logger,
                      mocked_statsd,
                      op_data_params) as imp:
        yield imp


@pytest.fixture()
def classification_data(db_conn, request):
    """Fixture to bootstrap DB with known classification_state data."""
    req_class_state_file = request.param
    assert req_class_state_file is not None

    class_state_import_sql = """
        CREATE TEMP TABLE classification_state_temp (LIKE classification_state INCLUDING DEFAULTS);
        ALTER TABLE classification_state_temp ALTER COLUMN run_id SET DEFAULT 1;
        ALTER TABLE classification_state_temp ALTER COLUMN virt_imei_shard DROP NOT NULL;
        COPY classification_state_temp(
            imei_norm,
            cond_name,
            start_date,
            end_date,
            block_date
        )
        FROM
            STDIN WITH CSV HEADER DELIMITER AS ','
    """
    with db_conn, db_conn.cursor() as cur:
        fn = req_class_state_file
        with open(path.join(path.abspath(path.dirname(__file__) + '/unittest_data'), fn)) as f:
            cur.copy_expert(sql=class_state_import_sql, file=f)
            cur.execute("""INSERT INTO classification_state(run_id,
                                                            imei_norm,
                                                            cond_name,
                                                            start_date,
                                                            end_date,
                                                            block_date,
                                                            virt_imei_shard)
                                SELECT run_id,
                                       imei_norm,
                                       cond_name,
                                       start_date,
                                       end_date,
                                       block_date,
                                       calc_virt_imei_shard(imei_norm)
                                  FROM classification_state_temp""")

    yield req_class_state_file
