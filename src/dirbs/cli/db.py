"""
DIRBS CLI for DB schema generation. Installed by setuptools as a dirbs-db console script.

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

import importlib
import logging
import pkgutil
import sys
import datetime
import copy

import click
from psycopg2 import ProgrammingError

import dirbs.utils as utils
import dirbs.partition_utils as partition_utils
import dirbs.cli.common as common
from dirbs import db_schema_version as code_db_schema_version, wl_db_schema_version
import dirbs.metadata as metadata
import dirbs.logging

# We changed our approach to schema management in DIRBS 4.0.0, so refuse to upgrade from schema prior to that
min_schema_version = 19


def _store_job_metadata(config, subcommand):
    """
    Utility method to store metadata about a dirbs-db invocation in the database.

    :param config: dirbs config obj
    :param subcommand: sub-command name
    """
    logger = logging.getLogger('dirbs.db')
    with utils.create_db_connection(config.db_config, autocommit=True) as conn:
        # We can only really store successful database installs/upgrades as we can't store
        # anything in an unknown schema version. Therefore, we can store at the end of the job
        # and mark it as successfully complete immediately
        run_id = metadata.store_job_metadata(conn, 'dirbs-db', logger, job_subcommand=subcommand)
        metadata.log_job_success(conn, 'dirbs-db', run_id)


@click.group(no_args_is_help=False)
@common.setup_initial_logging
@click.version_option()
@common.parse_verbosity_option
@common.parse_db_options
@common.parse_statsd_options
@click.pass_context
@common.configure_logging
def cli(ctx):
    """DIRBS script to intiliaze, configure and upgrade the PostgreSQL schema."""
    config = common.ensure_config(ctx)
    db_config = config.db_config
    logger = logging.getLogger('dirbs.db')
    subcommand = ctx.invoked_subcommand

    dirbs.logging.setup_file_logging(config.log_config, 'dirbs-db_{0}_{1}'
                                     .format(subcommand, datetime.datetime.now().strftime('%Y%m%d')))

    # check subcommand should try and fail regardless of these checks.
    # install_roles subcommand installs these roles so can't do these checks
    if subcommand not in ['install_roles', 'check']:
        with utils.create_db_connection(db_config) as conn:
            try:
                utils.warn_if_db_superuser(conn)
                utils.verify_db_roles_installed(conn)
                utils.verify_db_role_for_job(conn, 'dirbs_core_power_user')
                utils.verify_db_ownership(conn)
                utils.verify_hll_schema(conn)
                if subcommand != 'install':
                    # install subcommand creates the schema, so can't check it here
                    utils.verify_core_schema(conn)
                    utils.verify_db_search_path(conn)
                if config.operational_config.activate_whitelist:
                    utils.notify_if_whitelist_activation()
            except (utils.DatabaseRoleCheckException, utils.DatabaseSchemaException) as ex:
                logger.error(str(ex))
                sys.exit(1)


@cli.command()
@click.pass_context
@common.unhandled_exception_handler
def check(ctx):
    """Checks whether DB schema matches software DB version."""
    config = common.ensure_config(ctx)
    db_config = config.db_config

    logger = logging.getLogger('dirbs.db')
    logger.info('Querying DB schema version for DB %s on host %s',
                db_config.database,
                db_config.host)

    with utils.create_db_connection(db_config) as conn:
        version = utils.query_db_schema_version(conn)
        wl_version = utils.query_wl_db_schema_version(conn)

    logger.info('Code schema version: %d', code_db_schema_version)
    if version is None:
        logger.error('DB has not been clean installed. Maybe this DB pre-dates the version checking?')
        logger.error('DB schema version unknown.')
        # Exit code is used to determine if schema has(exit code:0) or has not(exit code:1) been installed.
        # Non-zero exit code triggers installation of schema at entrypoint of processing container.
        sys.exit(1)
    else:
        logger.info('DB schema version: %s', str(version))
        logger.info('Whitelist schema version: %s', str(wl_version))

        if version < code_db_schema_version:
            logger.error('DB schema older than code.')
        elif version > code_db_schema_version:
            logger.error('DB schema newer than code.')
        else:
            logger.info('Schema versions match between code and DB.')

        if wl_version < wl_db_schema_version:
            logger.error('Whitelist schema older then code.')
        elif wl_version > wl_db_schema_version:
            logger.error('Whitelist schema newer then code.')
        else:
            logger.info('Whitelist Schema versions match between code and DB.')


@cli.command()
@click.pass_context
@common.unhandled_exception_handler
def upgrade(ctx):  # noqa: C901
    """Upgrades the current DB schema to the version supported by this code using migration scripts.

    #TODO: fix suppressed C901 (upgrade is too complex)
    """
    logger = logging.getLogger('dirbs.db')
    config = common.ensure_config(ctx)
    db_config = config.db_config
    needs_analyze = False
    with utils.create_db_connection(db_config) as conn:
        logger.info('Querying DB schema version for DB %s on host %s',
                    db_config.database,
                    db_config.host)
        with conn.cursor() as cur:
            try:
                version = utils.query_db_schema_version(conn)
            except ProgrammingError:
                logger.warn('Could not determine current schema version. Assuming no version')
                version = None

            if version is None:
                logger.error("DB currently not installed or version number could not be determined. Can\'t upgrade")
                sys.exit(1)

            if version < min_schema_version:
                logger.error("Current DB schema is older than DIRBS 4.0.0. Can\'t upgrade")
                sys.exit(1)

            if version > code_db_schema_version:
                logger.error("DB schema newer than code. Can\'t upgrade")
                sys.exit(1)

            if version != code_db_schema_version:
                logger.info('Upgrading DB schema from version %d to %d', version, code_db_schema_version)

                # If we're upgrading, make sure we schedule a full ANALYZE outside the transaction later
                needs_analyze = True

                # Set our role here so that new objects get created with dirbs_core_power_user as owner by default
                with utils.db_role_setter(conn, role_name='dirbs_core_power_user'):
                    for old_version in range(version, code_db_schema_version):
                        new_version = old_version + 1
                        # Check if there is a special migration class, otherwise use standard SQL file
                        try:
                            module_name = 'dirbs.schema_migrators.v{0}_upgrade'.format(new_version)
                            module = importlib.import_module(module_name)
                            logger.info('Running Python migration script: %s', module_name)
                            migrator = module.migrator()
                            migrator.upgrade(conn)
                        except ImportError:
                            script_name = 'sql/migration_scripts/v{0:d}_upgrade.sql'.format(new_version)
                            logger.info('Running SQL migration script: %s', script_name)
                            sql = pkgutil.get_data('dirbs', script_name)
                            cur.execute(sql)

                        # We commit after every version upgrade
                        utils.set_db_schema_version(conn, new_version)
                        conn.commit()

                logger.info('Successfully updated schema - DB schema version is now %d', code_db_schema_version)
                # Can't do anything until we know the schema is the right version
                _store_job_metadata(config, 'upgrade')
            else:
                logger.info('DB schema is already latest version')

            # battle for the whitelist database migrations begins here
            if config.operational_config.activate_whitelist:
                try:
                    wl_version = utils.query_wl_db_schema_version(conn)
                except ProgrammingError:
                    logger.warning('Could not determine current whitelist schema version. Assuming no version')
                    wl_version = None

                if wl_version is None:
                    logger.error('Whitelist DB currently not installed or version number could not be determined. '
                                 "Can\'t upgrade")
                    sys.exit(1)
                if wl_version > wl_db_schema_version:
                    logger.error("Whitelist DB schema newer than code. Can\'t upgrade")
                    sys.exit(1)
                if wl_version != wl_db_schema_version:
                    logger.info('Upgrading Whitelist DB schema version %d to %d', wl_version, wl_db_schema_version)
                    with utils.db_role_setter(conn, role_name='dirbs_core_power_user'):
                        for old_version in range(wl_version, wl_db_schema_version):
                            new_version = old_version + 1
                            try:
                                module_name = 'dirbs.schema_migrators.whitelist.v{0}_upgrade'.format(new_version)
                                module = importlib.import_module(module_name)
                                logger.info('Running Python migration script: %s', module_name)
                                migrator = module.migrator()
                                migrator.upgrade(conn)
                            except ImportError:
                                script_name = 'sql/migration_scripts/whitelist/v{0:d}_upgrade.sql'.format(new_version)
                                logger.info('Running SQL migration script: %s', script_name)
                                sql = pkgutil.get_data('dirbs', script_name)
                                cur.execute(sql)

                            utils.set_wl_db_schema_version(conn, new_version)
                            conn.commit()
                    logger.info('Successfully updated whitelist schema - Whitelist DB schema version is now %d',
                                wl_db_schema_version)
                    # Can't do anything until we know the schema is the right version
                    _store_job_metadata(config, 'wl_upgrade')
                else:
                    logger.info('Whitelist DB schema is already latest version')
            # Schedule a full ANALYZE at the end of an upgrade
            if needs_analyze:
                logger.info('Running ANALYZE of entire database after upgrade...')
                cur.execute('ANALYZE')
                logger.info('Finished running ANALYZE of entire database after upgrade')


@cli.command()
@click.pass_context
@common.unhandled_exception_handler
def install(ctx):
    """Installs latest schema on clean DB instance."""
    logger = logging.getLogger('dirbs.db')
    config = common.ensure_config(ctx)
    db_config = config.db_config
    with utils.create_db_connection(db_config) as conn, conn.cursor() as cur:
        logger.info('Creating initial base DB schema in DB %s on host %s',
                    db_config.database,
                    db_config.host)

        # Check if there is stuff already in there
        cur.execute("""SELECT COUNT(*)
                         FROM pg_class c
                         JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = current_schema()""")
        is_clean = (cur.fetchone()[0] == 0)
        if not is_clean:
            logger.error("Can\'t install latest schema into a non-clean DB")
            logger.error('Instead, use dirbs-db upgrade to upgrade the schema to the latest version')
            sys.exit(1)

        # Set our role here so that new objects get created with dirbs_core_power_user as owner by default
        with utils.db_role_setter(conn, role_name='dirbs_core_power_user'):
            # First we setup the schema, search path etc.
            sql = pkgutil.get_data('dirbs', 'sql/base/on_db_creation.sql')
            cur.execute(sql)

            # Install the base schema for v19 and set current version to 19
            base_schema = 'sql/base/v19_schema.sql'
            logger.info('Restoring base v19 schema from SQL file: %s', base_schema)
            sql = pkgutil.get_data('dirbs', base_schema)
            cur.execute(sql)
            utils.set_db_schema_version(conn, min_schema_version)
            logger.info('Successfully created base v{0:d} schema. Scheduling dirbs-db upgrade...'
                        .format(min_schema_version))

    # Then we call upgrade to complete the process
    rv = 0
    if code_db_schema_version > min_schema_version:
        rv = ctx.invoke(upgrade)
    else:
        # Can't do anything until we know the schema is the right version
        _store_job_metadata(config, 'install')

    return rv


@cli.command(name='install_roles')
@click.pass_context
@common.unhandled_exception_handler
def install_roles(ctx):
    """Creates DIRBS Core PostgreSQL base roles if they don't exist."""
    logger = logging.getLogger('dirbs.db')
    config = common.ensure_config(ctx)
    db_config = copy.copy(config.db_config)
    # Allow install_roles to work even if database doesn't exist by using the postgres DB
    db_config.database = 'postgres'
    with utils.create_db_connection(db_config) as conn, conn.cursor() as cur:
        if not utils.can_db_user_create_roles(conn):
            logger.error('Current PostgreSQL user does not have the CREATEROLE privilege. Please run this command '
                         'as a normal user with the CREATEROLE privilege granted (preferred) or as a superuser')
            sys.exit(1)

        logger.info('Creating DIRBS Core PostgreSQL roles...')
        sql = pkgutil.get_data('dirbs', 'sql/base/roles.sql')
        cur.execute(sql)
        logger.info('Created DIRBS Core PostgreSQL roles')


def num_physical_shards_option(f):
    """
    Function to parse/validate the --num-physical-shards CLI option to dirbs-db repartition.

    :param f: obj
    :return: options obj
    """
    def callback(ctx, param, value):
        if value is not None:
            if value < 1 or value > 100:
                raise click.BadParameter('Number of physical IMEI shards must be between 1 and 100')
        return value

    return click.option('--num-physical-shards',
                        expose_value=True,
                        type=int,
                        help='The number of physical IMEI shards that tables in DIRBS Core should be split into.',
                        callback=callback)(f)


@cli.command()
@click.pass_context
@common.unhandled_exception_handler
@num_physical_shards_option
def repartition(ctx, num_physical_shards):
    """Repartition DIRBS Core tables into a new number of physical IMEI shards."""
    logger = logging.getLogger('dirbs.db')
    config = common.ensure_config(ctx)
    with utils.create_db_connection(config.db_config) as conn, conn.cursor() as cursor:
        logger.info('Repartitioning DB schema in DB %s on host %s into %d physical shards...',
                    config.db_config.database,
                    config.db_config.host,
                    num_physical_shards)

        logger.info('Re-partitioning classification_state table...')
        partition_utils.repartition_classification_state(conn, num_physical_shards=num_physical_shards)
        logger.info('Re-partitioned classification_state table')

        logger.info('Re-partitioning registration_list table...')
        partition_utils.repartition_registration_list(conn, num_physical_shards=num_physical_shards)
        logger.info('Re-partitioned registration_list table')

        logger.info('Re-partitioning stolen_list table...')
        partition_utils.repartition_stolen_list(conn, num_physical_shards=num_physical_shards)
        logger.info('Re-partitioned stolen_list table')

        logger.info('Re-partitioning pairing_list table...')
        partition_utils.repartition_pairing_list(conn, num_physical_shards=num_physical_shards)
        logger.info('Re-partitioned pairing_list table')

        logger.info('Re-partitioning blacklist table...')
        partition_utils.repartition_blacklist(conn, num_physical_shards=num_physical_shards)
        logger.info('Re-partitioned blacklist table')

        logger.info('Re-partitioning notifications_lists table...')
        partition_utils.repartition_notifications_lists(conn, num_physical_shards=num_physical_shards)
        logger.info('Re-partitioned notifications_lists table')

        logger.info('Re-partitioning exceptions_lists table...')
        partition_utils.repartition_exceptions_lists(conn, num_physical_shards=num_physical_shards)
        logger.info('Re-partitioned exceptions_lists table')

        logger.info('Re-partitioning network_imeis table...')
        partition_utils.repartition_network_imeis(conn, num_physical_shards=num_physical_shards)
        logger.info('Re-partitioned network_imeis table')

        logger.info('Re-partitioning monthly_network_triplets tables...')
        partition_utils.repartition_monthly_network_triplets(conn, num_physical_shards=num_physical_shards)
        logger.info('Re-partitioned monthly_network_triplets tables')

        # Update schema metadata table
        cursor.execute('UPDATE schema_metadata SET phys_shards = %s', [num_physical_shards])
