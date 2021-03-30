"""
DIRBS CLI for data import. Common functions for processing command line options.

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

import datetime
import logging
import sys
import os
from functools import wraps
import contextlib
import time

import click
import psycopg2

from dirbs.config import ConfigParser
from dirbs.config.common import ConfigParseException
import dirbs.metadata as metadata
import dirbs.logging
import dirbs.utils as utils


def parse_verbosity_option(f):
    """
    Function to parse the verbosity flag used by all CLI programs.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click option
    """
    def callback(ctx, param, value):
        config = ensure_config(ctx)
        if value is True:
            config.log_config.level = 'debug'

    return click.option('-v',
                        '--verbose',
                        expose_value=False,
                        is_flag=True,
                        help='Print debug console output - file output is unaffected.',
                        callback=callback)(f)


def validate_date(ctx, param, val):
    """
    Function to validate a date string supplied on the command line.

    Arguments:
        ctx -- current cli context
        param -- required param obj
        val -- value to parse as datetime
    Returns:
        date -- acceptable date in format
    """
    if val is None:
        return None

    try:
        d = datetime.datetime.strptime(val, '%Y%m%d')
    except ValueError:
        raise click.BadParameter('Date must be in YYYYMMDD format')

    return d.date()


def parse_statsd_options(f):
    """
    Decorator used to parse all the StatsD command line options and update the config.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click options
    """
    f = _parse_statsd_host(f)
    f = _parse_statsd_port(f)
    f = _parse_statsd_prefix(f)
    return f


def _parse_statsd_host(f):
    """
    Function to override the StatsD host on the command line.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click option
    """
    def callback(ctx, param, value):
        config = ensure_config(ctx)
        if value is not None:
            config.statsd_config.hostname = value
        return value

    return click.option('--statsd-host',
                        expose_value=False,
                        help='The StatsD host to send metrics to.',
                        callback=callback)(f)


def _parse_statsd_port(f):
    """
    Function to override the StatsD port on the command line.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click option
    """
    def callback(ctx, param, value):
        config = ensure_config(ctx)
        if value is not None:
            if value <= 0:
                raise click.BadParameter('StatsD port must be a positive integer')
            config.statsd_config.port = value
        return value

    return click.option('--statsd-port',
                        expose_value=False,
                        type=int,
                        help='The StatsD port to connect to on the configured host.',
                        callback=callback)(f)


def _parse_statsd_prefix(f):
    """
    Function to override the StatsD prefix on the command line.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click option
    """
    def callback(ctx, param, value):
        config = ensure_config(ctx)
        if value is not None:
            config.statsd_config.prefix = value
        return value

    return click.option('--statsd-prefix',
                        expose_value=False,
                        help='The environment prefix to prepend to all StatsD metrics.',
                        callback=callback)(f)


def parse_db_options(f):
    """
    Decorator used to parse all the PostgreSQL command line options and update the config.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click options
    """
    f = _parse_db_host(f)
    f = _parse_db_port(f)
    f = _parse_db_database(f)
    f = _parse_db_user(f)
    f = _parse_db_password(f)
    return f


def _parse_db_host(f):
    """
    Function to override the DB host on the command line.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click option
    """
    def callback(ctx, param, value):
        config = ensure_config(ctx)
        if value is not None:
            config.db_config.host = value
        return value

    return click.option('--db-host',
                        expose_value=False,
                        help='The PostgreSQL DB host to connect to.',
                        callback=callback)(f)


def _parse_db_port(f):
    """
    Function to override the DB port on the command line.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click option
    """
    def callback(ctx, param, value):
        config = ensure_config(ctx)
        if value is not None:
            if value <= 0:
                raise click.BadParameter('Database port must be a positive integer')
            config.db_config.port = value
        return value

    return click.option('--db-port',
                        expose_value=False,
                        type=int,
                        help='The PostgreSQL DB port to connect to.',
                        callback=callback)(f)


def _parse_db_database(f):
    """
    Function to override the DB port on the command line.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click option
    """
    def callback(ctx, param, value):
        config = ensure_config(ctx)
        if value is not None:
            config.db_config.database = value
        return value

    return click.option('--db-name',
                        expose_value=False,
                        help='The PostgreSQL DB database name to connect to.',
                        callback=callback)(f)


def _parse_db_user(f):
    """
    Function to override the DB user on the command line.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click option
    """
    def callback(ctx, param, value):
        config = ensure_config(ctx)
        if value is not None:
            config.db_config.user = value
        return value

    return click.option('--db-user',
                        expose_value=False,
                        help='The PostgreSQL DB database user to connect as.',
                        callback=callback)(f)


def _parse_db_password(f):
    """
    Function to override the DB password on the command line.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click option
    """
    def callback(ctx, param, value):
        config = ensure_config(ctx)
        if value is not None and value is True:
            password = click.prompt('Enter PostgreSQL DB password for user {0}'.format(config.db_config.user),
                                    default=None, hide_input=True)
            config.db_config.password = password
        else:
            env_password = os.environ.get('DIRBS_DB_PASSWORD')
            if env_password is not None:
                config.db_config.password = env_password
        return value

    return click.option('--db-password-prompt',
                        expose_value=False,
                        is_flag=True,
                        default=False,
                        help='If set, will prompt the user for a PostgreSQL password rather than reading from config.',
                        callback=callback)(f)


def parse_multiprocessing_options(f):
    """
    Decorator used to parse all the multiprocessing command line options and update the config.

    Arguments:
         f -- function to decorate
    Returns:
         decorated click option
    """
    f = _parse_max_local_cpus(f)
    f = _parse_max_db_connections(f)
    return f


def _parse_max_local_cpus(f):
    """
    Function to override the number of max local CPUs to use on the command line.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click option
    """
    def callback(ctx, param, value):
        config = ensure_config(ctx)
        if value is not None:
            try:
                config.multiprocessing_config.max_local_cpus = value
            except ValueError as e:
                raise click.BadParameter(str(e))
        return value

    return click.option('--max-local-cpus',
                        expose_value=False,
                        type=int,
                        help='The maximum number of local CPUs to use concurrently during this job.',
                        callback=callback)(f)


def _parse_max_db_connections(f):
    """
    Function to override the number of max database connections on the command line.

    Arguments:
        f -- function to decorate
    Returns:
        decorated click option (--max-db-connections)
    """
    def callback(ctx, param, value):
        config = ensure_config(ctx)
        if value is not None:
            try:
                config.multiprocessing_config.max_db_connections = value
            except ValueError as e:
                raise click.BadParameter(str(e))
        return value

    return click.option('--max-db-connections',
                        expose_value=False,
                        type=int,
                        help='The maximum DB connections to use concurrently during this job.',
                        callback=callback)(f)


def ensure_config(ctx):
    """
    Ensures that the DIRBS config has been created in the context and returns it.

    Arguments:
        ctx -- current cli context
    Returns:
        ensured config object
    """
    if ctx.obj is None:
        ctx.obj = {}
    if ctx.obj.get('APP_CONFIG', None) is None:
        cp = ConfigParser()
        try:
            ctx.obj['APP_CONFIG'] = cp.parse_config(ignore_env=False)
        except ConfigParseException:
            logger = logging.getLogger('dirbs.config')
            logger.critical('Exception encountered during parsing of config (.yml file). '
                            'See console above for details...')
            sys.exit(1)

    return ctx.obj['APP_CONFIG']


def setup_initial_logging(f):
    """
    Decorator intended which initializes the DIRBS logging system to sensible defaults before config can be read.

    This should be used as the first decorator under click.group or click.command to ensure that no log messages
    are missed completely.

    The configure_logging decorator should be applied later after the config has been read so that logging takes
    into account user preferences.

    Arguments:
        f -- function to decorate
    Returns:
        decorated function
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        dirbs.logging.setup_initial_logging()
        return f(*args, **kwargs)
    return decorated


def configure_logging(f):
    """
    Decorator intended which configures the already-init'ed DIRBS logging system based on user preferences.

    This decorator requires the click context so should be included after the click.pass_context decorator. It should
    included after the setup_initial_logging fixture, which is intended to init the logging.

    Arguments:
        f -- function to decorate
    Returns:
        decorated function
    """
    @wraps(f)
    def decorated(ctx, *args, **kwargs):
        config = ensure_config(ctx)
        dirbs.logging.configure_logging(config.log_config)
        return f(ctx, *args, **kwargs)
    return decorated


def ensure_statsd(ctx):
    """
    Ensures that the DIRBS config has been created in the context and returns it.

    Arguments:
        ctx -- current cli context
    Returns:
        ctx.obj['STATSD_CLIENT'] -- statsd obj in context
    """
    if ctx.obj is None:
        ctx.obj = {}
    if ctx.obj.get('STATSD_CLIENT', None) is None:
        config = ensure_config(ctx)
        ctx.obj['STATSD_CLIENT'] = dirbs.logging.StatsClient(config.statsd_config)
    return ctx.obj['STATSD_CLIENT']


def unhandled_exception_handler(f):
    """
    This decorator makes sure that any unhandled exception is logged and metrics are sent.

    This decorator requires the click context so should be included after the click.pass_context decorator. It should
    be the first decorator after the click.pass_context decorator to ensure that as many exceptions are caught as
    possible. If there are exceptions in decorators that appear before this one, they will not be caught and logged.

    This decorator will only catch exceptions in the main process thread -- thread exceptions and subprocess
    exceptions will not be caught, unless some other component makes sure that these are marshalled back to the main
    thread and re-raised. If concurrent.futures is being used, this will always happen if you call result() on the
    future.

    Arguments:
        f -- function to decorate
    Returns:
        decorated -- decorated function
    """
    @contextlib.contextmanager
    def hook_exceptions():
        """Since we catch exceptions here and log, temporarily install a customised hook."""
        old_exception_hook = sys.excepthook
        sys.excepthook = lambda *args, **kwargs: None
        yield sys.excepthook
        sys.excepthook = old_exception_hook

    @wraps(f)
    def decorated(ctx, *args, **kwargs):
        try:
            with hook_exceptions():
                return f(ctx, *args, **kwargs)
        except utils.DatabaseSchemaException as ex:
            logging.getLogger('dirbs.db').critical(ex)
            sys.exit(1)
        except click.exceptions.Abort:
            # If exception was due to a user-generated abort, ignore
            raise
        except Exception:
            try:
                statsd = ensure_statsd(ctx)
                cmd = os.path.basename(sys.argv[0])
                if cmd.startswith('dirbs-'):
                    cmd = cmd.replace('dirbs-', '')
                    statsd.incr('dirbs.exceptions.cli.{0}'.format(cmd))
                else:
                    statsd.incr('dirbs.exceptions.cli.unknown')

                logger = logging.getLogger('dirbs.exception')
                logger.error('DIRBS encountered an uncaught software exception',
                             exc_info=sys.exc_info())

            except:  # noqa: E722
                # In case there was an error logging, print to console
                exctype, value = sys.exc_info()[:2]
                print('{0}: {1}'.format(exctype.__name__, value), file=sys.stderr)
            finally:
                raise

    return decorated


def cli_wrapper(command=None, subcommand=None, logger_name=None, metrics_root=None,
                duration_callback=None, required_role='dirbs_core_poweruser'):  # noqa: C901
    """
    Wrapper around DIRBS CLI programs to try to eliminate boilerplate common code.

    This decorator requires the click context so should be included after the click.pass_context decorator. In most
    cases this should be the last decorator in the chain, directly above the actual CLI function.

    Keyword Arguments:
        command -- command name to wrap to (default None)
        subcommand -- subcommand name to wrap to (default None)
        logger_name -- related dirbs logger name (default None)
        metrics_root -- statsd metrics root instance (default None)
        duration_callback -- duration of the callback (default None)
        required_role -- role required for the operation (default dirbs_core_poweruser)
    Returns:
        decorator -- decorated function
    """
    def decorator(f):
        @wraps(f)
        def decorated(ctx, *args, **kwargs):
            _command = command or os.path.basename(sys.argv[0])
            _logger_name = logger_name or _command.replace('-', '.')
            if callable(metrics_root):
                _metrics_root = metrics_root(ctx, args, **kwargs)
            else:
                _metrics_root = metrics_root
            if _metrics_root is None:
                _metrics_root = _logger_name + '.'
                if subcommand is not None:
                    _metrics_root = _metrics_root + subcommand + '.'

            config = ensure_config(ctx)
            statsd = ensure_statsd(ctx)
            logger = logging.getLogger(_logger_name)
            metrics_run_root = None
            run_id = -1
            metadata_conn = None
            inited_file_logging = False

            try:
                # Store time so that we can track metrics for total listgen time
                st = time.time()

                # Get metadata connection in autocommit mode
                metadata_conn = utils.create_db_connection(config.db_config, autocommit=True)

                try:
                    # Verify DB schema
                    utils.verify_db_schema(metadata_conn, required_role)
                except (utils.DatabaseSchemaException, utils.DatabaseRoleCheckException) as ex:
                    logger.error(str(ex))
                    sys.exit(1)

                # Store metadata and get run_id
                run_id = metadata.store_job_metadata(metadata_conn, _command, logger, job_subcommand=subcommand)

                # Now that we have a run_id, we can setup logging
                if subcommand is not None:
                    log_filename = '{0}_{1}_run_id_{2:d}'.format(command, subcommand, run_id)
                else:
                    log_filename = '{0}_run_id_{1:d}'.format(command, run_id)
                inited_file_logging = dirbs.logging.setup_file_logging(config.log_config, log_filename)

                # Get metrics run root based on run_id
                metrics_run_root = '{0}runs.{1:d}.'.format(_metrics_root, run_id)

                # Validate that any exempted device types occur in the imported GSMA TAC DB
                utils.validate_exempted_device_types(metadata_conn, config)

                # Run the actual decorated function with injected args for config, conn, statsd, logger,
                # run_id and metadata_conn
                with utils.create_db_connection(config.db_config) as conn:
                    # Call CLI function with injected args
                    f(ctx,
                      config,
                      statsd,
                      logger,
                      run_id,
                      conn,
                      metadata_conn,
                      _command,
                      _metrics_root,
                      metrics_run_root,
                      *args,
                      **kwargs)

                # Update the last success timestamp
                statsd.gauge('{0}last_success'.format(_metrics_root), int(time.time()))
                metadata.log_job_success(metadata_conn, _command, run_id)
            except:  # noqa: E722
                # Make sure we track the last failure timestamp for any exception and re-raise
                statsd.gauge('{0}last_failure'.format(_metrics_root), int(time.time()))
                # Log metadata in job_metadata table
                if run_id != -1:
                    metadata.log_job_failure(metadata_conn, _command, run_id, logger)
                raise
            finally:
                # Make sure we init file logging so with date as a last resort so we flush our buffered
                # log output
                if not inited_file_logging:
                    if subcommand is not None:
                        log_filename = '{0}_{1}_run_id_unknown'.format(command, subcommand)
                    else:
                        log_filename = '{0}_run_id_unknown'.format(command)
                    dirbs.logging.setup_file_logging(config.log_config, log_filename)

                # Only track StatsD metrics for run time if we at least retrieved a run id, as this
                # forms part of the key
                dt = int((time.time() - st) * 1000)
                if metrics_run_root is not None:
                    statsd.gauge('{0}runtime.total'.format(metrics_run_root), dt)

                # If there was a duration_callback set, call it here with the calculated dt
                if duration_callback is not None:
                    duration_callback(dt)

                # Cleanup metadata connection (not in with statement)
                if metadata_conn is not None:
                    try:
                        metadata_conn.close()
                    except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
                        logger.error(str(e))

        return decorated
    return decorator


def validate_conditions(ctx, param, condition_names_str):
    """
    Checks if passed value is acceptable for processing worker count.

    Need atleast one thread and the maximum count is capped at number of cores on the system minus one.

    Arguments:
        ctx -- current cli context
        param -- any extra param
        condition_names_str -- comma separated list of condition names
    Returns:
        conditions -- list of acceptable conditions or None
    """
    config = ensure_config(ctx)
    if condition_names_str is None:
        return None
    else:
        condition_names = set([c.strip() for c in condition_names_str.split(',')])
        conditions = []
        for cname in condition_names:
            matched_conditions = [c for c in config.conditions if c.label == cname]
            if len(matched_conditions) == 0:
                raise click.BadParameter('Invalid condition name specified: {0}'.format(cname))
            conditions += matched_conditions
        return conditions


def validate_blocking_conditions(ctx, param, condition_names_str):
    """Checks if the passed value is acceptable for processing.

    This will validate the condition names and return only those which are blocking.

    Arguments:
        ctx -- current cli context
        param -- default required param for callback
        condition_names_str -- comma separated list of condition names
    Returns:
        conditions -- list of blocking conditions or none
    """
    config = ensure_config(ctx)
    if condition_names_str is None:
        return None
    else:
        condition_names = set([c.strip() for c in condition_names_str.split(',')])
        conditions = []
        for cname in condition_names:
            matched_conditions = [c for c in config.conditions if c.label == cname and c.blocking is True]
            if len(matched_conditions) == 0:
                raise click.BadParameter('Invalid condition name specified (should be valid and blocking): {0}'
                                         .format(cname))
            conditions += matched_conditions
        return conditions
