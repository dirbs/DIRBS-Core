"""
DIRBS module for utility classes and functions.

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

import datetime
import logging
import hashlib
import json
import time
import copy
import io
import contextlib

import psycopg2
from psycopg2 import sql
from psycopg2.extras import NamedTupleCursor

from dirbs import db_schema_version as code_db_schema_version
import dirbs.metadata as metadata
from dirbs.config import ConfigParseException


class DatabaseSchemaException(Exception):
    """Custom exception class to indicate there was a problem validating the schema."""

    def __init__(self, msg):
        """Constructor."""
        super().__init__('DB schema check failure: {0}'.format(msg))


class DatabaseRoleCheckException(Exception):
    """Custom exception class to indicate the user does not have the correct roles for this job."""

    def __init__(self, msg):
        """Constructor."""
        super().__init__('DB role check failure: {0}'.format(msg))


class JSONEncoder(json.JSONEncoder):
    """Custom JSONEncoder class which serializes dates in ISO format."""

    def default(self, obj):
        """Overrides JSONEncoder.default."""
        if isinstance(obj, datetime.date):
            return obj.isoformat()

        return JSONEncoder.default(self, obj)


class LoggingNamedTupleCursor(NamedTupleCursor):
    """Named tuple cursor that logs to DIRBS."""

    def __init__(self, *args, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        if self.name is not None:
            # Default itersize to 100000 for named cursors
            self.itersize = 100000

    def execute(self, query, params=None):
        """Overrides NamedTupleCursor.execute."""
        try:
            return super(LoggingNamedTupleCursor, self).execute(query, params)
        finally:
            if self.query is not None:
                logging.getLogger('dirbs.sql').log(logging.DEBUG, str(self.query, encoding='utf-8'))

    def callproc(self, procname, params=None):
        """Overrides NamedTupleCursor.callproc."""
        try:
            return super(LoggingNamedTupleCursor, self).callproc(procname, params)
        finally:
            if self.query is not None:
                logging.getLogger('dirbs.sql').log(logging.DEBUG, str(self.query, encoding='utf-8'))


@contextlib.contextmanager
def db_role_setter(conn, *, role_name):
    """Since we catch exceptions here and log, temporarily install a customised hook."""
    with conn.cursor() as cursor:
        cursor.execute('SHOW ROLE')
        old_role = cursor.fetchone()[0]
        cursor.execute('SET ROLE %s', [role_name])
        yield role_name
        cursor.execute('SET ROLE %s', [old_role])


class CodeProfiler(object):
    """Profile a block of code and store duration."""

    def __enter__(self):
        """Python context manager support for use in with statement (on enter)."""
        self.start = time.time()
        return self

    def __exit__(self, *args):
        """Python context manager support for use in with statement (on exit)."""
        self.duration = int((time.time() - self.start) * 1000)


def compute_md5_hash(file, buf_size=65536):
    """Utility method to generate a md5 hash of file."""
    md5_hash = hashlib.md5()
    while True:
        data = file.read(buf_size)
        if not data:
            break
        md5_hash.update(data)

    return md5_hash.hexdigest()


def cachebusted_filename_from_contents(byte_array):
    """Utility method to generate a unique filename based on the hash of a given content array (of bytes)."""
    return compute_md5_hash(io.BytesIO(byte_array))[:8]


def cli_db_params_from_dsn(dsn, user=None, database=None, port=None, host=None):
    """Convert DB-related command-line arguments from a DSN into a format appropriate for DIRBS CLI commands."""
    db_args = []
    db_args.append('--db-user={0}'.format(user if user is not None else dsn.get('user')))
    db_args.append('--db-name={0}'.format(database if database is not None else dsn.get('database')))
    db_args.append('--db-port={0}'.format(port if port is not None else dsn.get('port')))
    db_args.append('--db-host={0}'.format(host if host is not None else dsn.get('host')))
    return db_args


def create_db_connection(db_config, readonly=False, autocommit=False):
    """Creates a DB connection to the database.

    Imports the config module, which results in the config being read from disk.
    Changes to the config file made after this method has been called will not be read.

    Calling entity should handle connection errors as appropriate.
    """
    logger = logging.getLogger('dirbs.sql')
    logger.debug('Attempting to connect to the database {0} on host {1}'.format(db_config.database, db_config.host))
    # We hard-code 4 minutes idle keepalives, which is fairly aggressive, to avoid disconnections on VPNs, etc.
    conn = psycopg2.connect('{0} keepalives=1 keepalives_idle=240'.format(db_config.connection_string),
                            cursor_factory=LoggingNamedTupleCursor)
    conn.set_session(readonly=readonly, autocommit=autocommit)
    logger.debug('Connection to database successful.')
    return conn


def verify_db_schema(conn, required_role):
    """Function that runs all DB verification checks."""
    warn_if_db_superuser(conn)
    verify_db_roles_installed(conn)
    verify_db_role_for_job(conn, required_role)
    verify_db_schema_version(conn)
    verify_db_ownership(conn)
    verify_hll_schema(conn)
    verify_core_schema(conn)
    verify_db_search_path(conn)


def warn_if_db_superuser(conn):
    """Warn if the current DB user is a PostgreSQL superuser."""
    logger = logging.getLogger('dirbs.db')
    if is_db_user_superuser(conn):
        logger.warn('Running as PostgreSQL superuser -- for security reasons, we recommend running all '
                    'DIRBS tasks as a normal user')


def verify_db_roles_installed(conn):
    """Function used to verify whether roles have been installed in the DB."""
    # The below is not a guaranteed check, but a heuristic
    logger = logging.getLogger('dirbs.db')
    with conn.cursor() as cursor:
        cursor.execute('SELECT 1 AS res FROM pg_roles WHERE rolname = \'dirbs_core_power_user\'')
        if cursor.fetchone() is None:
            logger.error('DIRBS Core roles have not been installed - run \'dirbs-db install_roles\' before '
                         'running \'dirbs-db install\'')
            raise DatabaseSchemaException('DIRBS Core database roles have not been installed')


def verify_db_role_for_job(conn, expected_role):
    """Function used to verify that the current DB user is in the role expected for this job."""
    if not is_db_user_dirbs_role(conn, expected_role):
        role = conn.get_dsn_parameters().get('user')
        raise DatabaseRoleCheckException('Current DB user {0} does not have required role: {1}. To fix this:'
                                         '\n\t1. GRANT {1} TO {0};'.format(role, expected_role))


def verify_db_schema_version(conn):
    """Function used to check whether the DB schema version matches the code schema version."""
    logger = logging.getLogger('dirbs.db')
    version = query_db_schema_version(conn)
    if version != code_db_schema_version:
        if version is None:
            logger.error('DB schema has not been installed via dirbs-db install!')
            raise DatabaseSchemaException('No DB schema installed - perform a dirbs-db install first!')
        else:
            logger.error('DB schema version does not match code!')
            logger.error('Code schema version: %d', code_db_schema_version)
            logger.error('DB schema version: %d', version)
            raise DatabaseSchemaException('Mismatch between code and DB schema versions - perform a dirbs-db upgrade!')


def verify_db_ownership(conn):
    """Function used to check whether DB ownership matches what we expect."""
    logger = logging.getLogger('dirbs.db')
    if query_db_ownership(conn) != 'dirbs_core_power_user':
        logger.error('Database is not owned by the dirbs_core_power_user group! Please the '
                     'following as the current DB owner (whilst logged into the database):'
                     '\n\tALTER DATABASE <database> OWNER TO dirbs_core_power_user;')
        raise DatabaseSchemaException('Incorrect database ownership!')


def verify_core_schema(conn):
    """Function used to check whether Core schema exists and has correct ownership."""
    if not query_schema_existence(conn, 'core'):
        raise DatabaseSchemaException('Missing schema \'core\' in DB. Was dirbs-db install run successfully?')

    if query_schema_ownership(conn, 'core') != 'dirbs_core_power_user':
        raise DatabaseSchemaException('Schema \'core\' is not owned by dirbs_core_power_user!')


def verify_hll_schema(conn):
    """Function used to check whether HLL schema exists and that extension is installed correctly."""
    logger = logging.getLogger('dirbs.db')
    if not query_schema_existence(conn, 'hll'):
        logger.error('Schema \'hll\' does not exist. Please ensure the hll extension is installed and run the '
                     'following as a superuser whilst connected to this DB: '
                     '\n\t1. CREATE SCHEMA hll;'
                     '\n\t2. GRANT USAGE ON SCHEMA hll TO dirbs_core_base;'
                     '\n\t3. CREATE EXTENSION hll SCHEMA hll;')
        raise DatabaseSchemaException('HLL schema not created!')

    # Check if extension installed correctly by looking for hll.hll_print
    with conn.cursor() as cursor:
        try:
            cursor.execute('SELECT pg_get_functiondef(\'hll.hll_print(hll.hll)\'::regprocedure)')
        except psycopg2.ProgrammingError:
            logger.error('The HLL extension is not installed correctly. Please issue the following as a superuser '
                         'whilst connected to this DB: '
                         '\n\tCREATE EXTENSION hll SCHEMA hll;')
            raise DatabaseSchemaException('DB search_path does not include hll or extension not installed!')


def verify_db_search_path(conn):
    """Function used to check whether db_search_path is correct by looking for objects."""
    logger = logging.getLogger('dirbs.db')
    is_search_path_valid = True
    with conn.cursor() as cursor:
        cursor.execute('SELECT to_regclass(\'schema_version\')')
        res = cursor.fetchone()[0]
        if res is None:
            is_search_path_valid = False

        try:
            cursor.execute('SELECT pg_get_functiondef(\'hll_print(hll)\'::regprocedure)')
        except psycopg2.ProgrammingError:
            is_search_path_valid = False

        if not is_search_path_valid:
            logger.error('The search_path for the database is not set correctly. Please issue the following '
                         'whilst connected to this DB: '
                         '\n\tALTER DATABASE <database> SET search_path TO core, hll;')
            raise DatabaseSchemaException('DB search_path not set correctly!')


def query_db_schema_version(conn):
    """Function to fetch the DB version number from the database."""
    logger = logging.getLogger('dirbs.db')
    with conn.cursor() as cur:
        try:
            cur.execute('SELECT MAX(version) FROM schema_version')  # noqa: Q440
            return cur.fetchone()[0]
        except psycopg2.ProgrammingError as ex:
            logger.error(str(ex).strip())
            return None


def set_db_schema_version(conn, new_version):
    """Function to set the DB version number in the database."""
    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM schema_version')
        num_rows = cur.fetchone()[0]
        assert num_rows <= 1
        if num_rows > 0:
            cur.execute('UPDATE schema_version SET version = %s', [new_version])  # noqa: Q440
        else:
            cur.execute('INSERT INTO schema_version(version) VALUES(%s)', [new_version])


def is_db_user_superuser(conn):
    """Function to test whether the current DB user is a PostgreSQL superuser."""
    logger = logging.getLogger('dirbs.db')
    with conn.cursor() as cur:
        cur.execute("""SELECT rolsuper
                         FROM pg_roles
                        WHERE rolname = CURRENT_USER""")
        res = cur.fetchone()
        if res is None:
            logger.warn('Failed to find CURRENT_USER in pg_roles table')
            return False
        return res[0]


def is_db_user_dirbs_role(conn, role_name):
    """Function to test whether the current DB user is in a DIRBS role."""
    with conn.cursor() as cur:
        cur.execute("""SELECT pg_has_role(%s, 'MEMBER')""", [role_name])
        return cur.fetchone()[0]


def is_db_user_dirbs_poweruser(conn):
    """Function to test whether the current DB user is a DIRBS power user."""
    return is_db_user_dirbs_role(conn, 'dirbs_core_power_user')


def can_db_user_create_roles(conn):
    """Function to test whether the current DB user has the CREATEROLE privilege."""
    logger = logging.getLogger('dirbs.db')
    with conn.cursor() as cur:
        cur.execute("""SELECT rolcreaterole
                         FROM pg_roles
                        WHERE rolname = CURRENT_USER""")
        res = cur.fetchone()
        if res is None:
            logger.warn('Failed to find CURRENT_USER in pg_roles table')
            return False
        return res[0]


def query_db_ownership(conn):
    """Function to verify whether the current database ownership is correct."""
    logger = logging.getLogger('dirbs.db')
    with conn.cursor() as cur:
        cur.execute("""SELECT rolname
                         FROM pg_roles
                         JOIN pg_database
                              ON (pg_database.datdba = pg_roles.oid)
                        WHERE datname = current_database()""")
        res = cur.fetchone()
        if res is None:
            logger.warn('Failed to determing DB owner for current_database')
            return None
        return res[0]


def query_schema_existence(conn, schema_name):
    """Function to verify whether the current database schema ownership is correct."""
    with conn.cursor() as cur:
        cur.execute('SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE SCHEMA_NAME = %s)',
                    [schema_name])
        return cur.fetchone().exists


def query_schema_ownership(conn, schema_name):
    """Function to verify whether the current database schema ownership is correct."""
    logger = logging.getLogger('dirbs.db')
    with conn.cursor() as cur:
        cur.execute("""SELECT rolname
                         FROM pg_roles
                         JOIN pg_namespace
                              ON (pg_namespace.nspowner = pg_roles.oid)
                        WHERE nspname = %s""", [schema_name])
        res = cur.fetchone()
        if res is None:
            logger.warn('Failed to determing owner for current_schema')
            return None
        return res[0]


def compute_analysis_end_date(conn, curr_date):
    """Function to get the end of the analysis window based on current operator data."""
    end_date = curr_date
    if end_date is None:
        # If current date is None, set analysis end date as the last day for which operator data exists."""
        with conn.cursor() as cursor:
            monthly_country_child_tbl_list = child_table_names(conn, 'monthly_network_triplets_country')
            year_month_list_in_child_tbls_records = table_invariants_list(conn, monthly_country_child_tbl_list,
                                                                          ['triplet_year', 'triplet_month'])
            year_month_tuple_list = [(x.triplet_year, x.triplet_month) for x in year_month_list_in_child_tbls_records]
            if len(year_month_tuple_list) > 0:
                year_month_tuple_list.sort(key=lambda x: (x[0], x[1]), reverse=True)
                latest_year, latest_month = year_month_tuple_list[0]
                cursor.execute(sql.SQL("""SELECT MAX(last_seen)
                                            FROM monthly_network_triplets_country
                                           WHERE triplet_year = %s
                                             AND triplet_month = %s"""), [latest_year, latest_month])
                end_date = cursor.fetchone()[0]

    # If there was no operator data imported, this can be None
    if end_date is None:
        end_date = datetime.date.today()

    return end_date + datetime.timedelta(days=1)


def hash_string_64bit(s):
    """Basic string hash based on taking an initial prime number and multiplying it by another prime numnber."""
    string_hash = 7
    string_bytes = bytearray(s, 'utf-8')
    for b in string_bytes:
        string_hash = string_hash * 31 + b

    return string_hash % (pow(2, 63) - 1)  # noqa: S001 Make sure it fits into a 64-bit bigint


def child_table_names(conn, parent_name):
    """Return a list of table names for a parent table name."""
    with conn.cursor() as cursor:
        cursor.execute("""SELECT c.relname AS child_tblname
                            FROM pg_inherits
                            JOIN pg_class AS c
                                 ON (c.oid = inhrelid)
                            JOIN pg_class AS p
                                 ON (p.oid = inhparent)
                            JOIN pg_catalog.pg_namespace nc
                                 ON nc.oid = c.relnamespace
                            JOIN pg_catalog.pg_namespace np
                                 ON np.oid = p.relnamespace
                           WHERE p.relname = %s
                             AND np.nspname = current_schema()
                             AND nc.nspname = current_schema()""",
                       [parent_name])
        return [res.child_tblname for res in cursor]


def table_invariants_list(conn, table_names, invariant_col_names):
    """Gets a list of tuples containing the values for common table invariant columns across a list table names."""
    if len(table_names) == 0:
        # Need to return an empty list to avoid doing an empty query and generating an error
        return []

    with conn.cursor() as cursor:
        table_queries = []
        for tblname in table_names:
            table_queries.append(sql.SQL("""SELECT * FROM (SELECT {0} FROM {1} LIMIT 1) {2}""")
                                 .format(sql.SQL(', ').join(map(sql.Identifier, invariant_col_names)),
                                         sql.Identifier(tblname),
                                         sql.Identifier('tmp_{0}'.format(tblname))))
        cursor.execute(sql.SQL(' UNION ALL ').join(table_queries))
        return cursor.fetchall()


def most_recently_run_condition_info(conn, cond_names, successful_only=False):
    """For a list of condition names, return a dict of cond_name -> (run_id, cond_config) for the most recent results.

    If a particular condition has never completed successfully, the value of the dict will be None, unless the
    successful_only parameter is set to True, in which case the key will not exist in the returned dict.
    """
    conditions_to_find = copy.copy(cond_names)
    rv = {}
    # Get list of metadata for dirbs-classify, sorted in reverse order
    job_metadata_list = metadata.query_for_command_runs(conn, 'dirbs-classify')
    for job_metadata in job_metadata_list:
        # Loop back through recent dirbs-classify runs looking for the last time a classification
        # ran successfully. This is indicates in the metadata by the presence of an entry in the matched_imei_counts.
        # This can happen even though the overall dirbs-classify job failed
        extra_metadata = job_metadata.extra_metadata
        metadata_conditions = extra_metadata.get('conditions', {})
        matched_imei_counts = extra_metadata.get('matched_imei_counts', {})
        conditions_lookup = {c['label']: c for c in metadata_conditions}
        for req_cond_name in copy.copy(conditions_to_find):  # We modify the list in the loop, so take a copy
            if req_cond_name in matched_imei_counts:
                # If the name was in matched_imei_counts, it should always be in conditions as well
                rv[req_cond_name] = {
                    'run_id': job_metadata.run_id,
                    'config': conditions_lookup[req_cond_name],
                    'last_successful_run': job_metadata.start_time
                }
                # Remove this req_cond_name from conditions_to_find since we already found latest metadata
                conditions_to_find.remove(req_cond_name)

    # Any items in conditions_to_find at this point are conditions for which we never ran a successful condition
    # run
    if not successful_only:
        for missing_cond_name in conditions_to_find:
            rv[missing_cond_name] = None

    return rv


def filter_imei_list_sql_by_device_type(conn, exempted_device_types, imei_list_sql):
    """Function to return SQL filtering out exempted device types."""
    # If certain device types are exempted, first select the IMEIs passed in imei_list_sql query.
    # These IMEIs are then joined against GSMA TAC db to get their device type.
    # Finally, any IMEIs that belong to exempted device types are excluded.
    return sql.SQL("""SELECT imei_norm
                        FROM (SELECT imei_norm,
                                     SUBSTRING(imei_norm, 1, 8) AS tac
                                FROM ({0}) imeis) imeis_with_tac
                        JOIN gsma_data
                       USING (tac)
                       WHERE device_type NOT IN {1}
                    """).format(sql.SQL(imei_list_sql),
                                sql.Literal(tuple(exempted_device_types))).as_string(conn)


def format_datetime_for_report(timestamp_with_tz):
    """Format the datetime into a string for reporting.

    Replace this function with datetime.isoformat(sep=' ', timespec='seconds') after we update python version to 3.6
    """
    if timestamp_with_tz is not None:
        return timestamp_with_tz.strftime('%Y-%m-%d %X')
    else:
        return None


def validate_exempted_device_types(conn, config):
    """Method to validate exempted device types specified in config."""
    with conn.cursor() as cursor:
        logger = logging.getLogger('dirbs.config')
        exempted_device_types = config.region_config.exempted_device_types
        if len(exempted_device_types) > 0:
            cursor.execute('SELECT DISTINCT device_type FROM gsma_data')
            all_device_types = [x.device_type for x in cursor]
            if len(all_device_types) == 0:
                logger.warning('RegionConfig: Ignoring setting exempted_device_types={0} as GSMA TAC database '
                               'not imported or no device types found.'.format(exempted_device_types))
            else:
                invalid_device_types = set(exempted_device_types) - set(all_device_types)
                if len(invalid_device_types) > 0:
                    msg = 'RegionConfig: exempted_device_types \'{0}\' is/are not valid device type(s). ' \
                          'The valid GSMA device types are: \'{1}\''.format(invalid_device_types, all_device_types)
                    logger.error(msg)
                    raise ConfigParseException(msg)


def log_analysis_window(logger, analysis_start_date, analysis_end_date, start_message='',
                        start_date_inclusive=True, end_date_inclusive=False):
    """Helper function to print out window on used for analysis and list generation using interval notation."""
    start_date_interval_notation = '[' if start_date_inclusive else '('
    end_date_interval_notation = ']' if end_date_inclusive else ')'
    logger.debug('{0} {sd_interval_notation}{start_date}, '
                 '{end_date}{ed_interval_notation}'.format(start_message,
                                                           sd_interval_notation=start_date_interval_notation,
                                                           start_date=analysis_start_date,
                                                           end_date=analysis_end_date,
                                                           ed_interval_notation=end_date_interval_notation))


def registration_list_status_filter_sql():
    """SQL to filter for whitelisted or null registration_list statuses."""
    return sql.SQL('(status IS NULL OR status = \'whitelist\')')


def compute_amnesty_flags(app_config, curr_date):
    """Helper function to determine whether the date falls within amnesty eval or amnesty period."""
    in_amnesty_eval_period = True if app_config.amnesty_config.amnesty_enabled and \
        curr_date <= app_config.amnesty_config.evaluation_period_end_date else False
    in_amnesty_period = True if app_config.amnesty_config.amnesty_enabled and \
        curr_date > app_config.amnesty_config.evaluation_period_end_date and \
        curr_date <= app_config.amnesty_config.amnesty_period_end_date else False
    return in_amnesty_eval_period, in_amnesty_period


def table_exists_sql(any_schema=False):
    """SQL to check for existence of a table. Note that for temp tables, any_schema should be set to True."""
    if not any_schema:
        schema_filter_sql = sql.SQL('AND schemaname = current_schema()')
    else:
        schema_filter_sql = sql.SQL('')

    return sql.SQL("""SELECT EXISTS (SELECT 1
                                       FROM pg_tables
                                      WHERE tablename = %s
                                            {schema_filter_sql})""").format(schema_filter_sql=schema_filter_sql)


def is_table_partitioned(conn, tbl_name):
    """Function to determine whether a table is partitioned."""
    with conn.cursor() as cursor:
        cursor.execute("""SELECT EXISTS (SELECT 1
                                           FROM pg_class
                                           JOIN pg_partitioned_table
                                                ON pg_partitioned_table.partrelid = pg_class.oid
                                          WHERE pg_class.relname = %s)""", [tbl_name])
        return cursor.fetchone().exists
