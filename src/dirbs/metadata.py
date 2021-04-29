"""
DIRBS metadata storage API.

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

import sys
import json
import io
import traceback

import psycopg2
from psycopg2 import sql


def store_job_metadata(conn, job_command, logger, job_subcommand=None):
    """
    Generic method for storing metadata about a job. Returns a run_id.

    Arguments:
        conn: dirbs db connection object
        job_command: job command name currently running
        logger: dirbs logger instance
        job_subcommand: name of the sub command, default is None
    Returns:
        run_id of the job
    """
    # We must be using an autocommit connection. We do not want any job metadata to be rolled back.
    # In general, you want to use a different DB connection for metadata that you use for the rest
    # of your application
    assert conn.autocommit
    with conn.cursor() as cursor:
        cursor.execute("""INSERT INTO job_metadata(command, subcommand, db_user, command_line,
                                                   start_time, status)
                              VALUES(%s, %s, current_user, %s, now(), 'running')
                           RETURNING run_id""",
                       [job_command, job_subcommand, ' '.join(sys.argv)])
        run_id = cursor.fetchone()[0]
        logger.info('Stored metadata about this {0} job with run_id {1:d}'.format(job_command, run_id))
        return run_id


def log_job_success(conn, job_command, run_id):
    """
    Log metadata indicating that the job specified by job_command and run_id finished successfully.

    Arguments:
        conn: dirbs db connection object
        job_command: job command name
        run_id: id of the current job executing this
    """
    # An auto-commit connection must be used to ensure the log is not rolled back due to any reason, it
    # must be preserved.
    assert conn.autocommit
    with conn.cursor() as cursor:
        cursor.execute("""UPDATE job_metadata
                             SET end_time = NOW(),
                                 status = 'success'
                           WHERE command = %s
                             AND run_id = %s""",
                       [job_command, run_id])
        assert cursor.rowcount == 1


def log_job_failure(conn, job_command, run_id, logger):
    """
    Log metadata indicating that the job specified by job_command and run_id failed to complete.

    Arguments:
        conn: dirbs db connection object
        job_command: command name of the currently running job
        run_id: id of the currently running job
        logger: dirbs logger instance
    Raises:
        psycopg2.InterfaceError, psycopg2.OperationalError: if anything goes wrong while logging the activity
    """
    # An auto-commit connection must be used to ensure the log is not rolled back due to any reason, it
    # must be preserved.
    assert conn.autocommit
    sio = io.StringIO()
    etype, value, tb = sys.exc_info()
    traceback.print_exception(etype, value, tb, None, sio)
    exception_string = sio.getvalue()
    sio.close()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""UPDATE job_metadata
                                 SET end_time = NOW(),
                                     status = 'error',
                                     exception_info = %s
                               WHERE command = %s
                                 AND run_id = %s""",
                           [exception_string, job_command, run_id])
            assert cursor.rowcount == 1
    except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
        logger.error('Failed to log job failure due to DB exception: {0}'.format(str(e)))


def add_optional_job_metadata(conn, job_command, run_id, **new_metadata):
    """
    Log additional JSON metadata about this job but leave core fields unchanged.

    Arguments:
        conn: dirbs db connection object
        job_command: name of the command for current job
        run_id: id of the currently running job
        new_metadata: key-value pairs containing optional metadata to store about the job
    """
    # An auto-commit connection must be used to ensure the log is not rolled back due to any reason, it
    # must be preserved.
    assert conn.autocommit
    with conn.cursor() as cursor:
        cursor.execute("""UPDATE job_metadata
                             SET extra_metadata = extra_metadata || %s
                           WHERE command = %s
                             AND run_id = %s""",
                       [json.dumps(new_metadata), job_command, run_id])
        assert cursor.rowcount == 1


def add_time_metadata(conn, job_command, run_id, path):
    """
    Log additional JSON metadata about this job but leave core fields unchanged.

    Arguments:
        conn: dirbs db connection object
        job_command: name of the command for the job
        run_id: id of the currently running job
        path: No idea about it, might be the path to the file generated by the job
    """
    # An auto-commit connection must be used to ensure the log is not rolled back due to any reason, it
    # must be preserved.
    assert conn.autocommit
    with conn.cursor() as cursor:
        cursor.execute("""UPDATE job_metadata
                             SET extra_metadata = jsonb_set(extra_metadata, %s, to_jsonb(NOW()))
                           WHERE command = %s
                             AND run_id = %s""",
                       [path, job_command, run_id])
        assert cursor.rowcount == 1


def query_for_command_runs(conn, job_command, subcommand=None, successful_only=False, run_id=None):
    """
    Get all the metadata for all the invocations of a job commands, sorted most recent runs first.

    Arguments:
        conn: dirbs db connection object
        job_command: name of the command for the job
        subcommand: name of the sub-command used, default is None
        successful_only: bool to filter only successful jobs, default is False means it will fetch all of them
        run_id: id of the currently running job
    Returns:
        psycopg2 results object containing all the results based on the argument given
    """
    with conn.cursor() as cursor:
        if successful_only:
            status_filter_sql = sql.SQL("status = \'success\'")
        else:
            status_filter_sql = sql.SQL('TRUE')

        query_params = [job_command]
        if subcommand:
            subcommand_filter_sql = sql.SQL('AND subcommand = %s')
            query_params.append(subcommand)
        else:
            subcommand_filter_sql = sql.SQL('')

        if run_id:
            run_id_filter_sql = sql.SQL('AND run_id = %s')
            query_params.append(run_id)
        else:
            run_id_filter_sql = sql.SQL('')

        cursor.execute(sql.SQL("""SELECT *
                                    FROM job_metadata
                                   WHERE command = %s
                                     AND {status_filter_sql}
                                         {subcommand_filter_sql}
                                         {run_id_filter_sql}
                                ORDER BY start_time DESC""").format(status_filter_sql=status_filter_sql,
                                                                    subcommand_filter_sql=subcommand_filter_sql,
                                                                    run_id_filter_sql=run_id_filter_sql),
                       query_params)
        return cursor.fetchall()


def job_start_time_by_run_id(conn, run_id, successful_only=False):
    """
    Get the job start time for a job corresponding to the passed run_id.

    Arguments:
        conn: dirbs db connection object
        run_id: id of the job to filter on
        successful_only: bool to filter successful jobs only, default is False
    Returns:
        job start time if exists else none
    """
    with conn.cursor() as cursor:
        if successful_only:
            status_filter_sql = sql.SQL("status = \'success\'")
        else:
            status_filter_sql = sql.SQL('TRUE')

        cursor.execute(sql.SQL("""SELECT start_time
                                    FROM job_metadata
                                   WHERE run_id = %s
                                     AND {status_filter_sql}
                                ORDER BY start_time DESC""").format(status_filter_sql=status_filter_sql),
                       [run_id])
        jt = cursor.fetchone()
        return jt.start_time if jt else None


def most_recent_job_start_time_by_command(conn, command, subcommand=None, successful_only=False):
    """
    Get the start_time of the most recent successful job for the importer.

    Arguments:
        conn: dirbs db connection object
        command: name of the command
        subcommand: name of the sub-command, default is None
        successful_only: bool if true only successful jobs will be filtered, default is False
    Returns:
        start time of the most recent job if exists else None
    """
    job_for_command_runs_list = query_for_command_runs(conn,
                                                       command,
                                                       subcommand=subcommand,
                                                       successful_only=successful_only)
    if not job_for_command_runs_list:
        return None
    return job_for_command_runs_list[0].start_time
