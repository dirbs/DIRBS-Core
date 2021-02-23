"""
Reusable test helpers for unit tests.

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
import tempfile
from os import path
import hashlib
import uuid
import zipfile
import json
import sys
import fnmatch

from psycopg2 import sql
import pytest
from click.testing import CliRunner
from psycopg2.extras import execute_values

import dirbs.metadata as metadata
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
from dirbs.cli.classify import cli as dirbs_classify_cli
from dirbs.config.conditions import ConditionConfig
from dirbs.config.region import OperatorConfig
from dirbs.config.amnesty import AmnestyConfig
from dirbs.config.list_generation import ListGenerationConfig


def job_metadata_importer(*, db_conn, command, run_id, subcommand=None, status,
                          start_time='2017-08-15 01:15:39.54785+00', extra_metadata={}, **other):
    """Helper function for importing job_metadata data."""
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute("""INSERT INTO job_metadata(command, run_id, subcommand, db_user, command_line,
                                                   start_time, status, extra_metadata)
                              VALUES(%s, %s, %s, 'test_user', %s, %s, %s, %s)
                           RETURNING command, run_id""",
                       [command, run_id, subcommand, ' '.join(sys.argv), start_time, status,
                        json.dumps(extra_metadata)])
        job_metadata_pk = [(x.command, x.run_id) for x in cursor.fetchall()]
    return job_metadata_pk[0]


def get_importer(importer_type,
                 db_conn,
                 metadata_db_conn,
                 db_config,
                 tmpdir,
                 logger,
                 statsd,
                 importer_data_params):
    """Helper function for constructor an importer object with the supplied parameters."""
    subcommand_lookup = {
        GSMADataImporter: 'gsma_tac',
        PairingListImporter: 'pairing_list',
        StolenListImporter: 'stolen_list',
        RegistrationListImporter: 'registration_list.',
        GoldenListImporter: 'golden_list',
        OperatorDataImporter: 'operator',
        BarredListImporter: 'barred_list',
        BarredTacListImporter: 'barred_tac_list',
        SubscribersListImporter: 'subscribers_registration_list',
        DeviceAssociationListImporter: 'device_association_list',
        MonitoringListImporter: 'monitoring_list'
    }
    subcommand = subcommand_lookup[importer_type]

    import_id = metadata.store_job_metadata(metadata_db_conn, 'dirbs-import', logger, job_subcommand=subcommand)

    if importer_type == OperatorDataImporter:
        metrics_root = 'dirbs.import.operator.{0}.'.format(importer_data_params.operator)
    else:
        metrics_root = 'dirbs.import.{0}.'.format(subcommand)

    imp = importer_type(conn=db_conn,
                        metadata_conn=metadata_db_conn,
                        import_id=import_id,
                        db_config=db_config,
                        input_filename=importer_data_params.file(tmpdir),
                        logger=logger,
                        statsd=statsd,
                        metrics_root=metrics_root,
                        metrics_run_root='{0}runs.{1}.'.format(metrics_root, import_id),
                        **importer_data_params.kwparams_as_dict())
    return imp


def data_file_to_test(length, imei_imsi=False, imei_custom_header='imei', imei_imsi_msisdn=False):
    """Helper function for constructor an importer data file with customized lenght.

    If the param imei-imsi is set to True, this function will create a datafile containing
    both IMEIs and IMSIs columns.
    If the param is False the file will contain only IMEIs.
    Use imei_custom_header param to customize the header for IMEIs.
    """
    imei_start = 10000000000000
    msisdn_start = 500000000000000
    test_dir = tempfile.mkdtemp()
    data_file_to_test = path.join(test_dir, str(length) + 'test_file.csv')

    if imei_imsi:
        imsi_start = 20000000000000
        with open(data_file_to_test, 'w') as f:
            f.write('imei,imsi\n')

            for i in range(0, length):
                f.write('{0:d},{1:d}\n'.format(imei_start + i, imsi_start + i))

            f.flush()
    elif imei_imsi_msisdn:
        imsi_start = 20000000000000
        with open(data_file_to_test, 'w') as f:
            f.write('imei,imsi,msisdn\n')

            for i in range(0, length):
                f.write('{0:d},{1:d},{2:d}\n'.format(imei_start + i, imsi_start + i, msisdn_start + i))

            f.flush()
    else:
        with open(data_file_to_test, 'w') as f:
            f.write('{0}\n'.format(imei_custom_header))
            # if more than one field, add a comma for null field per each col
            csv_columns = imei_custom_header.split(',')
            for i in range(0, length):
                num_cols = len(csv_columns)
                if num_cols > 1 and 'device_id' in csv_columns:
                    # if registration_list and have device_id
                    f.write('{0:d}{1}dev1\n'.format(imei_start + i, ',' * (num_cols - 1)))
                elif num_cols > 1:
                    f.write('{0:d}{1}\n'.format(imei_start + i, ',' * (num_cols - 1)))
                else:
                    f.write('{0:d}\n'.format(imei_start + i))
            f.flush()

    return data_file_to_test


def expect_success(importer, row_count, conn, logger, log_message=None):
    """Helper function to test if data has been imported succesfully."""
    with conn, conn.cursor() as cursor:
        importer.import_data()
        cursor.execute(sql.SQL('SELECT COUNT(*) FROM {0}').format(sql.Identifier(importer._import_relation_name)))
        result = cursor.fetchone()
        assert result[0] == row_count

    if log_message is not None:
        assert (log_message in logger_stream_contents(logger))


def expect_failure(importer, exc_message=None):
    """Helper function to test if data imported failed."""
    with pytest.raises(Exception) as ex:
        importer.import_data()

    if exc_message is not None:
        assert exc_message in str(ex.value)


def imeis_md5_hashing_uuid(imei, convert_to_uuid=True):
    """Utility method for hashing IMEIs using MD5 and returning their UUID."""
    md5_hash = hashlib.md5()
    md5_hash.update(imei.encode('utf-8'))
    hashed_imei = str(uuid.UUID(md5_hash.hexdigest())) if convert_to_uuid else md5_hash.hexdigest()
    return hashed_imei


def zip_files_to_tmpdir(file_list, tmpdir):
    """Function to zip csv files to the tmpdir."""
    for file in file_list:
        data_dir = os.path.abspath(os.path.dirname(__file__))
        data_file = os.path.join(data_dir, file)
        file_name = os.path.basename(file)
        zipped_data_file_name = file_name[:-3] + 'zip'
        zipped_data_file = str(tmpdir.join(zipped_data_file_name))

        with zipfile.ZipFile(zipped_data_file, 'w') as zip_file:
            zip_file.write(data_file, file_name)


def _logger_stream(logger):
    """Function which can retrieve a handle to the logger stream being used for this test."""
    test_handlers = [handler for handler in logger.handlers if handler.name == 'dirbs.test']
    assert len(test_handlers) == 1
    return test_handlers[0].stream


def logger_stream_contents(logger):
    """Function which knows how to find the content of the stream associated with the logger fixture."""
    return _logger_stream(logger).getvalue()


def logger_stream_reset(logger):
    """Function which knows how to truncate/reset the logger stream being used in the test."""
    return _logger_stream(logger).truncate(0)


def from_cond_dict_list_to_cond_list(conditions_list):
    """Helper function to convert a list of condition dicts to a list of condition instances."""
    cond_list = []
    for cond in conditions_list:
        cond_list.append(ConditionConfig(ignore_env=True, **cond))

    return cond_list


def from_op_dict_list_to_op_list(operators_list):
    """Helper function to convert a list of operators config to list of operator instances."""
    op_list = []
    for op in operators_list:
        op_list.append(OperatorConfig(ignore_env=True, **op))

    return op_list


def from_amnesty_dict_to_amnesty_conf(amnesty_dict):
    """Helper function to convert a dict of amnesty conf to amnesty conf instance."""
    return AmnestyConfig(ignore_env=True, **amnesty_dict)


def from_listgen_dict_to_listgen_conf(listgen_dict):
    """Helper function to convert a dict of listgen conf to listgen instance."""
    return ListGenerationConfig(ignore_env=True, **listgen_dict)


def invoke_cli_classify_with_conditions_helper(conditions_list, mocked_config, monkeypatch,
                                               classify_options=None, curr_date=None, db_conn=None,
                                               expect_success=True, disable_sanity_checks=True):
    """Helper function used to set mocked_config conditon attribute and run the classification command."""
    if not classify_options:
        classify_options = []

    if not curr_date:
        classify_options.extend(['--curr-date', '20161130'])
    else:
        classify_options.extend(['--curr-date', curr_date])

    if disable_sanity_checks:
        classify_options.extend(['--disable-sanity-checks'])

    cond_list = from_cond_dict_list_to_cond_list(conditions_list)

    monkeypatch.setattr(mocked_config, 'conditions', cond_list)
    runner = CliRunner()
    result = runner.invoke(dirbs_classify_cli, classify_options, obj={'APP_CONFIG': mocked_config})
    if expect_success:
        assert result.exit_code == 0
    else:
        assert result.exit_code != 0

    if db_conn:
        matching_imeis = matching_imeis_for_cond_name(db_conn, cond_name=None)
        return matching_imeis


def matching_imeis_for_cond_name(db_conn, *, cond_name):
    """Helper function to return current classifications from a condition name."""
    with db_conn.cursor() as cursor:
        base_sql = sql.SQL("""SELECT imei_norm
                                FROM classification_state
                               WHERE end_date IS NULL
                                     {0}
                            ORDER BY imei_norm, cond_name ASC""")

        if cond_name:
            cond_sql = 'AND cond_name = %s'
            sql_bytes = cursor.mogrify(cond_sql, [cond_name])
            cond_filter_sql = sql.SQL(str(sql_bytes, db_conn.encoding))
        else:
            cond_filter_sql = sql.SQL('')

        cursor.execute(base_sql.format(cond_filter_sql))
        return [x.imei_norm for x in cursor.fetchall()]


def find_file_or_subdir_in_dir(pattern, base_dir, matching_names):
    """Commond code to return file or subdir by pattern."""
    matching_names = fnmatch.filter(matching_names, pattern)
    assert len(matching_names) == 1
    return os.path.join(base_dir, matching_names[0])


def find_file_in_dir(pattern, base_dir, matching_file=True):
    """Returns a file given a pattern for its name and the parent directory path."""
    matching_names = [x for x in os.listdir(base_dir) if os.path.isfile(os.path.join(base_dir, x))]
    return find_file_or_subdir_in_dir(pattern, base_dir, matching_names)


def find_subdirectory_in_dir(pattern, base_dir, matching_file=False):
    """Returns a subdirectory given a pattern for its name and the parent directory path."""
    matching_names = [x for x in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, x))]
    return find_file_or_subdir_in_dir(pattern, base_dir, matching_names)


def import_data(importer, table_name, row_count, conn, logger):
    """Function to import data in to the database."""
    importer.import_data()
    with conn, conn.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM {0}'.format(table_name))
        result = cursor.fetchone()
        assert result[0] == row_count


def insert_into_table(db_conn, field_names_list, field_values_list, table_name):
    """Helper function to insert into a table given a tbl_name, tbl_field_names and tbl_field_values."""
    with db_conn, db_conn.cursor() as cursor:
        execute_values(cursor, """INSERT INTO {tbl_name}({field_names_list})
                                       VALUES %s""".format(tbl_name=table_name,
                                                           field_names_list=', '.join(field_names_list)),
                       field_values_list)


def fetch_tbl_rows(tbl_name, db_conn):
    """Helper function to query a table and return the number of rows and values."""
    with db_conn, db_conn.cursor() as cursor:
        cursor.execute(sql.SQL("""SELECT * FROM {0}""".format(tbl_name)))
        return cursor.rowcount, cursor.fetchall()
