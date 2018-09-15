"""
Script to populate data_catalog and job_metadata table.

- This script TRUNCATES the data_catalog and job_metadata tables before populating them.

- Create pg_pass file ~/.pgpass for db_password i.e. *:*:*:*:<password>

- Specify the number of entries to add to job_metadata and catalog table as command line arguments in this order.
The number of entries for job_metadata must be greater than the data_catalog ones.

Copyright (c) 2018 Qualcomm Technologies, Inc.

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted (subject to the limitations in the disclaimer below) provided that the
following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this list of conditions
   and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions
   and the following disclaimer in the documentation and/or other materials provided with the distribution.
 * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse
   or promote products derived from this software without specific prior written permission.

NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED
BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import json
from collections import namedtuple
import sys
import random
import hashlib
import uuid

from psycopg2.extras import execute_values

from dirbs.utils import create_db_connection
from dirbs.config import DBConfig
from dirbs.cli.catalog import CatalogAttributes


def job_metadata_importer(db_conn, job_metadata_list):
    """Helper function to populate job_metadata data table."""
    full_list = []
    for jm in job_metadata_list:
        job_row = (jm.command, jm.run_id, jm.subcommand, 'dirbs', ' '.join(sys.argv),
                   jm.start_time, jm.status, json.dumps(jm.extra_metadata))
        full_list.append(job_row)

    with db_conn.cursor() as cursor:
        query = """INSERT INTO job_metadata(command, run_id, subcommand, db_user, command_line, start_time, status,
                                            extra_metadata)
                        VALUES %s"""
        execute_values(cursor, query, full_list)
    db_conn.commit()


def populate_data_catalog_table(conn, catalog_list, extra_attributes=None, first_seen='2017-10-31 00:00:00',
                                last_seen='2017-10-31 00:00:00'):
    """Helper function to populate data_catalog table."""
    full_list = []
    for ct in catalog_list:
        catalog_row = (ct.filename, ct.file_type, ct.compressed_size_bytes, ct.modified_time,
                       ct.is_valid_zip, ct.is_valid_format, ct.md5,
                       first_seen, last_seen)
        full_list.append(catalog_row)

    with conn.cursor() as cursor:
        query = """INSERT INTO data_catalog(filename, file_type, compressed_size_bytes, modified_time, is_valid_zip,
                                            is_valid_format, md5, first_seen, last_seen)
                        VALUES %s"""
        execute_values(cursor, query, full_list)
    conn.commit()


def dummy_data_generator(conn, data_catalog_enties, file_importer_map, date='2017-01-01 00:00:00',
                         command='dirbs-import'):
    """Helper function to generate sample data for the unit tests."""
    conn.cursor().execute('TRUNCATE data_catalog')
    conn.cursor().execute('TRUNCATE job_metadata')

    count_run_id = 0
    count_data_catalog_entries = 0

    catalog_list = []
    job_metadata_list = []
    for k, v in file_importer_map.items():
        # create data_catalog table list
        count_data_catalog_entries += 1
        byte_array = k.encode('utf-8')
        md5_hash = hashlib.md5()
        md5_hash.update(byte_array)
        md5_uuid = str(uuid.UUID(md5_hash.hexdigest()))
        if count_data_catalog_entries <= data_catalog_enties:
            f = CatalogAttributes(k, v, date, 46445454332, True, True, md5_uuid, {})
            catalog_list.append(f)

        # create job_metadata table list
        count_run_id += 1
        # FIXME: The function job_metadata_params_defaults_helper no longer exists. Uncomment below
        # status = 'success' if (count_run_id % 2) != 0 else random.choice(['running', 'error'])
        # extra_metadata = {'input_file_md5': md5_uuid}
        # job_metadata_list.append(job_metadata_params_defaults_helper(command=command,
        #                                                              run_id=count_run_id,
        #                                                              subcommand=f.file_type,
        #                                                              status=status,
        #                                                              extra_metadata=extra_metadata))

    # populate job_metadata table
    job_metadata_importer(conn, job_metadata_list)

    # populate data_catalog table
    populate_data_catalog_table(conn, catalog_list)


if __name__ == '__main__':
    """Main function."""
    filename_importer_nt = namedtuple('Filename_importer', ['filename', 'importer'])

    if len(sys.argv) <= 2:
        raise Exception('Please specify the number of entries to add to the job_metadata and catalog_data table')

    job_metadata_entries = sys.argv[1]
    data_catalog_enties = sys.argv[2]

    if data_catalog_enties > job_metadata_entries:
        raise Exception('Number of entries for job_metadata must be greater than data_catalog ones.')

    try:
        int(job_metadata_entries)
    except ValueError:
        raise Exception('Number of job_metadata entries must be a positive int digit')

    try:
        int_data_catalog_enties = int(data_catalog_enties)
    except ValueError:
        raise Exception('Number of data_catalog entries must be a positive int digit')

    a = b = c = n = 0
    file_importer_map = {}
    map_of_occurrences = {'operator': a, 'gsma': b, 'stolen': c}

    while n < int(job_metadata_entries):
        n += 1
        importer_list = ['operator', 'gsma', 'stolen']
        i = random.choice(importer_list)
        map_of_occurrences[i] += 1
        file_importer_map.update({'{0}_{1}_file.zip'.format(map_of_occurrences.get(i), i): i})

    key_map = {
        'database': '<db_name>',
        'user': '<user>',
        'host': '<host>',
        'port': '<port>'
    }

    db_config = DBConfig(ignore_env=False, **key_map)
    dummy_data_generator(create_db_connection(db_config), int_data_catalog_enties, file_importer_map)
