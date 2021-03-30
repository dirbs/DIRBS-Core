"""
DIRBS CLI for cataloging data received by DIRBS. Installed by setuptools as a dirbs-classify console script.

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
from os import listdir, stat, remove
from os.path import basename, dirname, isfile, isdir, join, exists
from concurrent import futures
from datetime import datetime
import json
import sys
from zipfile import BadZipFile
import zipfile

from psycopg2 import sql
import click

from dirbs.utils import create_db_connection
import dirbs.cli.common as common
import dirbs.metadata as metadata
from dirbs.utils import compute_md5_hash
from dirbs.importer.importer_utils import extract_csv_from_zip, split_file, prevalidate_file
import dirbs.importer.exceptions as exceptions
from dirbs.importer.importer_utils import perform_operator_filename_checks


class CatalogAttributes:
    """Data structure to store catalog attributes."""

    def __init__(self, filename, file_type, modified_time, compressed_size_bytes, is_valid_zip=None,
                 is_valid_format=None, md5=None, extra_attributes=None, uncompressed_size_bytes=None,
                 num_records=None):
        """
        Constructor.

        :param filename: catalog file name
        :param file_type: type of the file
        :param modified_time: modification time
        :param compressed_size_bytes: size in bytes when compressed
        :param is_valid_zip: valid zip check (default None)
        :param is_valid_format: valid format check (default None)
        :param md5: md5 hash of file (default None)
        :param extra_attributes: extra attributes of file (default None)
        :param uncompressed_size_bytes: size in bytes when uncompressed (default None)
        :param num_records: number of records (default None)
        """
        self.filename = filename
        self.file_type = file_type
        self.modified_time = modified_time
        self.compressed_size_bytes = compressed_size_bytes
        self.uncompressed_size_bytes = uncompressed_size_bytes
        self.num_records = num_records
        self.is_valid_zip = is_valid_zip
        self.is_valid_format = is_valid_format
        self.md5 = md5
        if extra_attributes is None:
            self.extra_attributes = {}
        else:
            self.extra_attributes = extra_attributes

    def __eq__(self, other):
        """
        Define the equality behavior.

        :param other:
        :return: bool (True/False)
        """
        if other is None:
            return False
        if self.filename == other.filename and self.file_type == other.file_type \
           and self.modified_time == other.modified_time \
           and self.compressed_size_bytes == other.compressed_size_bytes:
            return True
        else:
            return False

    def __ne__(self, other):
        """
        Define the non-equality behavior.

        :param other:
        :return: bool (negate __eq__())
        """
        return not self.__eq__(other)

    def __hash__(self):
        """Define the hash behavior."""
        return hash((self.filename, self.file_type, self.modified_time, self.compressed_size_bytes))


@click.command()
@common.setup_initial_logging
@click.version_option()
@common.parse_verbosity_option
@common.parse_db_options
@common.parse_statsd_options
@common.parse_multiprocessing_options
@click.pass_context
@common.unhandled_exception_handler
@common.configure_logging
@common.cli_wrapper(command='dirbs-catalog', required_role='dirbs_core_catalog')
def cli(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root):
    """DIRBS script to catalog data files received by DIRBS Core."""
    # Store metadata
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       prospectors=config.catalog_config.prospectors,
                                       perform_prevalidation=config.catalog_config.perform_prevalidation)

    harvested_files = _harvest_files(config.catalog_config.prospectors, logger)
    logger.info('Fetching files in the existing data catalog...')
    cataloged_files = _fetch_catalog_files(config)
    logger.info('Found {0} file(s) in the existing catalog'.format(len(cataloged_files)))

    uncataloged_files = [x for x in harvested_files if x['file_properties'] not in cataloged_files]
    logger.info('Discovered {0} new or modified file(s)'.format(len(uncataloged_files)))

    if len(uncataloged_files) > 0:
        logger.info('Determining catalog attributes for the discovered files...')
        uncataloged_files = _populate_file_properties(config, uncataloged_files, run_id,
                                                      config.catalog_config.perform_prevalidation, logger)
        logger.info('Finished determining catalog attributes for the discovered files')
        logger.info('Updating data catalog with new or modified files...')
        _update_catalog(uncataloged_files, config)
        logger.info('Finished updating data catalog')
    else:
        logger.info('Data catalog is already up-to-date!')


def _harvest_files(prospectors, logger):
    """
    Traverse all specified prospector paths and determine uncataloged files.

    :param prospectors: file prospectors to harvest files
    :param logger: logger instance
    :return: dict list
    """
    discovered_files = []

    # List of files specified explicitly in the prospectors
    absolute_file_paths = [path for x in prospectors for path in x['paths'] if isfile(path)]
    for prospector in prospectors:
        file_type = prospector['file_type']
        for path in prospector['paths']:
            if isdir(path) and exists(path):
                logger.info('Harvesting {0} files from directory...: {1}'.format(file_type, path))
                files = [join(path, f) for f in listdir(path) if isfile(join(path, f))]
                logger.info('Found {0} {1} file(s) at directory: {2}'.format(len(files), file_type, path))
            elif isfile(path):
                logger.info('Harvesting {0} file from path...: {1}'.format(file_type, path))
                files = [path]
            else:
                logger.error('The path specified for {0} file type does not exist: {1}'
                             .format(file_type, path))
                logger.error('Please verify the correct path was specified in the config and '
                             'that the path is readable from the processing machine.')
                sys.exit(1)
            logger.info('Fetching properties for {0} file(s)...'.format(file_type))
            for file_name in files:
                # If file was specified explicitly in prospector; do not index
                # it from other directory paths. This is done to make sure the
                # right file_type and schema is associated with the file.
                if file_name in absolute_file_paths and file_name != path:
                    continue
                if file_name.lower()[-3:] == 'zip':
                    # Stat the file to fetch the file properties and store the relevant fields
                    file_stat_result = stat(file_name)
                    file_properties = CatalogAttributes(basename(file_name), file_type,
                                                        datetime.utcfromtimestamp(file_stat_result.st_mtime),
                                                        file_stat_result.st_size)
                    discovered_files.append({'file_path': file_name, 'file_properties': file_properties,
                                             'schema': prospector['schema']})
                else:
                    logger.warning('Non-zip file found in path and will be ignored: {0}'.format(file_name))
            logger.info('Finished fetching properties for {0} files'.format(file_type))

    logger.info('Harvested a total of {0} file(s)'.format(len(discovered_files)))
    return discovered_files


def _fetch_catalog_files(config):
    """
    Fetch all the cataloged files from the database.

    :param config: dirbs config instance
    :return: list of cataloged files
    """
    with create_db_connection(config.db_config) as conn, conn.cursor() as cursor:
        cursor.execute('SELECT filename, file_type, modified_time, compressed_size_bytes FROM data_catalog')
        cataloged_files = []
        for res in cursor:
            file_properties = CatalogAttributes(res.filename, res.file_type, res.modified_time,
                                                res.compressed_size_bytes)
            cataloged_files.append(file_properties)
        return cataloged_files


def _populate_file_properties(config, file_list, run_id, perform_prevalidation, logger):
    """
    Determine the attributes associated with the file.

    :param config: dirbs config
    :param file_list: list of files
    :param run_id: job run id
    :param perform_prevalidation: bool check to perform validation
    :param logger: dirbs logger instance
    :return: list of files
    """
    uncataloged_files = []
    for file_name in file_list:
        file_properties = file_name['file_properties']
        file_path = file_name['file_path']
        is_valid_zip = None
        is_valid_format = None
        files_to_delete = []
        num_records = None
        uncompressed_size_bytes = None
        try:
            # Validate zip file
            extracted_file = extract_csv_from_zip(file_path)
            is_valid_zip = True
            if perform_prevalidation:
                is_valid_format = _prevalidate_file(config, extracted_file, file_path, file_properties.file_type,
                                                    run_id, file_name['schema'], files_to_delete, logger)
            num_records = sum(1 for _ in extracted_file)
            with zipfile.ZipFile(file_path) as file_test:
                uncompressed_size_bytes = file_test.getinfo(extracted_file.name).file_size

        except BadZipFile as err:
            is_valid_zip = False
            logger.warning('The zip file is invalid: {0}'.format(file_properties.filename))
            logger.warning('Zip check error: {0}'.format(str(err)))
        except exceptions.PrevalidationCheckRawException as err:
            is_valid_format = False
            logger.warning('Pre-validation failed for file: {0} with error: {1}'.format(file_path, str(err)))
        finally:
            logger.debug('Cleanup: deleting intermediate data files...')
            for fn in files_to_delete:
                logger.debug('Deleted intermediate file {0}'.format(fn))
                remove(fn)
            logger.debug('Cleanup: deleted intermediate data files')

        # Compute MD5 hash
        logger.info('Computing MD5 hash of the input file...')
        with open(file_path, 'rb') as f:
            md5 = compute_md5_hash(f)
        logger.info('Computed MD5 hash')

        # Fetch extra attributes (if any)
        extra_attributes = _get_extra_attributes(file_path, file_properties.file_type, logger)
        file_attributes = CatalogAttributes(file_properties.filename, file_properties.file_type,
                                            file_properties.modified_time, file_properties.compressed_size_bytes,
                                            is_valid_zip, is_valid_format, md5, extra_attributes,
                                            uncompressed_size_bytes, num_records)
        uncataloged_files.append(file_attributes)
    return uncataloged_files


def _get_extra_attributes(input_file, file_type, logger):
    """
    Function to perform additional checks if they are defined for the file_type.

    :param input_file: input file name
    :param file_type: type of the file
    :param logger: dirbs logger instance
    :return: dict or None
    """
    if file_type == 'operator':
        try:
            perform_operator_filename_checks(input_file)
            filename_check = True
        except exceptions.FilenameCheckRawException:
            filename_check = False
            logger.warning('Filename check failed on file {0} with error: {1}'.format(input_file, filename_check))
        return {'filename_check': filename_check}
    return None


def _prevalidate_file(config, file_descriptor, file_path, file_type, run_id, schema, files_to_delete, logger):
    """
    Pre-validate the input file using the CSV validator.

    :param config: dirbs config instance
    :param file_descriptor: file descriptor
    :param file_path: file path
    :param file_type: file type
    :param run_id: job run id
    :param schema: csv schema to validate with
    :param files_to_delete: list of files to delete
    :param logger: dirbs logger instance
    :return: bool
    """
    split_file_basename = '{0}_import_{1}_split'.format(file_type, run_id)
    split_files = list(split_file(file_descriptor, config.import_config.batch_size,
                                  dirname(file_path), logger, split_file_basename))
    files_to_delete.extend(split_files)
    num_batches = len(split_files)
    num_validated_batches = 0
    with futures.ThreadPoolExecutor(max_workers=config.multiprocessing_config.max_local_cpus) as prevalidator:
        logger.info('Pre-validating file {0} ({1} pre-validation workers)'
                    .format(basename(file_path), config.multiprocessing_config.max_local_cpus))
        tasks = [prevalidator.submit(prevalidate_file, f, schema) for f in split_files]
        for f in futures.as_completed(tasks):
            f.result()  # will throw exception if this one was thrown in thread
            num_validated_batches += 1
            lvl = logging.DEBUG
            if num_validated_batches % 50 == 0 or num_validated_batches == num_batches:
                lvl = logging.INFO  # Only print every 50 batches at INFO so as not to spam console
            logger.log(lvl, 'Pre-validated {validated_batches} of {num_batches} batches'
                       .format(validated_batches=num_validated_batches, num_batches=num_batches))
        logger.info('Successfully pre-validated file: {0}'.format(basename(file_path)))
        return True


def _update_catalog(uncataloged_files, config):
    """
    Write the new and modified files to the data catalog.

    :param uncataloged_files: list of uncataloged files
    :param config: dirbs config
    """
    with create_db_connection(config.db_config) as conn, conn.cursor() as cursor:
        for f in uncataloged_files:
            cursor.execute(sql.SQL("""INSERT INTO data_catalog AS dc(filename, file_type, modified_time,
                                                               compressed_size_bytes, is_valid_zip, is_valid_format,
                                                               md5, extra_attributes, first_seen, last_seen,
                                                               uncompressed_size_bytes, num_records)
                                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s, %s)
                                      ON CONFLICT (filename, file_type)
                                        DO UPDATE
                                              SET modified_time = %s,
                                                  compressed_size_bytes = %s,
                                                  is_valid_zip = %s,
                                                  is_valid_format = %s,
                                                  md5 = %s,
                                                  extra_attributes = %s,
                                                  last_seen = NOW(),
                                                  uncompressed_size_bytes = %s,
                                                  num_records = %s"""),  # noqa: Q441, Q449
                           [f.filename, f.file_type, f.modified_time, f.compressed_size_bytes,
                            f.is_valid_zip, f.is_valid_format, f.md5, json.dumps(f.extra_attributes),
                            f.uncompressed_size_bytes, f.num_records,
                            f.modified_time, f.compressed_size_bytes, f.is_valid_zip,
                            f.is_valid_format, f.md5, json.dumps(f.extra_attributes), f.uncompressed_size_bytes,
                            f.num_records])
