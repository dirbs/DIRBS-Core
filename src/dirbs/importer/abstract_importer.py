"""
Common code for importing data sets into DIRBS Core.

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
import copy
import zipfile
import time
from enum import Enum
import threading
from concurrent import futures
from collections import defaultdict
import logging

from psycopg2 import sql

import dirbs.importer.exceptions as exceptions
from dirbs.utils import create_db_connection, CodeProfiler, hash_string_64bit, compute_md5_hash, db_role_setter
import dirbs.metadata as metadata
from dirbs.importer.importer_utils import extract_csv_from_zip, split_file, prevalidate_file
import dirbs.partition_utils as partition_utils


class UploadPipelineJobType(Enum):
    """Enum for upload pipeline job type."""

    PREVALIDATE = 1
    UPLOAD = 2


class AbstractImporter:
    """Base class for all data importers in DIRBS Core."""

    # To fix Python 3.5 reflected bug in importer
    # https://bugs.python.org/issue22995
    _thread_local_storage = threading.local()

    def __init__(self, conn, metadata_conn, import_id, metrics_root, metrics_run_root,
                 db_config, input_filename, logger, statsd,
                 prevalidator_path='/opt/validator/bin/validate', prevalidator_schema_path='/opt/dirbs/etc/schema',
                 batch_size=100000, expected_suffix='.csv', extract=True, no_cleanup=False, extract_dir=None,
                 max_db_connections=1, max_local_cpus=1):
        """
        Constructor.

        :param conn: database connection
        :param metadata_conn: database connection for inserting job metadata
        :param import_id: id of the import job
        :param metrics_root:
        :param metrics_run_root:
        :param db_config: database config obj
        :param input_filename: name of file to be imported
        :param logger: dirbs logger obj
        :param statsd: statsd obj
        :param prevalidator_path: path to csv validator (default /opt/validator/bin/validate)
        :param prevalidator_schema_path: path to the validation schema (default /opt/dirbs/etc/schema)
        :param batch_size: batch size (default 100000)
        :param expected_suffix: expected file suffix (default .csv)
        :param extract: flag to extract file (default True)
        :param no_cleanup: flag to stop cleanup operation (default False)
        :param extract_dir: directory path to extract files (default None)
        :param max_db_connections: max number of database connection for this job (default 1)
        :param max_local_cpus: max number of local cpu to be used (default 1)
        """
        assert import_id != -1
        self.import_id = import_id
        self._conn = conn
        self._metadata_conn = metadata_conn
        self._metrics_root = metrics_root
        self._metrics_import_root = metrics_run_root
        self._db_config = db_config
        self._filename = input_filename
        self._logger = logger
        self._statsd = statsd
        self._validator = prevalidator_path
        self._validation_schema_dir = prevalidator_schema_path
        self._batch_size = batch_size
        self._expected_suffix = expected_suffix
        self._extract = extract
        self._no_cleanup = no_cleanup
        self._max_db_connections = max_db_connections
        self._max_local_cpus = max_local_cpus
        self._data_length = -1
        self._was_entered = False
        self._need_previous_count_for_stats = True
        self._files_to_delete = []
        self._tables_to_cleanup_list = []

        if extract_dir is None:
            self._extract_dir = os.path.dirname(self._filename)
        else:
            self._extract_dir = extract_dir

    def __enter__(self):
        """Context manager support (with statement)."""
        self._was_entered = True
        if not self._acquire_import_lock():
            raise exceptions.ImportLockException('Could not acquire lock for this type of import. '
                                                 'Are there any other imports of this type running at the moment?')
        return self

    def __exit__(self, exc_type, value, traceback):
        """Context manager support (with statement)."""
        try:
            self._was_entered = False
            self._do_final_cleanup()
        except Exception as e:
            self._logger.error('Failed to do final cleanup due to exception: {0}'.format(str(e)))

    def __getstate__(self):
        """Custom function to allow use of this class in ProcessPoolExecutor."""
        state = self.__dict__.copy()
        # We can't and shouldn't use _conn, _metadata_conn, _statsd and _logger in a separate process
        del state['_conn']
        del state['_metadata_conn']
        del state['_statsd']
        del state['_logger']
        return state

    def __setstate__(self, state):
        """
        Custom function to allow use of this class in ProcessPoolExecutor.

        :param state: current state
        """
        self.__dict__.update(state)

    @property
    def staging_row_count(self):
        """Count for valid rows in staging table for this import."""
        assert self._data_length != -1 and 'No data imported yet!'
        return self._data_length

    @property
    def _import_type(self):
        """String defining type of import."""
        raise NotImplementedError('Should be implemented')

    @property
    def _schema_file(self):
        """Filename of CSV schema to use for this type of import during pre-validation."""
        raise NotImplementedError('Should be implemented')

    @property
    def _import_relation_name(self):
        """Name of the database table to write data to."""
        raise NotImplementedError('Should be implemented')

    @property
    def _import_metadata(self):
        """Set of import metadata to store for this import."""
        return {
            'input_file': os.path.abspath(self._filename),
            'batch_size': self._batch_size,
            'performance_timing': {}
        }

    @property
    def _metrics_failures_root(self):
        """Root key to use for metrics for this import."""
        return '{0}validation_failures.'.format(self._metrics_root)

    @property
    def _staging_tbl_name(self):
        """Name for the staging table to use for this import."""
        return 'staging_{0}_import_{1}'.format(self._import_type, self.import_id)

    @property
    def _staging_tbl_identifier(self):
        """Staging table name identified."""
        return sql.Identifier(self._staging_tbl_name)

    @property
    def _owner_role_name(self):
        """Role that should be made the owner of any new table."""
        raise NotImplementedError('Should be implemented')

    @property
    def _supports_imei_shards(self):
        """Property indicating whether this type of import supports/requires sharding by IMEI."""
        return False

    def _time_component_perf(self, key, fn, *fnargs, **fnkwargs):
        """Helper function to time the performance of a function."""
        component_key = '{0}import_time.components.{1}'.format(self._metrics_import_root, key)
        perf_metadata_path = '{{performance_timing,{0}_start}}'.format(key)
        metadata.add_time_metadata(self._metadata_conn, 'dirbs-import', self.import_id, perf_metadata_path)
        with CodeProfiler() as cp:
            rv = fn(*fnargs, **fnkwargs)
        self._statsd.gauge(component_key, cp.duration)
        perf_metadata_path = '{{performance_timing, {0}_end}}'.format(key)
        metadata.add_time_metadata(self._metadata_conn, 'dirbs-import', self.import_id, perf_metadata_path)
        return rv

    def import_data(self):
        """Main import function that checks files and validates the contents before importing to the database."""
        assert self._was_entered and 'Attempting to import with an importer outside of a with statement!'
        try:
            # Store time so that we can track metrics for total listgen time
            st = time.time()

            # Store initial metadata
            metadata.add_optional_job_metadata(self._metadata_conn,
                                               'dirbs-import',
                                               self.import_id,
                                               **self._import_metadata)

            # Log initial message
            self._logger.info("Importing {0} data from file \'{1}\'"
                              .format(self._import_type, self._filename))

            # Init staging table (commit afterwards to ensure other processes can see table)
            with self._conn:
                self._time_component_perf('init_staging', self._init_staging_table)
                if self._supports_imei_shards:
                    self._time_component_perf('init_staging_shards', self._init_staging_table_shards)

            # Compute MD5 hash
            self._time_component_perf('compute_md5', self._compute_md5_hash)
            # Now do extract -> split -> preprocess -> prevalidate -> upload pipeline
            self._time_component_perf('upload_pipeline', self._upload_pipeline)
            # ANALYZE staging table after upload
            self._time_component_perf('analyze_staging', self._analyze_staging_table)
            # Run binary (yes/no) validation checks that operator on "raw" data (prior to post-processing)
            self._time_component_perf('validation_binary_checks_raw', self._validate_binary_checks_raw)
            # Post-process staging table
            self._time_component_perf('postprocess_staging', self._postprocess_staging_data)
            # Run binary (yes/no) validation checks
            self._time_component_perf('validation_binary_checks', self._validate_binary_checks)
            # Run row threshold validation checks
            self._time_component_perf('validation_threshold_checks', self._validate_threshold_checks)
            # Run validation checks based on historic data
            self._time_component_perf('validation_historical_checks', self._validate_historical_checks)
            # Copy data from the staging table
            rows_before = -1  # Sentinel value
            if self._need_previous_count_for_stats:
                rows_before = self.row_count
            rows_inserted, rows_updated, row_deleted = \
                self._time_component_perf('copy_from_staging', self._copy_staging_data)
            # Output import stats
            self._time_component_perf('output_stats', self._output_stats, rows_before, rows_inserted, rows_updated,
                                      row_deleted)

        finally:
            dt = int((time.time() - st) * 1000)
            self._log_normalized_import_time_metrics(dt)

    @property
    def _import_lock_key(self):
        """String Key for the advisory lock to guard against multiple concurrent imports of the same type.

        Subclasses should override if they want to allow concurrent imports. For example,
        the operator data importer allows multiples to happen as long as they are for different operators.
        """
        return hash_string_64bit(self._import_type)

    def _acquire_import_lock(self):
        """Acquires a PostgreSQL advisory lock to stop multiple imports of the same type happening simultaneously."""
        with self._conn.cursor() as cursor:
            cursor.execute('SELECT pg_try_advisory_lock(%s::BIGINT)', [self._import_lock_key])
            return cursor.fetchone()[0]

    def _release_import_lock(self):
        """Explicitly release the import lock.

        It is automatically released at the end of the current transaction, but releasing it explicitly at the end of
        an import allows two imports to use the same connection without having to do a commit in between. Useful for
        unit tests as well and also reduces the number of locks we hold open during a transaction.
        """
        with self._conn.cursor() as cursor:
            cursor.execute('SELECT pg_advisory_unlock(%s::BIGINT)', [self._import_lock_key])
            return cursor.fetchone()[0]

    def _compute_md5_hash(self):
        """Method to compute the MD5 hash for the filename."""
        self._logger.info('Computing MD5 hash of the input file...')
        with open(self._filename, 'rb') as f:
            md5 = compute_md5_hash(f)
        self._logger.info('Computed MD5 hash of the input file')
        metadata.add_optional_job_metadata(self._metadata_conn, 'dirbs-import', self.import_id,
                                           input_file_md5=md5)

    def _upload_pipeline(self):
        """Method to handle extracting, splitting, preprocessing, pre-validating and uploading input file."""
        self._logger.info('Extracting, splitting, preprocessing, prevalidating and uploading contents from file...')
        file_to_split = self._file_to_split()

        # State variables for parallel execution
        pipeline_state = defaultdict(int)

        # The idea here is that we have 2 executors, one for prevalidation and one for upload. We do this so
        # that we can have different numbers of workers for both and so that they can happen simultaneously.
        # A job is only ever submitted for upload once it has passed pre-validation but that should happen as soon
        # as a batch has been pre-validated
        futures_to_type = {}
        with futures.ProcessPoolExecutor(max_workers=self._max_local_cpus) as prevalidator, \
                futures.ProcessPoolExecutor(max_workers=self._max_db_connections) as uploader:
            self._logger.info('Simultaneously splitting, pre-validating and uploading '
                              '({0} pre-validation workers, {1} upload workers)'
                              .format(self._max_local_cpus, self._max_db_connections))
            for f in self._split_file(file_to_split):
                # We check for any completed jobs. We do this inside the loop with a zero timeout so that we can
                # kick off any upload jobs as soon as possible. We also do this before we kick off pre-validation
                # job so that when we exit this for loop we still have some jobs pending -- we do this so that we
                # at least the final batch has a progress message printed out
                self._process_pipeline_jobs(executor=uploader, futures_to_type=futures_to_type,
                                            state=pipeline_state, timeout=0)

                # Process new split batch
                pipeline_state['num_batches'] += 1
                processed_fn = self._preprocess_file(f)  # Pre-process file
                job = prevalidator.submit(self._prevalidate_file, processed_fn)  # Kick-off pre-validation job
                futures_to_type[job] = UploadPipelineJobType.PREVALIDATE

            # At this point, we are complete with splitting, so we just need to wait until all futures are done
            while futures_to_type:
                self._process_pipeline_jobs(executor=uploader, futures_to_type=futures_to_type,
                                            state=pipeline_state, num_batches_finalized=True)

            # Calculate number of uploaded rows
            with self._conn as conn, conn.cursor() as cursor:
                cursor.execute(sql.SQL("""SELECT COUNT(*) FROM {0}""").format(self._staging_tbl_identifier))
                self._data_length = cursor.fetchone()[0]

            self._logger.info('Successully pre-validated and uploaded {0:d} rows to the staging table'
                              .format(self._data_length))

    def _file_to_split(self):
        """Function to return file handle for the file to split."""
        file_to_split = None
        if self._extract:
            try:
                file_to_split = extract_csv_from_zip(self._filename)
                self._perform_filename_checks(file_to_split.name)
            except zipfile.BadZipFile as e:
                raise exceptions.ZipFileCheckException(str(e),
                                                       statsd=self._statsd,
                                                       metrics_failures_root=self._metrics_failures_root)
        else:
            self._perform_filename_checks(self._filename)
            file_to_split = open(self._filename, 'rb')

        return file_to_split

    def _split_file(self, file_to_split):
        """Method which unzips the input file into split files in the work directory."""
        split_file_basename = '{0}_import_{1}_split'.format(self._import_type, self.import_id)
        num_batches = 0
        for batch_filename in split_file(file_to_split, self._batch_size, self._extract_dir,
                                         self._logger, split_file_basename):
            self._files_to_delete.append(batch_filename)
            num_batches += 1
            yield batch_filename

        self._logger.info('Finished splitting input file into {num_batches} batches'
                          .format(num_batches=num_batches))
        return

    def _process_pipeline_jobs(self, *, executor, futures_to_type, state, num_batches_finalized=False, timeout=None):
        """Helper function to DRY out processing pipeline futures."""
        total_batches = state['num_batches']
        total_batches_msg = ' of {total:d}'.format(total=total_batches) if num_batches_finalized else ''

        try:
            for future in futures.as_completed(futures_to_type, timeout=timeout):
                job_type = futures_to_type[future]
                del futures_to_type[future]
                if job_type is UploadPipelineJobType.PREVALIDATE:
                    try:
                        res = future.result()  # will throw exception if this one was thrown
                        state['num_validated_batches'] += 1
                        done_batches = state['num_validated_batches']
                        msg = 'Pre-validated {done:d}{total} split files'.format(done=done_batches,
                                                                                 total=total_batches_msg)
                        # Queue upload job now that data has been pre-validated
                        job = executor.submit(self._upload_file_to_staging_table, res)
                        futures_to_type[job] = UploadPipelineJobType.UPLOAD
                    except exceptions.PrevalidationCheckRawException as err:
                        raise exceptions.PrevalidationCheckException(str(err),
                                                                     statsd=self._statsd,
                                                                     metrics_failures_root=self._metrics_failures_root)
                else:
                    future.result()
                    state['num_uploaded_batches'] += 1
                    done_batches = state['num_uploaded_batches']
                    msg = 'Uploaded {done:d}{total} split files'.format(done=done_batches, total=total_batches_msg)

                log_lvl = logging.DEBUG
                if done_batches % 50 == 0 or (done_batches == total_batches and num_batches_finalized):
                    log_lvl = logging.INFO
                self._logger.log(log_lvl, msg)
        except futures.TimeoutError:
            pass

    def _perform_filename_checks(self, input_filename):
        if not input_filename.endswith(self._expected_suffix):
            raise exceptions.FilenameCheckException(
                'Wrong suffix for passed file. Expected suffix {0} and got filename: {1}'
                .format(self._expected_suffix, self._filename),
                statsd=self._statsd,
                metrics_failures_root=self._metrics_failures_root
            )

    def _preprocess_file(self, input_filename):
        """Method which pre-processes the file ready for pre-validation."""
        return input_filename

    def _init_staging_table_shards(self):
        """Method to create IMEI shards in the staging table if supported by the importer."""
        assert self._supports_imei_shards
        with db_role_setter(self._conn, role_name=self._owner_role_name):
            tbl_name = self._staging_tbl_name
            partition_utils.create_imei_shard_partitions(self._conn, tbl_name=tbl_name, unlogged=True)
            for name, rstart, rend in partition_utils.physical_imei_shards(self._conn, tbl_name=tbl_name):
                self._on_staging_table_shard_creation(name, rstart, rend)

    def _on_staging_table_shard_creation(self, shard_name, virt_imei_shard_start, virt_imei_shard_end):
        """Function called whenever a shard is creating to give the importer a chance to add indices/triggers, etc."""
        pass

    def _prevalidate_file(self, input_filename):
        """Method which pre-validates the file using an external CSV validator against a CSV schema."""
        return prevalidate_file(input_filename, self._schema_file, self._validator, self._validation_schema_dir)

    def _upload_file_to_staging_table(self, input_filename):
        """Method to upload a single batch to the staging table."""
        conn = getattr(AbstractImporter._thread_local_storage, 'conn', None)
        if conn is None:
            conn = AbstractImporter._thread_local_storage.conn = create_db_connection(self._db_config)

        with open(input_filename, 'r') as f, conn, conn.cursor() as cursor:
            cursor.copy_expert(sql=self._upload_batch_to_staging_table_query(), file=f)
            return cursor.rowcount

    def _upload_batch_to_staging_table_query(self):
        """Method which returns the COPY query that copies data into the staging table."""
        raise NotImplementedError('Should be implemented')

    def _analyze_staging_table(self):
        """Run ANALYZE on the staging table to make sure that query plans are sane for loaded data volumes."""
        self._analyze_table_helper(self._staging_tbl_name)

    def _analyze_table_helper(self, tbl_name):
        """Function to DRY out code for ANALYZE'ing a table."""
        self._logger.debug('Running ANALYZE on {0} due to recently-updated data...'.format(tbl_name))
        with self._conn.cursor() as cursor:
            cursor.execute(sql.SQL('ANALYZE {0}').format(sql.Identifier(tbl_name)))
        self._logger.debug('Finished running ANALYZE on {0} due to recently-updated data'.format(tbl_name))

    @property
    def _binary_validation_checks_raw(self):
        """List of raw binary validation check results.

        Returns an iterable of result tuples (boolean result, message) for each binary validation check
        to be performed on raw staging data.
        """
        self._logger.info('No raw binary validation checks defined for this importer.')
        return []

    def _validate_binary_checks_raw(self):
        """Method to validate raw staging data prior to import. By default does nothing."""
        self._logger.info('Running binary validation checks on raw staging table data...')
        for result, msg, metric_key in self._binary_validation_checks_raw:
            if not result:
                raise exceptions.ValidationCheckException(msg,
                                                          metric_key=metric_key,
                                                          statsd=self._statsd,
                                                          metrics_failures_root=self._metrics_failures_root)
            else:
                self._logger.info(msg)
        self._logger.info('Finished running binary validation checks on raw staging table data')

    def _postprocess_staging_data(self):
        """Method to post-process staging data prior to validation. By default does nothing."""
        self._logger.info('No post-processing of staging data required.')

    @property
    def _binary_validation_checks(self):
        """List of binary validation check results.

        Returns an iterable of result tuples (boolean result, message) for each binary validation check
        to be performed on staging data.
        """
        self._logger.info('No binary validation checks defined for this importer.')
        return []

    def _validate_binary_checks(self):
        """Method to validate staging data prior to import. By default does nothing."""
        self._logger.info('Running binary validation checks on staging table data...')
        for result, msg, metric_key in self._binary_validation_checks:
            if not result:
                raise exceptions.ValidationCheckException(msg,
                                                          metric_key=metric_key,
                                                          statsd=self._statsd,
                                                          metrics_failures_root=self._metrics_failures_root)
            else:
                self._logger.info(msg)
        self._logger.info('Finished running binary validation checks on staging table data')

    @property
    def _threshold_validation_checks(self):
        """List of threshold validation check results.

        Returns an iterable of result tuples (check name, boolean result, input data ratio, max allowable ratio)
        for each threshold validation check to be performed on staging data.
        """
        self._logger.info('No threshold validation checks defined for this importer.')
        return []

    def _validate_threshold_checks(self):
        """Method to validate staging data prior to import. By default does nothing."""
        self._logger.info('Running threshold checks on staging table data...')
        for check, result, ratio, threshold, metric_key in self._threshold_validation_checks:
            if not result:
                msg = 'Failed {check} threshold check, limit is: {threshold:.2f} and imported data has: {ratio:.2f}'
                raise exceptions.ValidationCheckException(msg.format(check=check, ratio=ratio, threshold=threshold),
                                                          metric_key=metric_key,
                                                          statsd=self._statsd,
                                                          metrics_failures_root=self._metrics_failures_root)
            else:
                msg = 'Passed {check} threshold check, limit is: {threshold:.2f} and imported data has: {ratio:.2f}'
                self._logger.info(msg.format(check=check, ratio=ratio, threshold=threshold))
        self._logger.info('Finished running threshold validation checks on staging table data')

    @property
    def _historical_validation_checks(self):
        """List of historical validation check results.

        Returns an iterable of result tuples (check name, boolean result, staging data value, historical data value,
        min_required_value) for each historical validation check to be performed on staging data.
        """
        self._logger.info('No historical validation checks defined for this importer.')
        return []

    def _validate_historical_checks(self):
        """Method to validate staging data prior to import. By default does nothing."""
        self._logger.info('Running historic validation checks on staging table data...')
        for check, result, value, hvalue, min_required_value, metric_key in self._historical_validation_checks:
            if not result:
                msg = 'Failed {check} historic check, historic value is: {hvalue:.2f}, ' \
                      'imported data has: {value:.2f} and minimum required is {min_required_value:.2f}'
                raise exceptions.ValidationCheckException(msg.format(check=check,
                                                                     hvalue=hvalue,
                                                                     value=value,
                                                                     min_required_value=min_required_value),
                                                          metric_key=metric_key,
                                                          statsd=self._statsd,
                                                          metrics_failures_root=self._metrics_failures_root)
            else:
                msg = 'Passed {check} historic check, historic value is: {hvalue:.2f}, ' \
                      'imported data has: {value:.2f} and minimum required is {min_required_value:.2f}'
                self._logger.info(msg.format(check=check,
                                             hvalue=hvalue,
                                             value=value,
                                             min_required_value=min_required_value))
        self._logger.info('Finished running historic validation checks on staging table data')

    def _copy_staging_data(self):
        """Method to copy staging data into the real table using."""
        raise NotImplementedError('Should be implemented')

    def _output_stats(self, rows_before, rows_inserted, rows_updated, rows_deleted):
        """Method to log stats about the number of rows imported."""
        raise NotImplementedError('Should be implemented')

    def _do_final_cleanup(self):
        """An opportunity for an importer to clean up any temporary resources consumed during an import."""
        self._release_import_lock()
        if not self._no_cleanup:
            self._logger.debug('Cleanup: deleting intermediate data files...')
            for fn in self._files_to_delete:
                os.remove(fn)
                self._logger.debug('Deleted intermediate file {0}'.format(fn))
            self._logger.debug('Cleanup: deleted intermediate data files')
            with self._conn.cursor() as cursor:
                remaining_tables_to_delete = copy.copy(self._tables_to_cleanup_list)
                for t in self._tables_to_cleanup_list:
                    try:
                        cursor.execute(sql.SQL('DROP TABLE IF EXISTS {0} CASCADE').format(sql.Identifier(t)))
                        self._conn.commit()
                        remaining_tables_to_delete.remove(t)
                    except:  # noqa: E722
                        for t_not_deleted in remaining_tables_to_delete:
                            self._logger.warn('Failed to drop table {0} due to exception. Please issue '
                                              "\'DROP TABLE IF EXISTS {0}\' manually!".format(t_not_deleted))
                        raise
        else:
            self._logger.warn('Skipping staging tables/intermediate files cleanup due to command-line option')

    def _log_normalized_import_time_metrics(self, elapsed_time):
        """Method responsible for logging import time, normalized by the number of records in the input file."""
        # Only track normalized time stats if we worked out how many rows were in the source data and
        # it was greater than zero
        if self._data_length > 0:
            norm_factor = 1000000 / self._data_length
            self._statsd.gauge('{0}import_time.normalized'.format(self._metrics_import_root),
                               elapsed_time * norm_factor)
