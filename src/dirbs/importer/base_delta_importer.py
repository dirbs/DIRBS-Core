"""
Common code for importing delta and full list data sets into DIRBS Core.

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
from collections import defaultdict
from concurrent import futures

from psycopg2 import sql

from dirbs.importer.abstract_importer import AbstractImporter
import dirbs.metadata as metadata
import dirbs.partition_utils as partition_utils
import dirbs.utils as utils


class BaseDeltaImporter(AbstractImporter):
    """Base importer class for non-operator data import."""

    def __init__(self, *args, perform_delta_adds_check=True, delta=False,
                 perform_delta_removes_check=True, perform_delta_updates_check=True,
                 perform_historic_check=True, import_size_variation_percent=0.95,
                 import_size_variation_absolute=1000, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        self._perform_historic_check = perform_historic_check
        self._import_size_variation_percent = import_size_variation_percent
        self._import_size_variation_absolute = import_size_variation_absolute
        self._postprocessed = False
        self._delta = delta
        self._perform_delta_adds_check = perform_delta_adds_check
        self._perform_delta_removes_check = perform_delta_removes_check
        self._perform_delta_updates_check = perform_delta_updates_check

    @property
    def _historical_validation_checks(self):
        """Overrides AbstractImporter._historical_validation_checks."""
        if self._perform_historic_check:
            yield self._check_import_size_variation()
        else:
            self._logger.warning('Skipped historic import size check due to command-line option')

    @property
    def _import_metadata(self):
        """Overrides AbstractImporter._import_metadata."""
        md = super()._import_metadata
        md.update({
            'delta_mode': self._delta,
            'perform_historic_check': self._perform_historic_check,
            'historic_size_variation_max_abs': self._import_size_variation_absolute,
            'historic_size_variation_max_pct': self._import_size_variation_percent,
            'perform_delta_adds_check': self._perform_delta_adds_check,
            'perform_delta_removes_check': self._perform_delta_removes_check,
            'perform_delta_updates_check': self._perform_delta_updates_check,
        })
        return md

    @property
    def _binary_validation_checks_raw(self):
        """Overrides AbstractImporter._binary_validation_checks_raw."""
        yield self._check_for_conflicts()

    @property
    def _binary_validation_checks(self):
        """Overrides AbstractImporter._binary_validation_checks."""
        if self._delta:
            # fails if add and remove same rec
            yield self._delta_multiple_changes_check()
            # fails if record to add is not in the current list
            yield self._check_adds_not_in_db()
            # fails if record to remove is not in db
            yield self._check_removes_in_db()
            # fails if record to update is not in db
            yield self._check_updates_in_db()
        else:
            self._logger.info('Skipped validation of delta table because import is not in delta mode')

    def _count_changes_by_type_single_shard(self, staging_tbl_shard_name, filter_str=None):
        """Calculate staging table counts per change type for a single shard."""
        changes_by_type = {}
        if filter_str is None:
            filter_sql = sql.SQL('')
        else:
            filter_sql = sql.SQL('WHERE {filter_str}'.format(filter_str=filter_str))

        with utils.create_db_connection(self._db_config) as conn, conn.cursor() as cursor:
            cursor.execute(sql.SQL("""SELECT change_type, COUNT(*) AS cnt
                                        FROM {staging_tbl_shard}
                                             {change_filter}
                                    GROUP BY change_type
                                   """).format(staging_tbl_shard=sql.Identifier(staging_tbl_shard_name),
                                               change_filter=filter_sql))
            for res in cursor:
                changes_by_type[res.change_type] = res.cnt

        return changes_by_type

    def _output_stats(self, rows_before, rows_inserted, rows_updated, rows_deleted):
        """Overrides AbstractImporter._output_stats."""
        assert rows_before != -1 and 'rows_before should not be -1'
        input_file_type = 'delta' if self._delta else 'full'
        self._logger.info('Rows in table prior to import: {import_table_old_row_count}'
                          .format(import_table_old_row_count=rows_before))
        self._logger.info('Rows supplied in {input_file_type} input file: {file_row_count}'
                          .format(file_row_count=self._data_length, input_file_type=input_file_type))

        futures_list = []
        with futures.ThreadPoolExecutor(max_workers=self._max_db_connections) as executor:
            if self._supports_imei_shards:
                for name, rstart, rend in partition_utils.physical_imei_shards(self._conn,
                                                                               tbl_name=self._staging_tbl_name):
                    futures_list.append(executor.submit(self._count_changes_by_type_single_shard, name))
            else:
                futures_list.append(executor.submit(self._count_changes_by_type_single_shard,
                                                    self._staging_tbl_name))

        # All futures should be done at this point as with block is exited above
        changes_by_type = defaultdict(int)
        for f in futures_list:
            result = f.result()
            for k, v in result.items():
                changes_by_type[k] += v

        total_changes_count = sum(changes_by_type.values())
        break_down_str = '({0} adds, {1} removes, {2} updates)' \
                         .format(changes_by_type['add'], changes_by_type['remove'], changes_by_type['update'])
        self._logger.info('Changes supplied in {input_file_type} input file: {total_changes_count} {break_down_str}'
                          .format(total_changes_count=total_changes_count, input_file_type=input_file_type,
                                  break_down_str=break_down_str))

        self._logger.info('Rows in table after import: {import_table_new_row_count} '
                          '({import_table_rows_inserted} new, {import_table_rows_updated} updated, '
                          '{import_table_rows_deleted} removed)'
                          .format(import_table_new_row_count=rows_before + rows_inserted - rows_deleted,
                                  import_table_rows_inserted=rows_inserted,
                                  import_table_rows_updated=rows_updated,
                                  import_table_rows_deleted=rows_deleted))

        # Output StatsD metrics
        # Any delta row that resulted in either an add, update or delete is considered a "valid" row. This looks
        # a bit strange but basically "valid" means that the row had an effect - ie. was not ignored because it
        # was a duplicate row
        valid_changes = rows_inserted + rows_updated + rows_deleted
        invalid_changes = total_changes_count - valid_changes
        valid_adds = rows_inserted
        invalid_adds = changes_by_type['add'] - valid_adds
        valid_removes = rows_deleted
        invalid_removes = changes_by_type['remove'] - valid_removes
        valid_updates = rows_updated
        invalid_updates = changes_by_type['update'] - valid_updates

        # Generate StatsD metrics based on valid/invalid input rows
        self._statsd.gauge('{0}input_records.raw'.format(self._metrics_import_root), self._data_length)
        self._statsd.gauge('{0}input_records.delta.valid'.format(self._metrics_import_root), valid_changes)
        self._statsd.gauge('{0}input_records.delta.invalid'.format(self._metrics_import_root), invalid_changes)
        self._statsd.gauge('{0}input_records.delta_adds.valid'.format(self._metrics_import_root), valid_adds)
        self._statsd.gauge('{0}input_records.delta_adds.invalid'.format(self._metrics_import_root), invalid_adds)
        self._statsd.gauge('{0}input_records.delta_removes.valid'.format(self._metrics_import_root), valid_removes)
        self._statsd.gauge('{0}input_records.delta_removes.invalid'.format(self._metrics_import_root), invalid_removes)
        self._statsd.gauge('{0}input_records.delta_updates.valid'.format(self._metrics_import_root), valid_updates)
        self._statsd.gauge('{0}input_records.delta_updates.invalid'.format(self._metrics_import_root), invalid_updates)

        # Generate StatsD metrics based on output rows
        self._statsd.gauge('{0}imported_records.inserted'.format(self._metrics_import_root), valid_adds)
        self._statsd.gauge('{0}imported_records.updated'.format(self._metrics_import_root), valid_updates)
        self._statsd.gauge('{0}imported_records.deleted'.format(self._metrics_import_root), valid_removes)

        # Log stats to metadata table
        metadata.add_optional_job_metadata(self._metadata_conn, 'dirbs-import', self.import_id, input_stats={
            'num_raw_records': self._data_length,
            'num_delta_records_valid': valid_changes,
            'num_delta_records_invalid': invalid_changes,
            'num_delta_adds_valid': valid_adds,
            'num_delta_adds_invalid': invalid_adds,
            'num_delta_removes_valid': valid_removes,
            'num_delta_removes_invalid': invalid_removes,
            'num_delta_updates_valid': valid_updates,
            'num_delta_updates_invalid': invalid_updates
        })
        metadata.add_optional_job_metadata(self._metadata_conn, 'dirbs-import', self.import_id, output_stats={
            'num_records_old': rows_before,
            'num_records_new': self.row_count,
            'num_records_inserted': rows_inserted,
            'num_records_updated': rows_updated,
            'num_records_deleted': rows_deleted
        })

    @property
    def staging_row_count(self):
        """Overrides AbstractImporter.staging_row_count.

        This is NOT a completely accurate staging_row_count for performance reasons. Since this is only used
        in the import size check, which can be disabled, it has been decided to make a tradeoff between performance
        and accuracy. This is only inaccurate when:

        1. There are duplicate rows with the same primary keys and the same action in the delta table. A different
           action for the same primary key will have already failed the delta_multiple_changes check.
        2. An add is already in the DB or a remove is not in the DB. This will open happen if
           self._perform_delta_adds_check or self._perform_delta_removes_check is False when these checks would have
           failed if enabled.
        """
        assert self._postprocessed and \
            'BaseDeltaImporter::staging_row_count called on un-postprocessed/non-delta table'

        self._logger.info('Counting the approximate number of entries in staging table based on delta...')
        futures_list = []
        filter_str = "change_type != \'update\'"
        with futures.ThreadPoolExecutor(max_workers=self._max_db_connections) as executor:
            if self._supports_imei_shards:
                for name, rstart, rend in partition_utils.physical_imei_shards(self._conn,
                                                                               tbl_name=self._staging_tbl_name):
                    futures_list.append(executor.submit(self._count_changes_by_type_single_shard, name,
                                                        filter_str=filter_str))
            else:
                futures_list.append(executor.submit(self._count_changes_by_type_single_shard,
                                                    self._staging_tbl_name,
                                                    filter_str=filter_str))

        # All futures should be done at this point as with block is exited above
        changes_by_type = defaultdict(int)
        for f in futures_list:
            result = f.result()
            for k, v in result.items():
                changes_by_type[k] += v

        old_row_count = self.row_count
        approx_add_count = sum(v for k, v in changes_by_type.items() if k == 'add')
        approx_remove_count = sum(v for k, v in changes_by_type.items() if k == 'remove')
        approx_net_adds = approx_add_count - approx_remove_count
        approx_new_count = old_row_count + approx_net_adds
        self._logger.info('Found {0} approximate entries in staging table based on delta '
                          '({1} in existing table, {2} adds, {3} removes)'.format(approx_new_count,
                                                                                  old_row_count, approx_add_count,
                                                                                  approx_remove_count))

        return old_row_count + approx_net_adds

    @property
    def row_count(self):
        """Row count currently in DB for this type of import."""
        with self._conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM {0}'.format(self._import_relation_name))
            return cursor.fetchone()[0]

    @property
    def _delta_tbl_ddl(self):
        """Delta table DDL."""
        raise NotImplementedError('Should be implemented')

    @property
    def _delta_tbl_name(self):
        """Delta table name."""
        return '{0}_delta'.format(self._staging_tbl_name)

    @property
    def _delta_tbl_identifier(self):
        """Delta table name Identifier."""
        return sql.Identifier(self._delta_tbl_name)

    @property
    def _old_staging_tbl_name(self):
        """Old staging table name."""
        return '{0}_old'.format(self._staging_tbl_name)

    @property
    def _old_staging_tbl_identifier(self):
        """Old staging table Identifier."""
        return sql.Identifier(self._old_staging_tbl_name)

    @property
    def _historic_tbl_name(self):
        """Name for historic table."""
        return 'historic_{0}'.format(self._import_relation_name)

    @property
    def _staging_tbl_ddl(self):
        """DDL for the staging table."""
        raise NotImplementedError('Should be implemented')

    @property
    def _input_csv_field_names(self):
        """Column name(s) of the staging table before instert trigger (no imei_norm)."""
        raise NotImplementedError('Should be implemented')

    @property
    def _input_csv_field_identifiers(self):
        """Column name(s) of the staging table before insert trigger (no imei_norm)."""
        return [sql.Identifier(x) for x in self._input_csv_field_names]

    @property
    def _staging_data_insert_trigger_name(self):
        """Staging data insert trigger name."""
        return '{0}_staging_data_insert_trigger_fn'.format(self._import_type)

    @property
    def _pk_field_names(self):
        """Column name(s) that define the primary key in the import table."""
        raise NotImplementedError('Should be implemented')

    @property
    def _pk_field_identifiers(self):
        """Return a list of pk Identifiers."""
        return [sql.Identifier(pk) for pk in self._pk_field_names]

    @property
    def _pk_field_columns(self):
        """Return a list of pk columns."""
        return sql.SQL(', ').join(self._pk_field_identifiers)

    @property
    def _extra_field_names(self):
        """Returns all the extra fields of the import table."""
        return []

    @property
    def _extra_field_identifiers(self):
        """Return a list of extra_fields Identifiers."""
        return [sql.Identifier(pk) for pk in self._extra_field_names]

    @property
    def _extra_field_columns(self):
        """Return a list of extra field columns."""
        return sql.SQL(', ').join(self._extra_field_identifiers)

    @property
    def _output_field_names(self):
        """Returns column names defining all the primary key and extra_fields of the final table."""
        return self._pk_field_names + self._extra_field_names

    @property
    def _output_field_columns(self):
        """Returns output_field_names as sql.Identifiers and comma-separated as a sql.SQL object."""
        return sql.SQL(', ').join(map(sql.Identifier, self._output_field_names))

    @property
    def _all_fields_no_dupes_columns(self):
        """Return column names defining all the primary key and extra_fields aggregated with 'first' aggregate."""
        if self._extra_field_identifiers:
            # '"imei_norm", "first"("reporting_date")'
            aggregate_field_names = [sql.SQL('first({ex})').format(ex=ex) for ex in self._extra_field_identifiers]
            return sql.SQL(', ').join(self._pk_field_identifiers + aggregate_field_names)
        return self._pk_field_columns

    def _upload_batch_to_staging_table_query(self):
        """Overrides AbstractImporter._upload_batch_to_staging_table_query."""
        input_csv_field_identifiers = self._input_csv_field_identifiers
        if self._delta:
            input_csv_field_identifiers.append(sql.Identifier('change_type'))
        return sql.SQL("""COPY {0} ({1}) FROM STDIN WITH CSV HEADER""") \
            .format(self._staging_tbl_identifier, sql.SQL(', ').join(input_csv_field_identifiers))

    def _init_staging_table(self):
        """Overrides AbstractImporter._init_staging_table."""
        self._tables_to_cleanup_list.append(self._staging_tbl_name)
        with self._conn, self._conn.cursor() as cursor, \
                utils.db_role_setter(self._conn, role_name=self._owner_role_name):
            base_staging_tbl_ddl = sql.SQL(self._staging_tbl_ddl).format(self._staging_tbl_identifier)
            if self._supports_imei_shards:
                staging_tbl_ddl = \
                    sql.SQL('{0} PARTITION BY RANGE (calc_virt_imei_shard(imei))').format(base_staging_tbl_ddl)
            else:
                staging_tbl_ddl = base_staging_tbl_ddl

            cursor.execute(staging_tbl_ddl)

            if self._delta:
                self._add_change_type_column_with_constraint(self._staging_tbl_identifier, cursor)

            if self._staging_data_insert_trigger_name and not self._supports_imei_shards:
                # Tigger will be added on a per-shard basis for sharded imports
                trigger_name = '{0}_data_insert_staging_trigger_{1}'.format(self._import_relation_name, self.import_id)
                cursor.execute(sql.SQL("""CREATE TRIGGER {0} BEFORE INSERT
                                              ON {1}
                                             FOR EACH ROW
                               EXECUTE PROCEDURE {2}()""")
                               .format(sql.Identifier(trigger_name),
                                       self._staging_tbl_identifier,
                                       sql.Identifier(self._staging_data_insert_trigger_name)))

    def _on_staging_table_shard_creation(self, shard_name, virt_imei_shard_start, virt_imei_shard_end):
        """Overrides AbstractImporter._on_staging_table_shard_creation."""
        if self._staging_data_insert_trigger_name:
            with self._conn.cursor() as cursor:
                trigger_name = '{0}_data_insert_staging_trigger_{1:d}_{2:d}_{3:d}' \
                               .format(self._import_relation_name, self.import_id,
                                       virt_imei_shard_start, virt_imei_shard_end - 1)
                cursor.execute(sql.SQL("""CREATE TRIGGER {0} BEFORE INSERT
                                              ON {1}
                                             FOR EACH ROW
                               EXECUTE PROCEDURE {2}()""")
                               .format(sql.Identifier(trigger_name),
                                       sql.Identifier(shard_name),
                                       sql.Identifier(self._staging_data_insert_trigger_name)))

    def _check_import_size_variation(self):
        """Check whether the import size has changed appropriately in the new import."""
        curr_row_count = self.row_count
        staging_row_count = self.staging_row_count
        # If there is nothing in the DB, allow anything
        metric_key = 'historic_import_size'
        if curr_row_count == 0:
            return 'import size', True, staging_row_count, curr_row_count, 0, metric_key

        # Never allow the new import to be more than specified absolute rows smaller than the existing row count
        # -1 is a special value for _import_size_variation_absolute variable.
        # If _import_size_variation_absolute is a positive integer (zero allowed), it will
        # check that specified absolute rows are bigger than the existing row count.
        # By setting this variable to neg one, this check will be disabled.
        if self._import_size_variation_absolute == -1:
            min_staging_row_count = 1
        else:
            min_staging_row_count = max(1, curr_row_count - self._import_size_variation_absolute)
        # Don't allow more than specified decrease percentage
        min_staging_row_count = max(min_staging_row_count, self._import_size_variation_percent * curr_row_count)
        check_result = staging_row_count >= min_staging_row_count
        return 'import size', check_result, staging_row_count, curr_row_count, min_staging_row_count, metric_key

    def _check_for_conflicts_single_partition(self, partition_name):
        """Function to check for conflicts in a single partition."""
        with utils.create_db_connection(self._db_config) as conn, conn.cursor() as cursor:
            cursor.execute(sql.SQL("""SELECT {pk_ids}, COUNT(DISTINCT ({extra_fields}))-1 AS dc
                                        FROM {staging_tbl_partition}
                                    GROUP BY {pk_ids}
                                             HAVING COUNT(DISTINCT ({extra_fields})) > 1""")  # noqa Q441
                           .format(extra_fields=self._extra_field_columns,
                                   staging_tbl_partition=sql.Identifier(partition_name),
                                   pk_ids=self._pk_field_columns))
            return cursor.fetchall()

    def _check_for_conflicts(self):
        """Check whether this list contains any conflicts (same PK, different extra field values)."""
        metric_key = 'conflicts'
        if self._extra_field_names:
            futures_list = []
            with futures.ThreadPoolExecutor(max_workers=self._max_db_connections) as executor:
                if self._supports_imei_shards:
                    for name, rstart, rend in partition_utils.physical_imei_shards(self._conn,
                                                                                   tbl_name=self._staging_tbl_name):
                        futures_list.append(executor.submit(self._check_for_conflicts_single_partition, name))
                else:
                    futures_list.append(executor.submit(self._check_for_conflicts_single_partition,
                                                        self._staging_tbl_name))

            # All futures should be done at this point as with block is exited above
            conflict_rows = []
            for f in futures_list:
                partial_conflicts = f.result()
                conflict_rows.extend(partial_conflicts)

            if not conflict_rows:
                return True, 'Conflicting rows check passed', metric_key

            confl_rows_sum = 0
            for x in conflict_rows:
                self._logger.debug('Found {count} '
                                   'conflicting row(s) with primary key {pk_names}: {pk_values}'
                                   .format(count=x.dc,
                                           pk_names=tuple(self._pk_field_names),
                                           pk_values=tuple(getattr(x, pk) for pk in self._pk_field_names)))
                confl_rows_sum += x.dc
            return False, 'Conflicting rows check failed ({0:d} rows with same primary key and conflicting data)' \
                .format(confl_rows_sum), metric_key

        return True, 'Conflicting rows check skipped due to lack of extra_fields', metric_key

    def _add_change_type_column_with_constraint(self, tblname, cursor):
        """Helper function to add column change type to staging and delta table.

        Change type can be either 'add', 'remove' or 'update'.
        """
        cursor.execute(sql.SQL("""ALTER TABLE {0} ADD COLUMN change_type TEXT""").format(tblname))
        cursor.execute(sql.SQL("""ALTER TABLE {0}
                               ADD CONSTRAINT delta_change_type_check
                                        CHECK (change_type IN ('add',
                                                               'remove',
                                                               'update'))""").format(tblname))

    def _init_delta_table(self):
        """Init delta table."""
        with self._conn, self._conn.cursor() as cursor, \
                utils.db_role_setter(self._conn, role_name=self._owner_role_name):
            # copy constraints from staging table to delta to check if change_type is either 'add', 'remove', 'update'.
            base_delta_tbl_ddl = sql.SQL(
                """CREATE UNLOGGED TABLE {delta_tbl_name}
                   (LIKE {staging_tbl_name} INCLUDING DEFAULTS)
                """).format(delta_tbl_name=self._delta_tbl_identifier,
                            staging_tbl_name=self._staging_tbl_identifier)
            if self._supports_imei_shards:
                delta_tbl_ddl = \
                    sql.SQL('{0} PARTITION BY RANGE (calc_virt_imei_shard(imei_norm))').format(base_delta_tbl_ddl)
            else:
                delta_tbl_ddl = base_delta_tbl_ddl

            cursor.execute(delta_tbl_ddl)
            self._add_change_type_column_with_constraint(self._delta_tbl_identifier, cursor)

        if self._supports_imei_shards:
            with self._conn, self._conn.cursor() as cursor:
                tbl_name = self._delta_tbl_name
                partition_utils.create_imei_shard_partitions(self._conn, tbl_name=tbl_name, unlogged=True)

    def _compare_pks_sql(self):
        """Generate sql to compare primary keys between table with alias 'o' and 'n'."""
        return sql.SQL(' AND ').join([sql.SQL('{o}.{pk} = {n}.{pk}').format(o=sql.Identifier('o'), pk=pk,
                                                                            n=sql.Identifier('n'))
                                      for pk in self._pk_field_identifiers])

    def _vacuum_table_helper(self, tbl_name):
        """Function to DRY out code for VACUUM ANALYZE'ing a table."""
        # Use new autocommit connction to do VACUUM ANALYZE since it can't be done in a transaction
        self._logger.info('Running VACUUM ANALYZE on {0}...'.format(tbl_name))
        with utils.create_db_connection(self._db_config, autocommit=True) as conn, conn.cursor() as cursor:
            cursor.execute(sql.SQL('VACUUM ANALYZE {0}').format(sql.Identifier(tbl_name)))
        self._logger.info('Finished running VACUUM ANALYZE on {0}'.format(tbl_name))

    def _postprocess_staging_data(self):
        """Overrides BaseTableReplacementImporter._postprocess_staging_data.

        Compute delta table comparing staging table and existing import views.
        """
        #
        # Conditionally create an index on the staging table's key columns. We should only do this if we are
        # confident that the index will be used, as it will take a large amount of time (~15mins for a 155M row
        # staging table). If the PostgreSQL planner decides to use a hash join because one of the relations is much
        # smaller than the other and can be converted into an in-memory hash table, we will do a sequential scan of
        # the larger table and our index will not be used. The index will be vastly useful for a merge join though,
        # where we can do index-only scans of both tables to provide sorted intput to the join without needing to
        # explicitly sort and create temp tables.
        #
        # Our basic heuristic is that we think that it's worth creating the index if both relations are greater
        # than 10M rows per shard
        #
        min_row_count_threshold_for_index = 10e6
        existing_tbl_row_count = self.row_count
        # PK index is not unique in delta/staging tables as we filter out duplicates when moving to the
        # historic table
        pk_idx_metadatum = partition_utils.IndexMetadatum(idx_cols=self._pk_field_names)
        staging_tbl_has_pk_index = False

        n_workers = self._max_db_connections
        with futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
            with self._conn, self._conn.cursor() as cursor:
                cursor.execute(sql.SQL('SELECT COUNT(*) FROM {tbl}').format(tbl=self._staging_tbl_identifier))
                staging_tbl_row_count = cursor.fetchone()[0]

            if min(staging_tbl_row_count, existing_tbl_row_count) > min_row_count_threshold_for_index:
                staging_tbl_type = 'delta' if self._delta else 'staging'
                self._logger.info('Creating index on key columns in {tbl_type} table...'
                                  .format(tbl_type=staging_tbl_type))
                partition_utils.add_indices_parallel(self._conn, executor, self._db_config,
                                                     tbl_name=self._staging_tbl_name,
                                                     idx_metadata=[pk_idx_metadatum])
                staging_tbl_has_pk_index = True
                self._logger.info('Created index on key columns in {tbl_type} table'.format(tbl_type=staging_tbl_type))

                # VACUUM if we created an index, to improve index-only scans
                self._vacuum_table_helper(self._staging_tbl_name)

            if not self._delta:
                self._init_delta_table()

                # Populate delta table from staging and importer view
                for change_type, count in self._compute_full_list_to_delta(executor):
                    self._logger.info('Computed {count} {change_type} delta entries by comparing '
                                      'import list and current DB table'.format(count=count,
                                                                                change_type=change_type))

                # Rename staging to old_staging
                self._tables_to_cleanup_list.append(self._old_staging_tbl_name)
                idx_metadata = [pk_idx_metadatum] if staging_tbl_has_pk_index else None
                with self._conn:
                    partition_utils.rename_table_and_indices(self._conn,
                                                             old_tbl_name=self._staging_tbl_name,
                                                             new_tbl_name=self._old_staging_tbl_name,
                                                             idx_metadata=idx_metadata)

                    # Rename delta to staging
                    partition_utils.rename_table_and_indices(self._conn,
                                                             old_tbl_name=self._delta_tbl_name,
                                                             new_tbl_name=self._staging_tbl_name)

                # Conditionally create an index on the delta table's key columns. See comment at the top of this
                # function to see when the reasoning behind this.
                with self._conn, self._conn.cursor() as cursor:
                    cursor.execute(sql.SQL('SELECT COUNT(*) FROM {tbl}').format(tbl=self._staging_tbl_identifier))
                    delta_tbl_row_count = cursor.fetchone()[0]

                if min(delta_tbl_row_count, existing_tbl_row_count) > min_row_count_threshold_for_index:
                    self._logger.info('Creating index on key columns in delta table...')
                    partition_utils.add_indices_parallel(self._conn, executor, self._db_config,
                                                         tbl_name=self._staging_tbl_name,
                                                         idx_metadata=[pk_idx_metadatum])
                    self._logger.info('Created index on key columns in delta table')

                    # VACUUM if we created an index, to improve index-only scans
                    self._vacuum_table_helper(self._staging_tbl_name)
                else:
                    self._analyze_staging_table()

        # Boolean used to assert that staging_row_count is called only after postprocessing
        self._postprocessed = True

    def _compute_full_list_to_delta(self, executor):
        """Generator to compute the delta from a full list, yielding results as each type of delta is complete."""
        futures_to_type = {}
        num_physical_shards = partition_utils.num_physical_imei_shards(self._conn) if self._supports_imei_shards else 1
        if self._supports_imei_shards:
            virt_imei_shard_ranges = partition_utils.virt_imei_shard_bounds(num_physical_shards)
            for virt_imei_range_start, virt_imei_range_end in virt_imei_shard_ranges:
                staging_tbl_part_name = partition_utils.imei_shard_name(base_name=self._staging_tbl_name,
                                                                        virt_imei_range_start=virt_imei_range_start,
                                                                        virt_imei_range_end=virt_imei_range_end)
                import_tbl_part_name = partition_utils.imei_shard_name(base_name=self._historic_tbl_name,
                                                                       virt_imei_range_start=virt_imei_range_start,
                                                                       virt_imei_range_end=virt_imei_range_end)
                delta_tbl_part_name = partition_utils.imei_shard_name(base_name=self._delta_tbl_name,
                                                                      virt_imei_range_start=virt_imei_range_start,
                                                                      virt_imei_range_end=virt_imei_range_end)

                f = executor.submit(self._calc_delta_removes_single_shard,
                                    staging_tbl_part_name=staging_tbl_part_name,
                                    import_tbl_part_name=import_tbl_part_name,
                                    delta_tbl_part_name=delta_tbl_part_name,
                                    import_filter='end_date IS NULL')
                futures_to_type[f] = 'removes'
                f = executor.submit(self._calc_delta_adds_single_shard,
                                    staging_tbl_part_name=staging_tbl_part_name,
                                    import_tbl_part_name=import_tbl_part_name,
                                    delta_tbl_part_name=delta_tbl_part_name,
                                    import_filter='end_date IS NULL')
                futures_to_type[f] = 'adds'
                f = executor.submit(self._calc_delta_updates_single_shard,
                                    staging_tbl_part_name=staging_tbl_part_name,
                                    import_tbl_part_name=import_tbl_part_name,
                                    delta_tbl_part_name=delta_tbl_part_name,
                                    import_filter='end_date IS NULL')
                futures_to_type[f] = 'updates'
        else:
            f = executor.submit(self._calc_delta_removes_single_shard,
                                staging_tbl_part_name=self._staging_tbl_name,
                                import_tbl_part_name=self._import_relation_name,
                                delta_tbl_part_name=self._delta_tbl_name)
            futures_to_type[f] = 'removes'
            f = executor.submit(self._calc_delta_adds_single_shard,
                                staging_tbl_part_name=self._staging_tbl_name,
                                import_tbl_part_name=self._import_relation_name,
                                delta_tbl_part_name=self._delta_tbl_name)
            futures_to_type[f] = 'adds'
            f = executor.submit(self._calc_delta_updates_single_shard,
                                staging_tbl_part_name=self._staging_tbl_name,
                                import_tbl_part_name=self._import_relation_name,
                                delta_tbl_part_name=self._delta_tbl_name)
            futures_to_type[f] = 'updates'

        job_completion_state = dict(adds=0, removes=0, updates=0)
        rows_changed_state = dict(adds=0, removes=0, updates=0)
        for f in futures.as_completed(futures_to_type):
            rows_changed = f.result()
            change_type = futures_to_type[f]
            job_completion_state[change_type] += 1
            rows_changed_state[change_type] += rows_changed
            if job_completion_state[change_type] == num_physical_shards:
                yield change_type, rows_changed_state[change_type]

    def _calc_delta_removes_single_shard(self, *, staging_tbl_part_name, import_tbl_part_name, delta_tbl_part_name,
                                         import_filter=None):
        """Calculate the delta 'remove' entries for a single shard if the import is a full list."""
        # When remove entries are added to the delta table, the imei column will be NULL as import
        # table only has an imei_norm column.
        return self._calc_delta_common_single_shard(containing_table_part_name=import_tbl_part_name,
                                                    not_containing_table_part_name=staging_tbl_part_name,
                                                    delta_tbl_part_name=delta_tbl_part_name,
                                                    change_type='remove',
                                                    containing_filter=import_filter,
                                                    not_containing_filter=None)

    def _calc_delta_adds_single_shard(self, *, staging_tbl_part_name, import_tbl_part_name, delta_tbl_part_name,
                                      import_filter=None):
        """Calculate the delta 'add' entries for a single shard if the import is a full list."""
        return self._calc_delta_common_single_shard(containing_table_part_name=staging_tbl_part_name,
                                                    not_containing_table_part_name=import_tbl_part_name,
                                                    delta_tbl_part_name=delta_tbl_part_name,
                                                    change_type='add',
                                                    containing_filter=None,
                                                    not_containing_filter=import_filter)

    def _calc_delta_common_single_shard(self, *, containing_table_part_name, not_containing_table_part_name,
                                        delta_tbl_part_name, change_type, containing_filter,
                                        not_containing_filter):
        with utils.create_db_connection(self._db_config) as conn, conn.cursor() as cursor:
            containing_filter_sql = \
                sql.SQL('AND {0}').format(sql.SQL(containing_filter)) if containing_filter else sql.SQL('')
            not_containing_filter_sql = \
                sql.SQL('AND {0}').format(sql.SQL(not_containing_filter)) if not_containing_filter else sql.SQL('')

            cursor.execute(sql.SQL("""INSERT INTO {delta_table_name}({output_field_columns}, change_type)
                                           SELECT {output_field_columns}, %s
                                             FROM {containing_table} o
                                            WHERE NOT EXISTS (SELECT 1
                                                                FROM {not_containing_table} n
                                                               WHERE {join_tables_on_pk_sql}
                                                                     {not_containing_filter_sql})
                                                  {containing_filter_sql}""")
                           .format(delta_table_name=sql.Identifier(delta_tbl_part_name),
                                   output_field_columns=self._output_field_columns,
                                   containing_table=sql.Identifier(containing_table_part_name),
                                   not_containing_table=sql.Identifier(not_containing_table_part_name),
                                   join_tables_on_pk_sql=self._compare_pks_sql(),
                                   containing_filter_sql=containing_filter_sql,
                                   not_containing_filter_sql=not_containing_filter_sql,
                                   pk_ids=self._pk_field_columns), [change_type])

            return cursor.rowcount

    def _calc_delta_updates_single_shard(self, *, staging_tbl_part_name, import_tbl_part_name, delta_tbl_part_name,
                                         import_filter=None):
        """Delta update helper."""
        if self._extra_field_names:
            # Generate sql to compare primary extra_fields between table with alias 'o' and 'n'.
            import_filter_sql = sql.SQL('AND {0}').format(sql.SQL(import_filter)) if import_filter else sql.SQL('')
            _join_tbls_on_extra_fields_sql = [sql.SQL('{o}.{ex} != {n}.{ex}').format(o=sql.Identifier('o'), ex=ex,
                                                                                     n=sql.Identifier('n'))
                                              for ex in self._extra_field_identifiers]
            _join_tbls_on_extra_fields_sql = sql.SQL(' OR ').join(_join_tbls_on_extra_fields_sql)
            with utils.create_db_connection(self._db_config) as conn, conn.cursor() as cursor:
                cursor.execute(sql.SQL("""INSERT INTO {delta_table_name}({output_field_columns}, change_type)
                                               SELECT {output_field_columns}, 'update'
                                                 FROM {staging_table} o
                                                WHERE EXISTS (SELECT 1
                                                                FROM {import_table} n
                                                               WHERE {join_tables_on_pk_sql}
                                                                     {import_filter_sql}
                                                                 AND ({_join_tbls_on_extra_fields_sql}))""")
                               .format(delta_table_name=sql.Identifier(delta_tbl_part_name),
                                       output_field_columns=self._output_field_columns,
                                       staging_table=sql.Identifier(staging_tbl_part_name),
                                       import_table=sql.Identifier(import_tbl_part_name),
                                       join_tables_on_pk_sql=self._compare_pks_sql(),
                                       import_filter_sql=import_filter_sql,
                                       _join_tbls_on_extra_fields_sql=_join_tbls_on_extra_fields_sql))

            return cursor.rowcount
        else:
            return 0

    def _delta_multiple_changes_check_single_shard(self, staging_tbl_part_name):
        """Check that same record cannot be added or removed at the same time in a single shard."""
        with utils.create_db_connection(self._db_config) as conn, conn.cursor() as cursor:
            cursor.execute(sql.SQL("""SELECT {pks}
                                        FROM {staging_tbl}
                                    GROUP BY {pks}
                                             HAVING COUNT(DISTINCT(change_type)) > 1""")
                           .format(staging_tbl=sql.Identifier(staging_tbl_part_name),
                                   pks=self._pk_field_columns,
                                   join_tables_on_pk_sql=self._compare_pks_sql()))
            return [x._asdict() for x in cursor.fetchall()]

    def _delta_multiple_changes_check(self):
        """Check that same record cannot be added or removed at the same time."""
        futures_list = []
        with futures.ThreadPoolExecutor(max_workers=self._max_db_connections) as executor:
            if self._supports_imei_shards:
                for name, rstart, rend in partition_utils.physical_imei_shards(self._conn,
                                                                               tbl_name=self._staging_tbl_name):
                    futures_list.append(executor.submit(self._delta_multiple_changes_check_single_shard, name))
            else:
                futures_list.append(executor.submit(self._delta_multiple_changes_check_single_shard,
                                                    self._staging_tbl_name))

        # All futures should be done at this point as with block is exited above
        failing_rows = []
        for f in futures_list:
            failing_rows.extend(f.result())

        metric_key = 'multiple_changes_check'
        if len(failing_rows) > 0:
            failing_rows_formatted = ', '.join(['{0}: {1}'
                                               .format(pk, failing_rows[0][pk]) for pk in self._pk_field_names])
            return False, 'Same record cannot be added or removed at the same time in delta list. ' \
                          'Failing rows: {fr}...'.format(fr=failing_rows_formatted), metric_key
        return True, 'Conflicting changes check passed', metric_key

    def _check_adds_not_in_db(self):
        """Check that record to add must not be in the db."""
        metric_key = 'delta_adds_check'
        msg_check_passed = 'Adds not in db check passed'
        msg_check_failed = 'Failed add delta validation check. Cannot add item that is already in db. ' \
                           'Failing rows: {failing_rows}'
        return self._validate_check_helper('add', self._perform_delta_adds_check, msg_check_passed,
                                           msg_check_failed, metric_key)

    def _check_removes_in_db(self):
        """Check that record to remove must be in the db."""
        metric_key = 'delta_removes_check'
        msg_check_passed = 'Removes in db check passed'
        msg_check_failed = 'Failed remove delta validation check. Cannot remove records not in db. ' \
                           'Failing rows: {failing_rows}'
        return self._validate_check_helper('remove', self._perform_delta_removes_check, msg_check_passed,
                                           msg_check_failed, metric_key)

    def _check_updates_in_db(self):
        """Check that record to update must be in the db."""
        metric_key = 'delta_updates_check'
        msg_check_passed = 'Updates in db check passed'
        msg_check_failed = 'Failed update delta validation check. Cannot update records not in db. ' \
                           'Failing rows: {failing_rows}'
        return self._validate_check_helper('update', self._perform_delta_updates_check, msg_check_passed,
                                           msg_check_failed, metric_key)

    def _validate_check_helper_single_shard(self, change_type, staging_tbl_part_name, import_tbl_part_name):
        """Common code for validation_checks on a single shard."""
        with utils.create_db_connection(self._db_config) as conn, conn.cursor() as cursor:
            not_sql = sql.SQL('NOT') if change_type != 'add' else sql.SQL('')
            cursor.execute(sql.SQL("""SELECT {pks}
                                        FROM {staging_tbl} o
                                       WHERE change_type = %s
                                         AND {not_sql} EXISTS (SELECT 1
                                                                 FROM {historic_tbl} n
                                                                WHERE {join_tables_on_pk_sql}
                                                                  AND end_date IS NULL)""")
                           .format(pks=self._pk_field_columns,
                                   staging_tbl=sql.Identifier(staging_tbl_part_name),
                                   historic_tbl=sql.Identifier(self._historic_tbl_name),
                                   not_sql=not_sql,
                                   join_tables_on_pk_sql=self._compare_pks_sql()), [change_type])
            return [x._asdict() for x in cursor.fetchall()]

    def _validate_check_helper(self, change_type, perform_check, msg_check_passed, msg_check_failed, metric_key):
        """Common code for validation_checks."""
        if perform_check:
            futures_list = []
            with futures.ThreadPoolExecutor(max_workers=self._max_db_connections) as executor:
                if self._supports_imei_shards:
                    num_physical_shards = partition_utils.num_physical_imei_shards(self._conn)
                    virt_imei_shard_ranges = partition_utils.virt_imei_shard_bounds(num_physical_shards)
                    for virt_imei_range_start, virt_imei_range_end in virt_imei_shard_ranges:
                        staging_tbl_part_name = \
                            partition_utils.imei_shard_name(base_name=self._staging_tbl_name,
                                                            virt_imei_range_start=virt_imei_range_start,
                                                            virt_imei_range_end=virt_imei_range_end)
                        import_tbl_part_name = \
                            partition_utils.imei_shard_name(base_name=self._import_relation_name,
                                                            virt_imei_range_start=virt_imei_range_start,
                                                            virt_imei_range_end=virt_imei_range_end)

                        futures_list.append(executor.submit(self._validate_check_helper_single_shard,
                                                            change_type,
                                                            staging_tbl_part_name,
                                                            import_tbl_part_name))
                else:
                    futures_list.append(executor.submit(self._validate_check_helper_single_shard,
                                                        change_type,
                                                        self._staging_tbl_name,
                                                        self._import_relation_name))

            # All futures should be done at this point as with block is exited above
            failing_rows = []
            for f in futures_list:
                failing_rows.extend(f.result())

            if len(failing_rows) > 0:
                failing_rows_format = ', '.join(['{0}: {1}'.format(pk, failing_rows[0][pk])
                                                 for pk in self._pk_field_names])
                return False, msg_check_failed.format(failing_rows=failing_rows_format), metric_key
        else:
            self._logger.info('Skipped delta {change_type}s check due to command-line option'
                              .format(change_type=change_type))

        return True, msg_check_passed, metric_key

    def _copy_staging_data(self):
        """Overrides AbstractImporter._copy_staging_data."""
        self._logger.info('Updating {0} table...'.format(self._historic_tbl_name))
        # Get job_start_time
        with self._conn as conn, conn.cursor() as cursor:
            cursor.execute(sql.SQL('SELECT start_time FROM job_metadata WHERE run_id = %s'), [self.import_id])
            job_start_time = cursor.fetchone()[0]

            rows_affected = {}
            with futures.ThreadPoolExecutor(max_workers=self._max_db_connections) as executor:
                # Populate delta table from staging and importer view
                for change_type, count in self._populate_historic_from_delta(executor, job_start_time):
                    if change_type == 'add':
                        change_type_str = 'Added'
                    elif change_type == 'remove':
                        change_type_str = 'Removed'
                    elif change_type == 'update':
                        change_type_str = 'Updated'
                    else:
                        assert False, 'Unexpected change_type!'

                    self._logger.info('{change_type_str} {count} entries in {historic_tbl}'
                                      .format(change_type_str=change_type_str,
                                              count=count,
                                              historic_tbl=self._historic_tbl_name))
                    rows_affected[change_type] = count

        # Always VACUUM ANALYZE after insert, since inserts won't trigger autovacuum and visibility map should
        # be updated
        self._vacuum_table_helper(self._historic_tbl_name)

        return rows_affected['add'], rows_affected['update'], rows_affected['remove']

    def _populate_historic_from_delta(self, executor, job_start_time):
        """Generator which processes all the updates to the historic table and yields when a category is complete."""
        num_physical_shards = partition_utils.num_physical_imei_shards(self._conn) if self._supports_imei_shards else 1
        futures_list = []
        if self._supports_imei_shards:
            virt_imei_shard_ranges = partition_utils.virt_imei_shard_bounds(num_physical_shards)
            for virt_imei_range_start, virt_imei_range_end in virt_imei_shard_ranges:
                staging_tbl_part_name = partition_utils.imei_shard_name(base_name=self._staging_tbl_name,
                                                                        virt_imei_range_start=virt_imei_range_start,
                                                                        virt_imei_range_end=virt_imei_range_end)
                historic_tbl_part_name = partition_utils.imei_shard_name(base_name=self._historic_tbl_name,
                                                                         virt_imei_range_start=virt_imei_range_start,
                                                                         virt_imei_range_end=virt_imei_range_end)

                futures_list.append(executor.submit(self._add_into_historic_single_shard,
                                                    staging_tbl_part_name,
                                                    historic_tbl_part_name,
                                                    job_start_time))
                futures_list.append(executor.submit(self._remove_from_historic_single_shard,
                                                    staging_tbl_part_name,
                                                    historic_tbl_part_name,
                                                    job_start_time))
                futures_list.append(executor.submit(self._update_historic_single_shard,
                                                    staging_tbl_part_name,
                                                    historic_tbl_part_name,
                                                    job_start_time))
        else:
            futures_list.append(executor.submit(self._add_into_historic_single_shard,
                                                self._staging_tbl_name,
                                                self._historic_tbl_name,
                                                job_start_time))
            futures_list.append(executor.submit(self._remove_from_historic_single_shard,
                                                self._staging_tbl_name,
                                                self._historic_tbl_name,
                                                job_start_time))
            futures_list.append(executor.submit(self._update_historic_single_shard,
                                                self._staging_tbl_name,
                                                self._historic_tbl_name,
                                                job_start_time))

        job_completion_state = dict(add=0, update=0, remove=0)
        rows_changed_state = dict(add=0, update=0, remove=0)
        for f in futures.as_completed(futures_list):
            change_type, rows_changed = f.result()
            job_completion_state[change_type] += 1
            rows_changed_state[change_type] += rows_changed
            if job_completion_state[change_type] == num_physical_shards:
                yield change_type, rows_changed_state[change_type]

    def _add_into_historic_single_shard(self, src_shard_name, dest_shard_name, job_start_time,
                                        change_type='add'):
        """Add entries into historic table (if import supports IMEI sharding, is called once per shard)."""
        with utils.create_db_connection(self._db_config) as conn, conn.cursor() as cursor:
            if self._supports_imei_shards:
                virtual_imei_col = sql.SQL(', virt_imei_shard')
                virtual_imei_data = sql.SQL(', calc_virt_imei_shard(imei_norm)')
            else:
                virtual_imei_col = sql.SQL('')
                virtual_imei_data = sql.SQL('')

            cursor.execute(sql.SQL("""INSERT INTO {dest_shard} ({all_fields},
                                                                start_date, end_date{virtual_imei_col})
                                           SELECT {all_fields}, %s, NULL{virtual_imei_data}
                                             FROM {src_shard} n
                                            WHERE change_type = %s
                                                  ON CONFLICT ({pks}) WHERE (end_date IS NULL)
                                                  DO NOTHING""")  # noqa Q441
                           .format(pks=self._pk_field_columns,
                                   dest_shard=sql.Identifier(dest_shard_name),
                                   src_shard=sql.Identifier(src_shard_name),
                                   all_fields=self._output_field_columns,
                                   virtual_imei_col=virtual_imei_col,
                                   virtual_imei_data=virtual_imei_data),
                           [job_start_time, change_type])

            return change_type, cursor.rowcount

    def _remove_from_historic_single_shard(self, src_shard_name, dest_shard_name, job_start_time,
                                           change_type='remove'):
        """Remove entries fron historic table (if import supports IMEI sharding, is called once per shard)."""
        with utils.create_db_connection(self._db_config) as conn, conn.cursor() as cursor:
            cursor.execute(sql.SQL("""UPDATE {dest_shard} AS o
                                         SET end_date = %s
                                       WHERE end_date IS NULL
                                         AND EXISTS (SELECT 1
                                                       FROM {src_shard} n
                                                      WHERE change_type = %s
                                                        AND {join_tables_on_pk_sql})""")
                           .format(dest_shard=sql.Identifier(dest_shard_name),
                                   src_shard=sql.Identifier(src_shard_name),
                                   join_tables_on_pk_sql=self._compare_pks_sql()),
                           [job_start_time, change_type])

            return change_type, cursor.rowcount

    def _update_historic_single_shard(self, src_shard_name, dest_shard_name, job_start_time):
        """Update entries in historic table (if import supports IMEI sharding, is called once per shard)."""
        self._remove_from_historic_single_shard(src_shard_name, dest_shard_name, job_start_time, change_type='update')
        return self._add_into_historic_single_shard(src_shard_name, dest_shard_name, job_start_time,
                                                    change_type='update')
