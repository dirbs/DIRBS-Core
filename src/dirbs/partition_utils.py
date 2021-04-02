"""
DIRBS module for managing partitioned tables, creating partitions, etc.

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

import hashlib

from psycopg2 import sql

import dirbs.utils as utils


class IndexMetadatum:
    """Class to represent metadata about an index."""

    def __init__(self, *, idx_cols, is_unique=False, partial_sql=None):
        """Constructor to initialize IndexMetadatum class.

        Arguments:
            idx_cols: list of column names to be indexed.
            is_unique: bool to indicate weather an indexed is to unique, default is False.
            partial_sql: optional partial sql string to be used with indexing, default is None.
        """
        self.idx_cols = idx_cols
        self.is_unique = is_unique
        self.partial_sql = partial_sql

    def idx_name(self, tbl_name):
        """
        Method to calculate an index name for a table and this set of columns.

        Arguments:
            tbl_name: name of the table to use in index name calculation for the same table.
        Returns:
            index name (idx_name)
        """
        hashed_tbl_name = hashlib.md5(tbl_name.encode('utf-8')).hexdigest()
        idx_name = '{0}_{1}_idx'.format(hashed_tbl_name, '_'.join(self.idx_cols))
        assert len(idx_name) < 64
        return idx_name


def physical_imei_shards(conn, *, tbl_name):
    """
    Iterates over all the IMEI shards for a given table name.

    Arguments:
        conn: dirbs db connection instance
        tbl_name: name of the table for iteration of imei shards
    Yields:
        name of the shard, start of the shard, end of the shard
    """
    num_physical_shards = num_physical_imei_shards(conn)
    virt_imei_shard_ranges = virt_imei_shard_bounds(num_physical_shards)
    for virt_imei_range_start, virt_imei_range_end in virt_imei_shard_ranges:
        shard_name = imei_shard_name(base_name=tbl_name,
                                     virt_imei_range_start=virt_imei_range_start,
                                     virt_imei_range_end=virt_imei_range_end)
        yield shard_name, virt_imei_range_start, virt_imei_range_end


def num_physical_imei_shards(conn):
    """
    Helper function to return the number of physical shards for IMEI-shared partitions.

    Arguments:
        conn: dirbs db connection object
    Returns:
        number of physical imei shard in the schema
    """
    with conn.cursor() as cursor:
        cursor.execute('SELECT phys_shards FROM schema_metadata')
        return cursor.fetchone()[0]


def _add_index_to_single_shard(conn, *, part_name, idx_metadatum, if_not_exists=False):
    """
    Method to dry out the addition of a single index to a single shard partition in a table.

    Arguments:
        conn: dirbs db connection object
        part_name: name of the partition
        idx_metadatum: idx_metadatum instance to return metadata about index
        if_not_exists:IF NOT EXISTS statement activation, default is False
    """
    with conn.cursor() as cursor:
        if if_not_exists:
            if_not_exists_sql = sql.SQL('IF NOT EXISTS ')
        else:
            if_not_exists_sql = sql.SQL('')

        idx_name = idx_metadatum.idx_name(part_name)

        if idx_metadatum.is_unique:
            unique_sql = sql.SQL('UNIQUE ')
        else:
            unique_sql = sql.SQL('')

        if idx_metadatum.partial_sql:
            partial_sql = sql.SQL(idx_metadatum.partial_sql)
        else:
            partial_sql = sql.SQL('')

        cursor.execute(
            sql.SQL(
                'CREATE {unique_sql}INDEX {if_not_exists_sql}{idx_name} ON {tbl_name}({cols}) {partial_sql}'
            ).format(unique_sql=unique_sql,
                     idx_name=sql.Identifier(idx_name),
                     tbl_name=sql.Identifier(part_name),
                     cols=sql.SQL(',').join(map(sql.Identifier, idx_metadatum.idx_cols)),
                     partial_sql=partial_sql,
                     if_not_exists_sql=if_not_exists_sql)
        )


def _add_indices_to_single_shard(conn, *, part_name, idx_metadata, if_not_exists=False):
    """
    Method to DRY out to addition of indexes to a single shard partition in a table.

    Arguments:
        conn: dirbs db connection instance
        part_name: name of partition to add index to
        idx_metadata: index metadata object, contains metadata about index
        if_not_exists: bool to activate 'IF NOT EXISTS', default False
    """
    for idx_metadatum in idx_metadata:
        _add_index_to_single_shard(conn, part_name=part_name, idx_metadatum=idx_metadatum, if_not_exists=if_not_exists)


def _add_indices_parallel_single_job(db_config, *, tbl_name, idx_metadatum, if_not_exists=False):
    """
    Job function called by add_indices_parallel.

    Arguments:
        db_config: dirbs db configuration object
        tbl_name: name of the table to add indices parallel
        idx_metadatum: index metadata object
        if_not_exists: bool to activate 'IF NOT EXISTS', default False
    """
    with utils.create_db_connection(db_config) as conn:
        _add_index_to_single_shard(conn, part_name=tbl_name, idx_metadatum=idx_metadatum, if_not_exists=if_not_exists)


def _queue_add_indices_parallel_job(conn, executor, db_config, *, tbl_name, idx_metadata, if_not_exists=False):
    """
    Function to queue and accumulate futures.

    Arguments:
        conn: dirbs db connection object
        executor: python executor class object
        db_config: dirbs db configuration object
        tbl_name: name of the table to be processed for the job
        idx_metadata: index metadata
        if_not_exists: bool to activate 'IF NOT EXISTS' statement, default False
    Returns:
        python futures object
    """
    futures = []
    if not utils.is_table_partitioned(conn, tbl_name):
        for idx_metadatum in idx_metadata:
            futures.append(executor.submit(_add_indices_parallel_single_job,
                                           db_config,
                                           tbl_name=tbl_name,
                                           idx_metadatum=idx_metadatum,
                                           if_not_exists=if_not_exists))
    else:
        for child_tbl_name in utils.child_table_names(conn, tbl_name):
            futures.extend(_queue_add_indices_parallel_job(conn,
                                                           executor,
                                                           db_config,
                                                           tbl_name=child_tbl_name,
                                                           idx_metadata=idx_metadata,
                                                           if_not_exists=if_not_exists))

    return futures


def add_indices_parallel(conn, executor, db_config, *, tbl_name, idx_metadata, if_not_exists=False):
    """Parallel version of add_indices.

    Can not be used with temporary tables and requires that transactions writing the table data be previously
    committed to avoid lock issues.

    Arguments:
        conn: dirbs db connection object
        executor: python future executor object
        db_config: dirbs db configuration object
        tbl_name: name of the table to be processed for the job
        idx_metadata: meta data of the index
        if_not_exists: bool to activate 'IF NOT EXISTS' statement
    """
    for future in _queue_add_indices_parallel_job(conn, executor, db_config, tbl_name=tbl_name,
                                                  idx_metadata=idx_metadata, if_not_exists=if_not_exists):
        # Simply wait for each job to complete
        future.result()


def add_indices(conn, *, tbl_name, idx_metadata, if_not_exists=False):
    """Helper that add indexes to a potentially partitioned table.

    If table is not partitioned, will just add the index to the physical table. Otherwise, will recursively
    go through the child tables until if find non-partitioned tables to add the indices to.

    Arguments:
        conn: dirbs db connection object
        tbl_name: name of the table to be indexed
        idx_metadata: metadata of the index
        if_not_exists: bool to activate 'IF NOT EXISTS' statement, default False
    """
    if not utils.is_table_partitioned(conn, tbl_name):
        _add_indices_to_single_shard(conn, part_name=tbl_name, idx_metadata=idx_metadata,
                                     if_not_exists=if_not_exists)
    else:
        for child_tbl_name in utils.child_table_names(conn, tbl_name):
            add_indices(conn, tbl_name=child_tbl_name, idx_metadata=idx_metadata, if_not_exists=if_not_exists)


def rename_table_and_indices(conn, *, old_tbl_name, new_tbl_name, idx_metadata=None):
    """Function to rename a potentially partitioned table and all associated indices on leaf tables.

    This will recursively rename the child tables and indices on them if the target table is partitioned, otherwise
    it will simply rename table and indices on the main table.

    Arguments:
        conn: dirbs db connection object
        old_tbl_name: old name of the table
        new_tbl_name: desired new name of the table
        idx_metadata: metadata about an index if exists, default is None
    """
    if idx_metadata is None:
        idx_metadata = []

    with conn.cursor() as cursor:
        cursor.execute(sql.SQL('ALTER TABLE {0} RENAME TO {1}').format(sql.Identifier(old_tbl_name),
                                                                       sql.Identifier(new_tbl_name)))

        if not utils.is_table_partitioned(conn, new_tbl_name):
            for idx_metadatum in idx_metadata:
                old_idx_name = idx_metadatum.idx_name(old_tbl_name)
                new_idx_name = idx_metadatum.idx_name(new_tbl_name)
                cursor.execute(sql.SQL('ALTER INDEX {0} RENAME TO {1}').format(
                    sql.Identifier(old_idx_name), sql.Identifier(new_idx_name)))
        else:
            for child_tbl_name in utils.child_table_names(conn, new_tbl_name):
                # Child tables should start with the old table name
                assert child_tbl_name.startswith(old_tbl_name)
                suffix = child_tbl_name[len(old_tbl_name):]
                dest_table_name = new_tbl_name + suffix
                rename_table_and_indices(conn, old_tbl_name=child_tbl_name, new_tbl_name=dest_table_name,
                                         idx_metadata=idx_metadata)


def create_imei_shard_partitions(conn, *, tbl_name, num_physical_shards=None, perms_func=None,
                                 fillfactor=100, temporary=False, unlogged=False):
    """
    Helper function try to dry out creating physical IMEI shards.

    Arguments:
        conn: dirbs db connection object
        tbl_name: name of the table to create imei based shards
        num_physical_shards: number of shards to create, default None, if None then system will pick default number
                             of imei shards
        perms_func: permissions function to supply, it will execute and apply permissions on the table
        fillfactor: fill factor of the partition, default value is set 100%
        temporary: bool to specify if the table to be partitioned is temporary, default is False
        unlogged: bool to specify if the table to be partitioned is unlogged, default is False
    """
    if num_physical_shards is None:
        num_physical_shards = num_physical_imei_shards(conn)

    with conn.cursor() as cursor:
        virt_imei_shard_ranges = virt_imei_shard_bounds(num_physical_shards)
        for virt_imei_range_start, virt_imei_range_end in virt_imei_shard_ranges:
            if temporary:
                temporary_sql = sql.SQL('TEMPORARY ')
            else:
                temporary_sql = sql.SQL('')

            if unlogged:
                unlogged_sql = sql.SQL('UNLOGGED ')
            else:
                unlogged_sql = sql.SQL('')

            part_name = imei_shard_name(base_name=tbl_name,
                                        virt_imei_range_start=virt_imei_range_start,
                                        virt_imei_range_end=virt_imei_range_end)
            assert len(part_name) < 64

            cursor.execute(
                sql.SQL(
                    """CREATE {temporary_sql}{unlogged_sql}TABLE {part_name} PARTITION OF {tbl_name}
                       FOR VALUES FROM (%s) TO (%s)
                       WITH (fillfactor = %s)
                    """
                ).format(temporary_sql=temporary_sql,
                         unlogged_sql=unlogged_sql,
                         part_name=sql.Identifier(part_name),
                         tbl_name=sql.Identifier(tbl_name)),
                [virt_imei_range_start, virt_imei_range_end, fillfactor]
            )
            if perms_func is not None:
                perms_func(conn, part_name=part_name)


def virt_imei_shard_bounds(num_physical_imei_shards):
    """
    Utility function to determine the virtual IMEI shard ranges that should be created for each physical shard.

    Arguments:
        num_physical_imei_shards: number of physical imei shards
    Returns:
        list of virt imei shards bounds
    """
    k, m = divmod(100, num_physical_imei_shards)
    return [(i * k + min(i, m), (i + 1) * k + min(i + 1, m)) for i in range(num_physical_imei_shards)]


def imei_shard_name(*, base_name, virt_imei_range_start, virt_imei_range_end):
    """
    Function to DRY out generation of IMEI shard partition names.

    Arguments:
        base_name: base name table
        virt_imei_range_start: start of the virt imei shard
        virt_imei_range_end: end of the virt imei shard
    Returns:
        name of the imei shard
    """
    return '{0}_{1}_{2}'.format(base_name, virt_imei_range_start, virt_imei_range_end - 1)


def _grant_perms_classification_state(conn, *, part_name):
    """
    Function to DRY out granting of permissions to classification_state partitions.

    Arguments:
        conn: dirbs db connection object
        part_name: name of the partition to grant permissions on
    """
    with conn.cursor() as cursor:
        part_id = sql.Identifier(part_name)
        cursor.execute(sql.SQL('GRANT SELECT ON {0} TO dirbs_core_listgen').format(part_id))
        cursor.execute(sql.SQL('GRANT SELECT ON {0} TO dirbs_core_report').format(part_id))
        cursor.execute(sql.SQL('GRANT SELECT ON {0} TO dirbs_core_api').format(part_id))
        cursor.execute(sql.SQL('GRANT SELECT, INSERT, UPDATE ON {0} TO dirbs_core_classify').format(part_id))


def repartition_classification_state(conn, *, num_physical_shards, src_filter_sql=None):
    """
    Function to repartition the classification_state table.

    Arguments:
        conn: dirbs db connection object
        num_physical_shards: number of physical shards to use
        src_filter_sql: custom filter sql, default is None
    """
    with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_power_user'):
        # Create parent partition
        cursor.execute(
            """CREATE TABLE classification_state_new (
                   LIKE classification_state INCLUDING DEFAULTS
                                             INCLUDING IDENTITY
                                             INCLUDING CONSTRAINTS
                                             INCLUDING STORAGE
                                             INCLUDING COMMENTS
               )
               PARTITION BY RANGE (virt_imei_shard)
            """
        )
        _grant_perms_classification_state(conn, part_name='classification_state_new')

        # Create child partitions
        create_imei_shard_partitions(conn, tbl_name='classification_state_new',
                                     num_physical_shards=num_physical_shards,
                                     perms_func=_grant_perms_classification_state, fillfactor=80)

        # Insert data from original partition
        base_sql = sql.SQL("""INSERT INTO classification_state_new
                                   SELECT *
                                     FROM classification_state""")
        if src_filter_sql is not None:
            insert_sql = sql.SQL('{0} {1}').format(base_sql, sql.SQL(src_filter_sql))
        else:
            insert_sql = base_sql

        cursor.execute(insert_sql)

        # Add in indexes to each partition
        idx_metadata = [
            IndexMetadatum(idx_cols=cols, is_unique=is_uniq, partial_sql=partial)
            for cols, is_uniq, partial in [
                (['row_id'], True, None),
                (['imei_norm', 'cond_name'], True, 'WHERE end_date IS NULL'),
                (['block_date'], False, 'WHERE end_date IS NULL'),
                (['cond_name'], False, 'WHERE end_date IS NULL')
            ]
        ]
        add_indices(conn, tbl_name='classification_state_new', idx_metadata=idx_metadata)

        # Drop old table, rename tables, indexes and constraints
        cursor.execute('ALTER SEQUENCE classification_state_row_id_seq OWNED BY classification_state_new.row_id')
        cursor.execute('DROP TABLE classification_state CASCADE')
        rename_table_and_indices(conn, old_tbl_name='classification_state_new',
                                 new_tbl_name='classification_state', idx_metadata=idx_metadata)


def _grant_perms_registration_list(conn, *, part_name):
    """
    Function to DRY out granting of permissions to registration_list partitions.

    Arguments:
        conn: dirbs db connection object
        part_name: name of the partition
    """
    with conn.cursor() as cursor:
        part_id = sql.Identifier(part_name)
        cursor.execute(sql.SQL('GRANT SELECT ON {0} TO dirbs_core_classify, dirbs_core_api').format(part_id))
        cursor.execute(sql.SQL('GRANT SELECT, INSERT, UPDATE ON {0} TO dirbs_core_import_registration_list')
                       .format(part_id))


def _grant_perms_barred_list(conn, *, part_name):
    """
    Method to DRY out granting of permissions to barred_list partitions.

    Arguments:
        conn: dirbs db connection object
        part_name: name of the partition
    """
    with conn.cursor() as cursor:
        part_id = sql.Identifier(part_name)
        cursor.execute(sql.SQL('GRANT SELECT ON {0} TO dirbs_core_classify, dirbs_core_api').format(part_id))
        cursor.execute(sql.SQL('GRANT SELECT, INSERT, UPDATE ON {0} TO dirbs_core_import_barred_list')
                       .format(part_id))


def _grant_perms_monitoring_list(conn, *, part_name):
    """
    Method to DRY out granting of permissions to monitoring_list partitions.

    Arguments:
        conn: dirbs db connection object
        part_name: name of the partition
    """
    with conn.cursor() as cursor:
        part_id = sql.Identifier(part_name)
        cursor.execute(sql.SQL('GRANT SELECT ON {0} TO dirbs_core_classify, dirbs_core_api').format(part_id))
        cursor.execute(sql.SQL('GRANT SELECT, INSERT, UPDATE ON {0} TO dirbs_core_import_monitoring_list')
                       .format(part_id))


def _grant_perms_association_list(conn, *, part_name):
    """
    Method to DRY out granting of permissions to device association list partitions.

    Arguments:
        conn: dirbs db connection object
        part_name: name of the partition
    """
    with conn.cursor() as cursor:
        part_id = sql.Identifier(part_name)
        cursor.execute(sql.SQL('GRANT SELECT ON {0} TO dirbs_core_classify, dirbs_core_api').format(part_id))
        cursor.execute(sql.SQL('GRANT SELECT, INSERT, UPDATE ON {0} TO dirbs_core_import_device_association_list')
                       .format(part_id))


def repartition_registration_list(conn, *, num_physical_shards):
    """
    Function to repartition the registration_list table.

    Arguments:
        conn: dirbs db connection object
        num_physical_shards: number of physical shard used to repartition
    """
    with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_power_user'):
        # Create parent partition
        cursor.execute(
            """CREATE TABLE historic_registration_list_new (
                   LIKE historic_registration_list INCLUDING DEFAULTS
                                                   INCLUDING IDENTITY
                                                   INCLUDING CONSTRAINTS
                                                   INCLUDING STORAGE
                                                   INCLUDING COMMENTS
               )
               PARTITION BY RANGE (virt_imei_shard)
            """
        )
        _grant_perms_registration_list(conn, part_name='historic_registration_list_new')

        # Create child partitions
        create_imei_shard_partitions(conn, tbl_name='historic_registration_list_new',
                                     num_physical_shards=num_physical_shards,
                                     perms_func=_grant_perms_registration_list, fillfactor=80)

        # Insert data from original partition
        cursor.execute("""INSERT INTO historic_registration_list_new
                               SELECT *
                                 FROM historic_registration_list""")

        # Add in indexes to each partition
        idx_metadata = [IndexMetadatum(idx_cols=['imei_norm'],
                                       is_unique=True,
                                       partial_sql='WHERE end_date IS NULL')]
        add_indices(conn, tbl_name='historic_registration_list_new', idx_metadata=idx_metadata)

        # Drop old view + table, rename tables, indexes and constraints
        cursor.execute('DROP VIEW registration_list')
        cursor.execute('DROP TABLE historic_registration_list CASCADE')
        rename_table_and_indices(conn, old_tbl_name='historic_registration_list_new',
                                 new_tbl_name='historic_registration_list', idx_metadata=idx_metadata)

        cursor.execute("""CREATE OR REPLACE VIEW registration_list AS
                              SELECT imei_norm, make, model, status, virt_imei_shard, model_number, brand_name,
                                     device_type, radio_interface, device_id
                                FROM historic_registration_list
                               WHERE end_date IS NULL WITH CHECK OPTION""")
        cursor.execute("""GRANT SELECT ON registration_list
                          TO dirbs_core_classify, dirbs_core_api, dirbs_core_import_registration_list""")


def _grant_perms_stolen_list(conn, *, part_name):
    """
    Function to DRY out granting of permissions to stolen_list partitions.

    Arguments:
        conn: dirbs db connection object
        part_name: name of the partition
    """
    with conn.cursor() as cursor:
        part_id = sql.Identifier(part_name)
        cursor.execute(sql.SQL('GRANT SELECT ON {0} TO dirbs_core_classify, dirbs_core_api').format(part_id))
        cursor.execute(sql.SQL('GRANT SELECT, INSERT, UPDATE ON {0} TO dirbs_core_import_stolen_list')
                       .format(part_id))


def repartition_stolen_list(conn, *, num_physical_shards):
    """
    Function to repartition the stolen_list table.

    Arguments:
        conn: dirbs db connection object
        num_physical_shards: number of physical shards to repartition
    """
    with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_power_user'):
        # Create parent partition
        cursor.execute(
            """CREATE TABLE historic_stolen_list_new (
                   LIKE historic_stolen_list INCLUDING DEFAULTS
                                             INCLUDING IDENTITY
                                             INCLUDING CONSTRAINTS
                                             INCLUDING STORAGE
                                             INCLUDING COMMENTS
               )
               PARTITION BY RANGE (virt_imei_shard)
            """
        )
        _grant_perms_stolen_list(conn, part_name='historic_stolen_list_new')

        # Create child partitions
        create_imei_shard_partitions(conn, tbl_name='historic_stolen_list_new',
                                     num_physical_shards=num_physical_shards,
                                     perms_func=_grant_perms_stolen_list, fillfactor=80)

        # Insert data from original partition
        cursor.execute("""INSERT INTO historic_stolen_list_new
                               SELECT *
                                 FROM historic_stolen_list""")

        # Add in indexes to each partition
        idx_metadata = [IndexMetadatum(idx_cols=['imei_norm'],
                                       is_unique=True,
                                       partial_sql='WHERE end_date IS NULL')]
        add_indices(conn, tbl_name='historic_stolen_list_new', idx_metadata=idx_metadata)

        # Drop old view + table, rename tables, indexes and constraints
        cursor.execute('DROP VIEW stolen_list')
        cursor.execute('DROP TABLE historic_stolen_list CASCADE')
        rename_table_and_indices(conn, old_tbl_name='historic_stolen_list_new',
                                 new_tbl_name='historic_stolen_list', idx_metadata=idx_metadata)
        cursor.execute("""CREATE VIEW stolen_list AS
                               SELECT imei_norm, reporting_date, status, virt_imei_shard
                                 FROM historic_stolen_list
                                WHERE end_date IS NULL WITH CHECK OPTION""")
        cursor.execute("""GRANT SELECT ON stolen_list
                          TO dirbs_core_classify, dirbs_core_api, dirbs_core_import_stolen_list""")


def _grant_perms_pairing_list(conn, *, part_name):
    """
    Function to DRY out granting of permissions to pairing_list partitions.

    Arguments:
        conn: dirbs db connection object
        part_name: name of the partition
    """
    with conn.cursor() as cursor:
        part_id = sql.Identifier(part_name)
        cursor.execute(sql.SQL("""GRANT SELECT ON {0}
                                  TO dirbs_core_listgen, dirbs_core_report, dirbs_core_api""").format(part_id))
        cursor.execute(sql.SQL('GRANT SELECT, INSERT, UPDATE ON {0} TO dirbs_core_import_pairing_list')
                       .format(part_id))


def repartition_pairing_list(conn, *, num_physical_shards):
    """
    Function to repartition the pairing_list table.

    Arguments:
        conn: dirbs db connection object
        num_physical_shards: number of shards to repartition table on
    """
    with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_power_user'):
        cursor.execute(
            """CREATE TABLE historic_pairing_list_new (
                   LIKE historic_pairing_list INCLUDING DEFAULTS
                                              INCLUDING IDENTITY
                                              INCLUDING CONSTRAINTS
                                              INCLUDING STORAGE
                                              INCLUDING COMMENTS
               )
               PARTITION BY RANGE (virt_imei_shard)
            """
        )
        _grant_perms_pairing_list(conn, part_name='historic_pairing_list_new')

        # Create child partitions
        create_imei_shard_partitions(conn, tbl_name='historic_pairing_list_new',
                                     num_physical_shards=num_physical_shards,
                                     perms_func=_grant_perms_pairing_list, fillfactor=80)

        # Insert data from original partition
        cursor.execute("""INSERT INTO historic_pairing_list_new
                               SELECT *
                                 FROM historic_pairing_list""")

        # Add in indexes to each partition
        idx_metadata = [IndexMetadatum(idx_cols=['imei_norm', 'imsi'],
                                       is_unique=True,
                                       partial_sql='WHERE end_date IS NULL')]
        add_indices(conn, tbl_name='historic_pairing_list_new', idx_metadata=idx_metadata)

        # Drop old view + table, rename tables, indexes and constraints
        cursor.execute('DROP VIEW pairing_list')
        cursor.execute('DROP TABLE historic_pairing_list CASCADE')
        rename_table_and_indices(conn, old_tbl_name='historic_pairing_list_new',
                                 new_tbl_name='historic_pairing_list', idx_metadata=idx_metadata)
        cursor.execute("""CREATE VIEW pairing_list AS
                               SELECT imei_norm, imsi, virt_imei_shard
                                 FROM historic_pairing_list
                                WHERE end_date IS NULL WITH CHECK OPTION""")
        cursor.execute("""GRANT SELECT ON pairing_list
                          TO dirbs_core_listgen, dirbs_core_report, dirbs_core_api, dirbs_core_import_pairing_list""")


def _grant_perms_list(conn, *, part_name):
    """
    Function to DRY out granting of permissions to list (black/notifications/exceptions) partitions.

    Arguments:
        conn: dirbs db connection object
        part_name: name of the partition
    """
    pass


def repartition_blacklist(conn, *, num_physical_shards, src_filter_sql=None):
    """
    Function to repartition the blacklist table.

    Arguments:
        conn: dirbs db connection object
        num_physical_shards: number of shard to partition with
        src_filter_sql: custom filtration sql, default None
    """
    with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_listgen'):
        # Create parent partition
        cursor.execute(
            """CREATE TABLE blacklist_new (
                   LIKE blacklist INCLUDING DEFAULTS
                                  INCLUDING IDENTITY
                                  INCLUDING CONSTRAINTS
                                  INCLUDING STORAGE
                                  INCLUDING COMMENTS
               )
               PARTITION BY RANGE (virt_imei_shard)
            """
        )

        # Create child partitions
        create_imei_shard_partitions(conn, tbl_name='blacklist_new', num_physical_shards=num_physical_shards,
                                     perms_func=_grant_perms_list, fillfactor=80)

        # Insert data from original partition
        base_sql = sql.SQL("""INSERT INTO blacklist_new
                                   SELECT *
                                     FROM blacklist""")
        if src_filter_sql is not None:
            insert_sql = sql.SQL('{0} {1}').format(base_sql, sql.SQL(src_filter_sql))
        else:
            insert_sql = base_sql
        cursor.execute(insert_sql)

        # Add in indexes to each partition
        idx_metadata = [
            IndexMetadatum(idx_cols=cols, is_unique=is_uniq, partial_sql=partial)
            for cols, is_uniq, partial in [
                (['imei_norm'], True, 'WHERE end_run_id IS NULL'),
                (['end_run_id'], False, None),
                (['start_run_id'], False, None)
            ]
        ]
        add_indices(conn, tbl_name='blacklist_new', idx_metadata=idx_metadata)

        # Drop old table, rename tables, indexes and constraints
        cursor.execute('ALTER SEQUENCE blacklist_row_id_seq OWNED BY blacklist_new.row_id')
        cursor.execute('DROP TABLE blacklist CASCADE')
        rename_table_and_indices(conn, old_tbl_name='blacklist_new',
                                 new_tbl_name='blacklist', idx_metadata=idx_metadata)


def per_mno_lists_partition(*, operator_id, list_type, suffix=''):
    """
    Function to DRY out the name of a per-MNO lists partition for a given operator_id.

    Arguments:
        operator_id: str operator id
        list_type: type of the list
        suffix: suffix string default empty
    Returns:
        name of the partition
    """
    return '{0}_lists{1}_{2}'.format(list_type, suffix, operator_id)


def create_per_mno_lists_partition(conn, *, operator_id, parent_tbl_name, tbl_name, num_physical_shards=None,
                                   unlogged=False, fillfactor=80):
    """
    Function to DRY out creation of a new operator partition for notifications_lists.

    Arguments:
        conn: dirbs db connection object
        operator_id: operator id to create partition of
        parent_tbl_name: name of the parent table to create partition
        tbl_name: name of the current table
        num_physical_shards: number of physical shards, default None
        unlogged: bool to indicate if it is an unlogged table
        fillfactor: fill factor of the partition, default 80%
    """
    assert len(tbl_name) < 64

    if num_physical_shards is None:
        num_physical_shards = num_physical_imei_shards(conn)

    if unlogged:
        unlogged_sql = sql.SQL('UNLOGGED ')
    else:
        unlogged_sql = sql.SQL('')

    with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_listgen'):
        cursor.execute(
            sql.SQL(
                """CREATE {unlogged}TABLE {0} PARTITION OF {1}
                   FOR VALUES IN (%s) PARTITION BY RANGE (virt_imei_shard)
                """
            ).format(sql.Identifier(tbl_name), sql.Identifier(parent_tbl_name), unlogged=unlogged_sql),
            [operator_id]
        )
        _grant_perms_list(conn, part_name=tbl_name)

        # Create child partitions
        create_imei_shard_partitions(conn, tbl_name=tbl_name, num_physical_shards=num_physical_shards,
                                     perms_func=_grant_perms_list, fillfactor=fillfactor)


def notifications_lists_indices():
    """Index metadata for notifications lists."""
    return [
        IndexMetadatum(idx_cols=cols, is_unique=is_uniq, partial_sql=partial)
        for cols, is_uniq, partial in [
            (['imei_norm', 'imsi', 'msisdn'], True, 'WHERE end_run_id IS NULL'),
            (['end_run_id'], False, None),
            (['start_run_id'], False, None)
        ]
    ]


def repartition_notifications_lists(conn, *, num_physical_shards, src_filter_sql=None):
    """
    Function to repartition the notifications_lists table.

    Arguments:
        conn: dirbs db connection instance
        num_physical_shards: number of physical shards
        src_filter_sql: custom filter sql, default None
    """
    with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_listgen'):
        # Create parent partition
        cursor.execute(
            """CREATE TABLE notifications_lists_new (
                   LIKE notifications_lists INCLUDING DEFAULTS
                                            INCLUDING IDENTITY
                                            INCLUDING CONSTRAINTS
                                            INCLUDING STORAGE
                                            INCLUDING COMMENTS
               )
               PARTITION BY LIST (operator_id)
            """
        )
        _grant_perms_list(conn, part_name='notifications_lists_new')

        # Work out who the operators are
        imei_shard_names = utils.child_table_names(conn, 'notifications_lists')
        operators = [x.operator_id for x in utils.table_invariants_list(conn, imei_shard_names, ['operator_id'])]

        # Create child partitions (operator at top level, then IMEI-sharded)
        for op_id in operators:
            tbl_name = per_mno_lists_partition(operator_id=op_id, suffix='_new', list_type='notifications')
            create_per_mno_lists_partition(conn, parent_tbl_name='notifications_lists_new', tbl_name=tbl_name,
                                           operator_id=op_id, num_physical_shards=num_physical_shards)

        # Insert data from original partition
        base_sql = sql.SQL("""INSERT INTO notifications_lists_new
                                   SELECT *
                                     FROM notifications_lists""")
        if src_filter_sql is not None:
            insert_sql = sql.SQL('{0} {1}').format(base_sql, sql.SQL(src_filter_sql))
        else:
            insert_sql = base_sql
        cursor.execute(insert_sql)

        # Add in indexes to each partition
        add_indices(conn, tbl_name='notifications_lists_new', idx_metadata=notifications_lists_indices())

        # Drop old table after assigning ownership of sequence
        cursor.execute('ALTER SEQUENCE notifications_lists_row_id_seq OWNED BY notifications_lists_new.row_id')
        cursor.execute('DROP TABLE notifications_lists CASCADE')

        #  Rename tables, indexes and constraints
        rename_table_and_indices(conn, old_tbl_name='notifications_lists_new',
                                 new_tbl_name='notifications_lists', idx_metadata=notifications_lists_indices())


def exceptions_lists_indices():
    """Index metadata for exceptions lists."""
    return [
        IndexMetadatum(idx_cols=cols, is_unique=is_uniq, partial_sql=partial)
        for cols, is_uniq, partial in [
            (['imei_norm', 'imsi', 'msisdn'], True, 'WHERE end_run_id IS NULL'),
            (['end_run_id'], False, None),
            (['start_run_id'], False, None)
        ]
    ]


def repartition_exceptions_lists(conn, *, num_physical_shards, src_filter_sql=None):
    """
    Function to repartition the exceptions_lists table.

    Arguments:
        conn: dirbs db connection instance
        num_physical_shards: number of physical shards to repartition
        src_filter_sql: custom filter sql, default None
    """
    with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_listgen'):
        # Create parent partition
        cursor.execute(
            """CREATE TABLE exceptions_lists_new (
                   LIKE exceptions_lists INCLUDING DEFAULTS
                                         INCLUDING IDENTITY
                                         INCLUDING CONSTRAINTS
                                         INCLUDING STORAGE
                                         INCLUDING COMMENTS
               )
               PARTITION BY LIST (operator_id)
            """
        )
        _grant_perms_list(conn, part_name='exceptions_lists_new')

        # Work out who the operators are
        imei_shard_names = utils.child_table_names(conn, 'exceptions_lists')
        operators = [x.operator_id for x in utils.table_invariants_list(conn, imei_shard_names, ['operator_id'])]

        # Create child partitions (operator at top level, then IMEI-sharded)
        for op_id in operators:
            tbl_name = per_mno_lists_partition(operator_id=op_id, suffix='_new', list_type='exceptions')
            create_per_mno_lists_partition(conn, parent_tbl_name='exceptions_lists_new', tbl_name=tbl_name,
                                           operator_id=op_id, num_physical_shards=num_physical_shards)

        # Insert data from original partition
        base_sql = sql.SQL("""INSERT INTO exceptions_lists_new
                                   SELECT *
                                     FROM exceptions_lists""")
        if src_filter_sql is not None:
            insert_sql = sql.SQL('{0} {1}').format(base_sql, sql.SQL(src_filter_sql))
        else:
            insert_sql = base_sql
        cursor.execute(insert_sql)

        # Add in indexes to each partition
        add_indices(conn, tbl_name='exceptions_lists_new', idx_metadata=exceptions_lists_indices())

        # Drop old table, after assigning sequence to new table
        cursor.execute('ALTER SEQUENCE exceptions_lists_row_id_seq OWNED BY exceptions_lists_new.row_id')
        cursor.execute('DROP TABLE exceptions_lists CASCADE')

        #  Rename tables, indexes and constraints
        rename_table_and_indices(conn, old_tbl_name='exceptions_lists_new',
                                 new_tbl_name='exceptions_lists', idx_metadata=exceptions_lists_indices())


def _grant_perms_network_imeis(conn, *, part_name):
    """
    Function to DRY out granting of permissions to network_imeis partitions.

    Arguments:
        conn: dirbs db connection object
        part_name: partition name
    """
    with conn.cursor() as cursor:
        part_id = sql.Identifier(part_name)
        cursor.execute(sql.SQL("""GRANT SELECT ON {0}
                                  TO dirbs_core_classify, dirbs_core_report, dirbs_core_api""").format(part_id))


def repartition_network_imeis(conn, *, num_physical_shards):
    """
    Function to repartition the network_imeis table.

    Arguments:
        conn: dirbs db connection object
        num_physical_shards: number of physical shards
    """
    with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_import_operator'):
        # Create parent partition
        cursor.execute(
            """CREATE TABLE network_imeis_new (
                   LIKE network_imeis INCLUDING DEFAULTS
                                      INCLUDING IDENTITY
                                      INCLUDING CONSTRAINTS
                                      INCLUDING STORAGE
                                      INCLUDING COMMENTS
               )
               PARTITION BY RANGE (virt_imei_shard)
            """
        )
        _grant_perms_network_imeis(conn, part_name='network_imeis_new')

        # Create child partitions
        create_imei_shard_partitions(conn, tbl_name='network_imeis_new', num_physical_shards=num_physical_shards,
                                     perms_func=_grant_perms_network_imeis, fillfactor=80)

        # Insert data from original partition
        cursor.execute("""INSERT INTO network_imeis_new
                               SELECT *
                                 FROM network_imeis""")

        # Add in indexes to each partition
        idx_metadata = [
            IndexMetadatum(idx_cols=cols, is_unique=is_uniq, partial_sql=partial)
            for cols, is_uniq, partial in [
                (['imei_norm'], True, None),
                (['first_seen'], False, None)
            ]
        ]
        add_indices(conn, tbl_name='network_imeis_new', idx_metadata=idx_metadata)

        # Drop old table, rename tables, indexes and constraints
        cursor.execute('DROP TABLE network_imeis CASCADE')
        rename_table_and_indices(conn, old_tbl_name='network_imeis_new',
                                 new_tbl_name='network_imeis', idx_metadata=idx_metadata)


def _grant_perms_monthly_network_triplets(conn, *, part_name):
    """
    Function to DRY out granting of permissions to monthly_network_triplet partitions.

    Arguments:
        conn: dirbs db connection object
        part_name: partition name
    """
    with conn.cursor() as cursor:
        part_id = sql.Identifier(part_name)
        cursor.execute(sql.SQL("""GRANT SELECT ON {0}
                                  TO dirbs_core_classify, dirbs_core_report, dirbs_core_listgen,
                                     dirbs_core_api""").format(part_id))


def monthly_network_triplets_country_partition(*, month, year, suffix=''):
    """
    Function to DRY out the name of a monthly_network_triplets_country partition for a month and year.

    Arguments:
        month: partition month
        year: partition year
        suffix: suffix string, default empty
    Returns:
        name of the monthly_network_triplets_country partition for month/year
    """
    return 'monthly_network_triplets_country{0}_{1:d}_{2:02d}'.format(suffix, year, month)


def monthly_network_triplets_per_mno_partition(*, operator_id, month, year, suffix=''):
    """
    Function to DRY out the name of a monthly_network_triplets_per_mno partition for a month and year.

    Arguments:
        operator_id: id of the operator
        month: partition month
        year: partition year
        suffix: suffix string, default empty
    Returns:
        name of the monthly_network_triplets_per_mno partition for month/year
    """
    if month is None and year is None:
        return 'monthly_network_triplets_per_mno{0}_{1}'.format(suffix, operator_id)
    else:
        return 'monthly_network_triplets_per_mno{0}_{1}_{2:d}_{3:02d}'.format(suffix, operator_id, year, month)


def create_monthly_network_triplets_country_partition(conn, *, month, year, suffix='', num_physical_shards=None,
                                                      fillfactor=45):
    """
    Function to DRY out creation of a new month/year partition for monthly_network_triplets_country.

    Arguments:
        conn: dirbs db connection object
        month: partition month
        year: partition year
        suffix: suffix string, default empty
        num_physical_shards: number of physical shards to apply, default none
        fillfactor: fill factor of the partition, default 45%
    """
    if num_physical_shards is None:
        num_physical_shards = num_physical_imei_shards(conn)

    with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_import_operator'):
        part_name = monthly_network_triplets_country_partition(month=month, year=year, suffix=suffix)
        assert len(part_name) < 64

        parent_tbl_name = 'monthly_network_triplets_country{0}'.format(suffix)
        cursor.execute(
            sql.SQL(
                """CREATE TABLE {0} PARTITION OF {1}
                   FOR VALUES FROM %s TO %s PARTITION BY RANGE (virt_imei_shard)
                """
            ).format(sql.Identifier(part_name), sql.Identifier(parent_tbl_name)),
            [(year, month), (year, month + 1)]
        )
        _grant_perms_monthly_network_triplets(conn, part_name=part_name)

        # Create child partitions
        create_imei_shard_partitions(conn, tbl_name=part_name, num_physical_shards=num_physical_shards,
                                     perms_func=_grant_perms_monthly_network_triplets, fillfactor=fillfactor)


def monthly_network_triplets_country_indices():
    """Index metadata for monthly_network_triplets_country partitions."""
    return [
        IndexMetadatum(idx_cols=cols, is_unique=is_uniq, partial_sql=partial)
        for cols, is_uniq, partial in [
            (['triplet_hash'], True, None),
            (['imei_norm'], False, None),
            (['msisdn'], False, None)
        ]
    ]


def create_monthly_network_triplets_per_mno_partition(conn, *, operator_id, month, year, suffix='',
                                                      num_physical_shards=None, fillfactor=45):
    """
    Function to DRY out creation of a new month/year partition for monthly_network_triplets_per_mno.

    Arguments:
        operator_id: id of the operator/mno
        conn: dirbs db connection object
        month: partition month
        year: partition year
        suffix: suffix string, default empty
        num_physical_shards: number of physical shards to apply, default none
        fillfactor: fill factor of the partition, default 45%
    """
    if num_physical_shards is None:
        num_physical_shards = num_physical_imei_shards(conn)

    with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_import_operator'):
        parent_tbl_name = 'monthly_network_triplets_per_mno{0}'.format(suffix)
        op_part_name = monthly_network_triplets_per_mno_partition(operator_id=operator_id, month=None,
                                                                  year=None, suffix=suffix)
        assert len(op_part_name) < 64

        cursor.execute(
            sql.SQL(
                """CREATE TABLE IF NOT EXISTS {0} PARTITION OF {1}
                   FOR VALUES IN (%s) PARTITION BY RANGE (triplet_year, triplet_month)
                """
            ).format(sql.Identifier(op_part_name), sql.Identifier(parent_tbl_name)),
            [operator_id]
        )
        _grant_perms_monthly_network_triplets(conn, part_name=op_part_name)

        part_name = monthly_network_triplets_per_mno_partition(operator_id=operator_id,
                                                               month=month, year=year, suffix=suffix)
        assert len(part_name) < 64

        cursor.execute(
            sql.SQL(
                """CREATE TABLE {0} PARTITION OF {1}
                   FOR VALUES FROM %s TO %s PARTITION BY RANGE (virt_imei_shard)
                """
            ).format(sql.Identifier(part_name), sql.Identifier(op_part_name)),
            [(year, month), (year, month + 1)]
        )
        _grant_perms_monthly_network_triplets(conn, part_name=part_name)

        # Create child partitions
        create_imei_shard_partitions(conn, tbl_name=part_name, num_physical_shards=num_physical_shards,
                                     perms_func=_grant_perms_monthly_network_triplets, fillfactor=fillfactor)


def monthly_network_triplets_per_mno_indices():
    """Index metadata for monthly_network_triplets_per_mno partitions."""
    return [
        IndexMetadatum(idx_cols=cols, is_unique=is_uniq, partial_sql=partial)
        for cols, is_uniq, partial in [
            (['triplet_hash'], True, None),
            (['imei_norm'], False, None),
        ]
    ]


def repartition_monthly_network_triplets(conn, *, num_physical_shards):
    """
    Function to repartition the monthly_network_triplets_country and monthly_network_triplets_country tables.

    Arguments:
        conn: dirbs db connection object
        num_physical_shards: number of physical shards to apply
    """
    with conn.cursor() as cursor, utils.db_role_setter(conn, role_name='dirbs_core_import_operator'):
        # Create parent partitions
        cursor.execute(
            """CREATE TABLE monthly_network_triplets_country_new (
                   LIKE monthly_network_triplets_country INCLUDING DEFAULTS
                                                         INCLUDING IDENTITY
                                                         INCLUDING CONSTRAINTS
                                                         INCLUDING STORAGE
                                                         INCLUDING COMMENTS
               ) PARTITION BY RANGE (triplet_year, triplet_month)
            """
        )
        _grant_perms_monthly_network_triplets(conn, part_name='monthly_network_triplets_country_new')

        cursor.execute(
            """CREATE TABLE monthly_network_triplets_per_mno_new (
                   LIKE monthly_network_triplets_per_mno INCLUDING DEFAULTS
                                                         INCLUDING IDENTITY
                                                         INCLUDING CONSTRAINTS
                                                         INCLUDING STORAGE
                                                         INCLUDING COMMENTS
               ) PARTITION BY LIST (operator_id)
            """
        )
        _grant_perms_monthly_network_triplets(conn, part_name='monthly_network_triplets_per_mno_new')

        # Work out what year-month tuples we have
        country_monthly_partitions = utils.child_table_names(conn, 'monthly_network_triplets_country')
        country_year_month_tuples = [(x.triplet_year, x.triplet_month)
                                     for x in utils.table_invariants_list(conn,
                                                                          country_monthly_partitions,
                                                                          ['triplet_year', 'triplet_month'])]

        operator_partitions = utils.child_table_names(conn, 'monthly_network_triplets_per_mno')
        operator_monthly_partitions = set()
        for op_partition in operator_partitions:
            operator_monthly_partitions.update(utils.child_table_names(conn, op_partition))
        mno_year_month_tuples = [(x.operator_id, x.triplet_year, x.triplet_month)
                                 for x in utils.table_invariants_list(conn,
                                                                      operator_monthly_partitions,
                                                                      ['operator_id',
                                                                       'triplet_year',
                                                                       'triplet_month'])]

        latest_year_month = None
        # Sort year month tuples and get the maximum year month combination.
        country_year_month_tuples = sorted(country_year_month_tuples, key=lambda x: (x[0], x[1]), reverse=True)
        if len(country_year_month_tuples) > 0:
            latest_year_month = country_year_month_tuples[0]

        # Create child partitions at country level
        for year, month in country_year_month_tuples:
            # Fillfactor is 45 for most recent month since it will likely still be updated. For older months we
            # pack tightly to ensure optimal usage of disk space and optimal scan performance
            latest_year, latest_month = latest_year_month
            fillfactor = 45 if year == latest_year and month == latest_month else 100
            create_monthly_network_triplets_country_partition(conn, month=month, year=year, suffix='_new',
                                                              num_physical_shards=num_physical_shards,
                                                              fillfactor=fillfactor)

        # Create child partitions at per-MNO level
        for op, year, month in mno_year_month_tuples:
            # Fillfactor is 45 for most recent month since it will likely still be updated. For older months we
            # pack tightly to ensure optimal usage of disk space and optimal scan performance
            latest_year, latest_month = latest_year_month
            fillfactor = 45 if year == latest_year and month == latest_month else 100
            create_monthly_network_triplets_per_mno_partition(conn, operator_id=op, month=month, year=year,
                                                              suffix='_new', num_physical_shards=num_physical_shards,
                                                              fillfactor=fillfactor)

        # Populate country-level table from old table
        cursor.execute("""INSERT INTO monthly_network_triplets_country_new
                               SELECT *
                                 FROM monthly_network_triplets_country""")

        # Populate per-MNO-level table from old table
        cursor.execute("""INSERT INTO monthly_network_triplets_per_mno_new
                               SELECT *
                                 FROM monthly_network_triplets_per_mno""")

        # Add in indexes
        add_indices(conn, tbl_name='monthly_network_triplets_country_new',
                    idx_metadata=monthly_network_triplets_country_indices())
        add_indices(conn, tbl_name='monthly_network_triplets_per_mno_new',
                    idx_metadata=monthly_network_triplets_per_mno_indices())

        # Drop old tables
        cursor.execute('DROP TABLE monthly_network_triplets_country CASCADE')
        cursor.execute('DROP TABLE monthly_network_triplets_per_mno CASCADE')

        # Renames tables
        rename_table_and_indices(conn,
                                 old_tbl_name='monthly_network_triplets_country_new',
                                 new_tbl_name='monthly_network_triplets_country',
                                 idx_metadata=monthly_network_triplets_country_indices())
        rename_table_and_indices(conn,
                                 old_tbl_name='monthly_network_triplets_per_mno_new',
                                 new_tbl_name='monthly_network_triplets_per_mno',
                                 idx_metadata=monthly_network_triplets_per_mno_indices())

        cursor.execute("""CREATE OR REPLACE VIEW operator_data AS
                          SELECT sq.connection_date,
                                 sq.imei_norm,
                                 sq.imsi,
                                 sq.msisdn,
                                 sq.operator_id
                            FROM (SELECT make_date(nt.triplet_year::integer,
                                         nt.triplet_month::integer,
                                         dom.dom) AS connection_date,
                                         nt.imei_norm,
                                         nt.imsi,
                                         nt.msisdn,
                                         nt.operator_id
                                    FROM generate_series(1, 31) dom(dom),
                                         monthly_network_triplets_per_mno nt
                                   WHERE (nt.date_bitmask & (1 << (dom.dom - 1))) <> 0) sq""")
        cursor.execute("""CREATE VIEW monthly_network_triplets_country_no_null_imeis AS
                          SELECT *
                            FROM monthly_network_triplets_country
                           WHERE imei_norm IS NOT NULL""")
        cursor.execute("""CREATE VIEW monthly_network_triplets_per_mno_no_null_imeis AS
                          SELECT *
                            FROM monthly_network_triplets_per_mno
                           WHERE imei_norm IS NOT NULL""")

        cursor.execute(sql.SQL('GRANT SELECT ON operator_data TO dirbs_core_base'))
        for role in ['dirbs_core_listgen', 'dirbs_core_classify', 'dirbs_core_report', 'dirbs_core_api']:
            cursor.execute(sql.SQL("""GRANT SELECT ON monthly_network_triplets_country_no_null_imeis
                                      TO {0}""").format(sql.Identifier(role)))
            cursor.execute(sql.SQL("""GRANT SELECT ON monthly_network_triplets_per_mno_no_null_imeis
                                      TO {0}""").format(sql.Identifier(role)))

        cursor.execute("""CREATE VIEW monthly_network_triplets_with_invalid_data_flags AS
                               SELECT nt.*,
                                      nt.imei_norm IS NULL AS is_null_imei,
                                      is_unclean_imei(nt.imei_norm) AS is_unclean_imei,
                                      nt.imsi IS NULL AS is_null_imsi,
                                      is_unclean_imsi(nt.imsi) AS is_unclean_imsi,
                                      nt.msisdn IS NULL AS is_null_msisdn
                                 FROM monthly_network_triplets_per_mno nt""")
