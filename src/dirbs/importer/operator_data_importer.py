"""
Code for importing MNO data sets into DIRBS Core.

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
from concurrent import futures
from collections import defaultdict
from functools import partial

from psycopg2 import sql
from dateutil.rrule import rrule, MONTHLY

import dirbs.importer.exceptions as exceptions
from dirbs.utils import create_db_connection, hash_string_64bit, table_exists_sql, db_role_setter
from dirbs.importer.abstract_importer import AbstractImporter
import dirbs.metadata as metadata
import dirbs.importer.importer_utils as importer_utils
import dirbs.partition_utils as partition_utils


class OperatorDataImporter(AbstractImporter):
    """Operator data importer."""

    def __init__(self,
                 operator_id,
                 mcc_mnc_pairs,
                 cc,
                 *args,
                 null_imei_threshold=None,
                 null_imsi_threshold=None,
                 null_msisdn_threshold=None,
                 null_rat_threshold=None,
                 null_threshold=0.05,
                 unclean_imei_threshold=0.05,
                 unclean_imsi_threshold=0.05,
                 unclean_threshold=0.05,
                 out_of_region_imsi_threshold=0.1,
                 out_of_region_msisdn_threshold=0.1,
                 out_of_region_threshold=0.1,
                 non_home_network_threshold=0.2,
                 historic_imei_threshold=0.9,
                 historic_imsi_threshold=0.9,
                 historic_msisdn_threshold=0.9,
                 leading_zero_suspect_limit=0.5,
                 perform_msisdn_import=True,
                 perform_rat_import=True,
                 perform_file_daterange_check=True,
                 perform_leading_zero_check=True,
                 perform_null_checks=True,
                 perform_unclean_checks=True,
                 perform_region_checks=True,
                 perform_home_network_check=True,
                 perform_historic_checks=True,
                 perform_auto_analyze=True,
                 **kwargs):
        """Constructor."""
        assert mcc_mnc_pairs is not None and len(mcc_mnc_pairs) > 0
        assert cc is not None and len(cc) > 0
        super().__init__(*args, **kwargs)
        assert operator_id == operator_id.lower()
        self._operator_id = operator_id
        # NULL check thresholds
        self._null_imei_threshold = null_imei_threshold
        self._null_imsi_threshold = null_imsi_threshold
        self._null_msisdn_threshold = null_msisdn_threshold
        self._null_rat_threshold = null_rat_threshold
        self._null_threshold = null_threshold
        # Unclean data thresholds
        self._unclean_imei_threshold = unclean_imei_threshold
        self._unclean_imsi_threshold = unclean_imsi_threshold
        self._unclean_threshold = unclean_threshold
        # Regional thresholds
        self._out_of_region_imsi_threshold = out_of_region_imsi_threshold
        self._out_of_region_msisdn_threshold = out_of_region_msisdn_threshold
        self._out_of_region_threshold = out_of_region_threshold
        self._non_home_network_threshold = non_home_network_threshold
        # Historic thresholds
        self._historic_imei_threshold = historic_imei_threshold
        self._historic_imsi_threshold = historic_imsi_threshold
        self._historic_msisdn_threshold = historic_msisdn_threshold
        self._leading_zero_suspect_limit = leading_zero_suspect_limit
        # Switches to disable importing of certain columns
        self._perform_msisdn_import = perform_msisdn_import
        self._perform_rat_import = perform_rat_import
        # Switches to disable groups of checks
        self._perform_file_daterange_check = perform_file_daterange_check
        self._perform_leading_zero_check = perform_leading_zero_check
        self._perform_null_checks = perform_null_checks
        self._perform_unclean_checks = perform_unclean_checks
        self._perform_region_checks = perform_region_checks
        self._perform_home_network_check = perform_home_network_check
        self._perform_historic_checks = perform_historic_checks
        # We don't want to get any global row count for this imported as it could be horrendously slow
        self._need_previous_count_for_stats = False
        # By default the system will automatically run Analyze on monthly_network_triplets_country,
        # network_imeis and monthly_network_triplets_per_mno of the data
        self._perform_auto_analyze = perform_auto_analyze
        # These will be set to non-None during import
        self._min_connection_date = None
        self._max_connection_date = None
        self._distinct_triplet_count = 0
        self._mcc_mnc_pairs = mcc_mnc_pairs
        self._mcc_mnc_str = ['{0}{1}%'.format(x['mcc'], x['mnc']) for x in mcc_mnc_pairs]
        self._mcc = ['{0}%'.format(x['mcc']) for x in mcc_mnc_pairs]
        self._cc = ['{0}%'.format(x) for x in cc]

    @property
    def _import_type(self):
        """Overrides AbstractImporter._import_type."""
        return 'operator'

    @property
    def _schema_file(self):
        """Overrides AbstractImporter._schema_file."""
        if self._perform_rat_import:
            return 'OperatorImportSchema_v2.csvs'
        else:
            return 'OperatorImportSchema.csvs'

    @property
    def _import_relation_name(self):
        """Overrides AbstractImporter._import_relation_name."""
        return 'operator_data'

    @property
    def _import_metadata(self):
        """Overrides AbstractImporter._import_metadata."""
        md = super()._import_metadata
        md.update({
            'operator_id': self._operator_id,
            'null_imei_threshold': self._null_imei_threshold,
            'null_imsi_threshold': self._null_imsi_threshold,
            'null_msisdn_threshold': self._null_msisdn_threshold,
            'null_rat_threshold': self._null_rat_threshold,
            'null_threshold': self._null_threshold,
            'unclean_imei_threshold': self._unclean_imei_threshold,
            'unclean_imsi_threshold': self._unclean_imsi_threshold,
            'unclean_threshold': self._unclean_threshold,
            'out_of_region_imsi_threshold': self._out_of_region_imsi_threshold,
            'out_of_region_msisdn_threshold': self._out_of_region_msisdn_threshold,
            'out_of_region_threshold': self._out_of_region_threshold,
            'non_home_network_threshold': self._non_home_network_threshold,
            'historic_imei_threshold': self._historic_imei_threshold,
            'historic_imsi_threshold': self._historic_imsi_threshold,
            'historic_msisdn_threshold': self._historic_msisdn_threshold,
            'leading_zero_suspect_limit': self._leading_zero_suspect_limit,
            'perform_file_daterange_check': self._perform_file_daterange_check,
            'perform_leading_zero_check': self._perform_leading_zero_check,
            'perform_null_checks': self._perform_null_checks,
            'perform_clean_checks': self._perform_unclean_checks,
            'perform_region_checks': self._perform_region_checks,
            'perform_home_network_check': self._perform_home_network_check,
            'perform_historic_checks': self._perform_historic_checks,
            'perform_rat_import': self._perform_rat_import,
            'perform_msisdn_import': self._perform_msisdn_import,
            'mcc_mnc_pairs': self._mcc_mnc_pairs,
            'cc': self._cc
        })
        return md

    @property
    def _import_lock_key(self):
        """Overrides AbstractImporter._import_lock_key to allow concurrent imports for different operators."""
        return hash_string_64bit('{0}_{1}'.format(self._import_type, self._operator_id))

    @property
    def _owner_role_name(self):
        """Overrides AbstractImporter._owner_role."""
        return 'dirbs_core_import_operator'

    @property
    def _supports_imei_shards(self):
        """Overrides AbstractImporter._supports_imei_shards."""
        return True

    @property
    def _staging_hll_sketches_tbl_name(self):
        """Name for the staging hll sketches table to use for this import."""
        return 'staging_hll_sketches_import_{0}'.format(self.import_id)

    @property
    def _staging_hll_sketches_tbl_id(self):
        """Id for the staging hll sketches table to use for this import."""
        return sql.Identifier(self._staging_hll_sketches_tbl_name)

    def _perform_filename_checks(self, input_filename):
        """Overrides AbstractImporter._perform_filename_checks."""
        super()._perform_filename_checks(input_filename)
        try:
            importer_utils.perform_operator_filename_checks(input_filename)
        except exceptions.FilenameCheckRawException as err:
            raise exceptions.FilenameCheckException(
                str(err),
                statsd=self._statsd,
                metrics_failures_root=self._metrics_failures_root
            )

    def _init_staging_table(self):
        """Overrides AbstractImporter._init_staging_table."""
        with self._conn.cursor() as cursor, db_role_setter(self._conn, role_name=self._owner_role_name):
            self._tables_to_cleanup_list.append(self._staging_tbl_name)
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (
                                          row_id            BIGSERIAL NOT NULL,
                                          msisdn            TEXT,
                                          msisdn_norm       TEXT,
                                          imei              TEXT,
                                          imei_norm         TEXT,
                                          imsi              TEXT,
                                          imsi_norm         TEXT,
                                          rat               TEXT,
                                          rat_norm          TEXT,
                                          connection_date   DATE NOT NULL
                                      ) PARTITION BY RANGE (calc_virt_imei_shard(imei))""")
                           .format(self._staging_tbl_identifier))

            self._tables_to_cleanup_list.append(self._staging_hll_sketches_tbl_name)
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (LIKE daily_per_mno_hll_sketches)""")
                           .format(self._staging_hll_sketches_tbl_id))

    def _on_staging_table_shard_creation(self, shard_name, virt_imei_range_start, virt_imei_range_end):
        """Overrides AbstractImporter._on_staging_table_shard_creation."""
        with self._conn.cursor() as cursor:
            trigger_name = 'operator_data_insert_staging_trigger_{0:d}_{1:d}_{2:d}'.format(self.import_id,
                                                                                           virt_imei_range_start,
                                                                                           virt_imei_range_end - 1)
            cursor.execute(sql.SQL("""CREATE TRIGGER {0} BEFORE INSERT ON {1}
                                      FOR EACH ROW EXECUTE PROCEDURE operator_staging_data_insert_trigger_fn()""")
                           .format(sql.Identifier(trigger_name), sql.Identifier(shard_name)))

    def _upload_batch_to_staging_table_query(self):
        """Overrides AbstractImporter._upload_batch_to_staging_table_query."""
        import_column_names = ['connection_date', 'imei', 'imsi', 'msisdn']
        if self._perform_rat_import:
            import_column_names.append('rat')
        return sql.SQL("""COPY {0} ({1}) FROM STDIN WITH CSV HEADER""") \
            .format(self._staging_tbl_identifier,
                    sql.SQL(', ').join(map(sql.Identifier, import_column_names)))

    @property
    def _binary_validation_checks(self):
        """Overrides AbstractImporter._binary_validation_checks."""
        if self._perform_file_daterange_check:
            yield self._check_data_date_range()
        else:
            self._logger.warning('Skipped file date range check due to command-line option')

        if self._perform_leading_zero_check:
            yield self._check_for_leading_zeroes()
        else:
            self._logger.warning('Skipped leading zero check due to command-line option')

    def _check_data_date_range(self):
        """Checks whether the data in the staging table matches what is specified in the filename."""
        expected_start_date, expected_end_date = importer_utils.operator_expected_file_dates(self._filename)
        metric_key = 'data_daterange'
        curr_date = datetime.date.today()
        if expected_end_date > curr_date:
            return False, \
                'End date on operator data dump file is in the future (later than current system date)', \
                metric_key

        with self._conn.cursor() as cursor:
            cursor.execute(sql.SQL('SELECT COUNT(*) FROM {0} WHERE connection_date < %s OR connection_date > %s')
                           .format(self._staging_tbl_identifier),
                           [expected_start_date, expected_end_date])
            invalid_count = cursor.fetchone()[0]
            if invalid_count > 0:
                return False, '{0:d} records are outside the date range supplied by the filename {1}' \
                    .format(invalid_count, self._filename), metric_key

            return True, 'Data date range check passed', metric_key

    def _check_for_leading_zeroes(self):
        """Method to validate that the input file did not have leading zeroes stripped from it."""
        with self._conn.cursor() as cursor:
            metric_key = 'leading_zero'
            leading_digit_query = sql.SQL('SELECT COUNT(*) FROM {0} WHERE SUBSTRING(imei, 1, 1) = %s') \
                .format(self._staging_tbl_identifier)

            # Work out if there are more TACs that start with 1 than 0. If so, zeroes have likely been stripped
            cursor.execute(leading_digit_query, ['0'])
            leading_zero_count = cursor.fetchone()[0]
            cursor.execute(leading_digit_query, ['1'])
            leading_one_count = cursor.fetchone()[0]
            if leading_one_count > leading_zero_count:
                return False, 'Failed leading zero check - suspect leading 0s have ' \
                              'been removed from IMEIs. Import aborted - no rows have been imported.', \
                              metric_key

            # Get all IMEIs starting with 1 taking first 7 digits and compare to digits 2-8 of GSMA TAC DB TAC
            cursor.execute(sql.SQL("""SELECT COUNT(*)
                                        FROM {0}
                                  INNER JOIN gsma_data ON SUBSTRING(imei, 1, 7) = SUBSTRING(tac, 2, 8)
                                       WHERE SUBSTRING(imei, 1, 1) = '1'
                                         AND SUBSTRING(tac, 1, 1) = '0'""")
                           .format(self._staging_tbl_identifier))
            suspect_missing_zero_imeis = cursor.fetchone()[0]

            leading_zero_suspect_limit = self._leading_zero_suspect_limit
            if leading_one_count > 1 and suspect_missing_zero_imeis / leading_one_count > leading_zero_suspect_limit:
                return False, 'Too many IMEIs that start with 1 match the TAC DB when prepended with 0', metric_key

            return True, 'Leading zero check passed', metric_key

    @property
    def _threshold_validation_checks(self):
        """Overrides AbstractImporter._threshold_validation_checks."""
        if self._data_length == 0:
            self._logger.warning('Skipped threshold checks due to zero rows in input data')
            return []

        if self._perform_null_checks:
            yield self._check_null_imei_ratio()
            yield self._check_null_imsi_ratio()
            if self._perform_msisdn_import:
                yield self._check_null_msisdn_ratio()
            else:
                self._logger.warning('Skipped NULL MSISDN data threshold check due to disabled MSISDN import')
            if self._perform_rat_import:
                yield self._check_null_rat_ratio()
            else:
                self._logger.warning('Skipped NULL RAT data threshold check due to disabled RAT import')
            yield self._check_null_ratio()
        else:
            self._logger.warning('Skipped NULL data threshold checks due to command-line option')

        if self._perform_unclean_checks:
            yield self._check_unclean_imei_ratio()
            yield self._check_unclean_imsi_ratio()
            yield self._check_unclean_ratio()
        else:
            self._logger.warning('Skipped unclean data threshold checks due to command-line option')

        if self._perform_region_checks:
            yield self._check_out_of_region_imsi_ratio()
            if self._perform_msisdn_import:
                yield self._check_out_of_region_msisdn_ratio()
            else:
                self._logger.warning('Skipped out-of-region MSISDN data threshold check due to disabled MSISDN import')
            if self._perform_msisdn_import:
                yield self._check_out_of_region_ratio()
            else:
                self._logger.warning('Skipped out-of-region (combined) data threshold check due to disabled '
                                     'MSISDN import')
        else:
            self._logger.warning('Skipped out-of-region data threshold checks due to command-line option')

        if self._perform_home_network_check:
            yield self._check_non_home_network_imsi_ratio()
        else:
            self._logger.warning('Skipped home network data threshold check due to command-line option')

    def _check_null_ratio_helper(self, *, column, check_name, threshold, metric_key):
        assert self._data_length > 0 and 'This should not be called on an empty file!'
        with self._conn.cursor() as cursor:
            cursor.execute(sql.SQL('SELECT COUNT(*) FROM {0} WHERE {1} IS NULL')
                           .format(self._staging_tbl_identifier,
                                   sql.Identifier(column)))
            failing_null_check = cursor.fetchone()[0]
            ratio = failing_null_check / self._data_length
            return check_name, ratio <= threshold, ratio, threshold, metric_key

    def _check_null_imei_ratio(self):
        """Check null imei.

        Check whether the percentage of records with a NULL IMEI value is less than
        our configured threshold.
        """
        return self._check_null_ratio_helper(column='imei_norm', check_name='NULL IMEI data',
                                             threshold=self._null_imei_threshold, metric_key='null_imei_norm')

    def _check_null_imsi_ratio(self):
        """Check null imsi.

        Check whether the percentage of records with a NULL IMSI value is less than
        our configured threshold.
        """
        return self._check_null_ratio_helper(column='imsi_norm', check_name='NULL IMSI data',
                                             threshold=self._null_imsi_threshold, metric_key='null_imsi')

    def _check_null_msisdn_ratio(self):
        """Check null msisdn.

        Check whether the percentage of records with a NULL MSISDN value is less than
        our configured threshold.
        """
        return self._check_null_ratio_helper(column='msisdn_norm', check_name='NULL MSISDN data',
                                             threshold=self._null_msisdn_threshold, metric_key='null_msisdn')

    def _check_null_rat_ratio(self):
        """Check null rat.

        Check whether the percentage of records with a NULL rat value is less than
        our configured threshold.
        """
        return self._check_null_ratio_helper(column='rat_norm', check_name='NULL RAT data',
                                             threshold=self._null_rat_threshold, metric_key='null_rat')

    @property
    def _null_clean_check_valid_columns(self):
        """Returns column names that should be validated against for the clean check."""
        valid_columns = ['imei_norm', 'imsi_norm']
        if self._perform_msisdn_import:
            valid_columns.append('msisdn_norm')
        if self._perform_rat_import:
            valid_columns.append('rat_norm')
        return valid_columns

    def _check_null_ratio(self):
        """Combined NULL check.

        Check whether the percentage of records with any NULL column value is less than
        our configured threshold.
        """
        with self._conn.cursor() as cursor:
            cols_to_check = self._null_clean_check_valid_columns
            null_filters = sql.SQL(' OR ').join([sql.SQL('{0} IS NULL').format(sql.Identifier(c))
                                                 for c in cols_to_check])

            cursor.execute(sql.SQL('SELECT COUNT(*) FROM {0} WHERE {1}')
                           .format(self._staging_tbl_identifier, null_filters))
            failing_null_check = cursor.fetchone()[0]
            ratio = failing_null_check / self._data_length
            return 'NULL data (combined)', ratio <= self._null_threshold, ratio, self._null_threshold, 'null'

    def _check_unclean_imei_ratio(self):
        """Check whether the percentage of IMEIs with invalid values is less than our configured threshold."""
        with self._conn.cursor() as cursor:
            cursor.execute(sql.SQL("""SELECT COUNT(*)
                                        FROM {0}
                                       WHERE is_unclean_imei(imei_norm, imei)""")
                           .format(self._staging_tbl_identifier))
            failing_check = cursor.fetchone()[0]
            ratio = failing_check / self._data_length
            threshold = self._unclean_imei_threshold
            return 'unclean IMEI data', ratio <= threshold, ratio, threshold, 'unclean_imei'

    def _check_unclean_imsi_ratio(self):
        """Check whether the percentage of IMSIs with invalid values is less than our configured threshold."""
        with self._conn.cursor() as cursor:
            cursor.execute(sql.SQL("""SELECT COUNT(*)
                                        FROM {0}
                                       WHERE is_unclean_imsi(imsi_norm)""")
                           .format(self._staging_tbl_identifier))
            failing_check = cursor.fetchone()[0]
            ratio = failing_check / self._data_length
            threshold = self._unclean_imsi_threshold
            return 'unclean IMSI data', ratio <= threshold, ratio, threshold, 'unclean_imsi'

    def _check_unclean_ratio(self):
        """Check whether the percentage of rows with either an invalid IMEI or IMSI is less than our threshold."""
        with self._conn.cursor() as cursor:
            cursor.execute(sql.SQL("""SELECT COUNT(*)
                                        FROM {0}
                                       WHERE is_unclean_imei(imei_norm, imei)
                                          OR is_unclean_imsi(imsi_norm)""")
                           .format(self._staging_tbl_identifier))
            failing_check = cursor.fetchone()[0]
            ratio = failing_check / self._data_length
            threshold = self._unclean_threshold
            return 'unclean data (combined)', ratio <= threshold, ratio, threshold, 'unclean'

    def _check_out_of_region_imsi_ratio(self):
        """Check whether the percentage of rows with an out-of-region IMSI is less than our threshold."""
        with self._conn.cursor() as cursor:
            cursor.execute(sql.SQL("""SELECT COUNT(*)
                                        FROM {0}
                                       WHERE fails_prefix_check(imsi_norm, %s)""")
                           .format(self._staging_tbl_identifier),
                           [self._mcc])
            failing_check = cursor.fetchone()[0]
            ratio = failing_check / self._data_length
            threshold = self._out_of_region_imsi_threshold
            return 'out-of-region IMSI data', ratio <= threshold, ratio, threshold, 'out_of_region_imsi'

    def _check_out_of_region_msisdn_ratio(self):
        """Check whether the percentage of rows with an out-of-region MSISDN is less than our threshold."""
        with self._conn.cursor() as cursor:
            cursor.execute(sql.SQL("""SELECT COUNT(*)
                                        FROM {0}
                                       WHERE fails_prefix_check(msisdn_norm, %s)""")
                           .format(self._staging_tbl_identifier),
                           [self._cc])
            failing_check = cursor.fetchone()[0]
            ratio = failing_check / self._data_length
            threshold = self._out_of_region_msisdn_threshold
            return 'out-of-region MSISDN data', ratio <= threshold, ratio, threshold, 'out_of_region_msisdn'

    def _check_out_of_region_ratio(self):
        """Check whether the % of rows with either an out-of-region IMSI or MSISDN is less than our threshold."""
        with self._conn.cursor() as cursor:
            cursor.execute(sql.SQL("""SELECT COUNT(*)
                                        FROM {0}
                                       WHERE fails_prefix_check(imsi_norm, %s)
                                          OR fails_prefix_check(msisdn_norm, %s)""")
                           .format(self._staging_tbl_identifier),
                           [self._mcc, self._cc])
            failing_check = cursor.fetchone()[0]
            ratio = failing_check / self._data_length
            threshold = self._out_of_region_threshold
            return 'out-of-region data (combined)', ratio <= threshold, ratio, threshold, 'out_of_region'

    def _check_non_home_network_imsi_ratio(self):
        """Check whether the percentage of IMSIs with an invalid home check is less than our configured threshold."""
        with self._conn.cursor() as cursor:
            cursor.execute(sql.SQL("""SELECT COUNT(*)
                                        FROM {0}
                                       WHERE fails_prefix_check(imsi_norm, %s)""")
                           .format(self._staging_tbl_identifier),
                           [self._mcc_mnc_str])
            failing_check = cursor.fetchone()[0]
            ratio = failing_check / self._data_length
            threshold = self._non_home_network_threshold
            return 'non-home network IMSI data', ratio <= threshold, ratio, threshold, 'non_home_imsi'

    @property
    def _historical_validation_checks(self):
        """Overrides AbstractImporter._historical_validation_checks."""
        if self._perform_historic_checks:
            with self._conn.cursor() as cursor:
                # The idea with the MAX(data_id) subquery is that we can have multiple reporting runs for the same
                # date and we want to make sure we have the most recent reporting run for that date
                cursor.execute("""SELECT AVG(hll_cardinality(imei_hll))::REAL AS imei_avg,
                                         AVG(hll_cardinality(imsi_hll))::REAL AS imsi_avg,
                                         AVG(hll_cardinality(msisdn_hll))::REAL AS msisdn_avg
                                    FROM (SELECT *
                                            FROM daily_per_mno_hll_sketches
                                           WHERE operator_id = %s
                                             AND hll_cardinality(triplet_hll) > 0
                                        ORDER BY data_date DESC
                                           LIMIT 30) stats""",
                               [self._operator_id])
                historical_imei, historical_imsi, historical_msisdn = cursor.fetchone()

                cursor.execute(sql.SQL(
                    """SELECT AVG(hll_cardinality(imei_hll))::REAL AS imei_avg,
                              AVG(hll_cardinality(imsi_hll))::REAL AS imsi_avg,
                              AVG(hll_cardinality(msisdn_hll))::REAL AS msisdn_avg
                         FROM (SELECT COALESCE(hll_add_agg(hll_hash_text(imei_norm)), hll_empty()) AS imei_hll,
                                      COALESCE(hll_add_agg(hll_hash_text(imsi_norm)), hll_empty()) AS imsi_hll,
                                      COALESCE(hll_add_agg(hll_hash_text(msisdn_norm)), hll_empty()) AS msisdn_hll
                                 FROM {0}
                             GROUP BY connection_date) daily_counts
                    """).format(self._staging_tbl_identifier))  # noqa: Q441, Q447
                data_set_imei, data_set_imsi, data_set_msisdn = cursor.fetchone()

                if historical_imei is not None:
                    yield self._check_unique_imei_per_day(historical_imei, data_set_imei)
                else:
                    self._logger.warning('Skipped historic IMEI per day check due to lack of historic data')
                if historical_imsi is not None:
                    yield self._check_unique_imsi_per_day(historical_imsi, data_set_imsi)
                else:
                    self._logger.warning('Skipped historic IMSI per day check due to lack of historic data')
                if historical_msisdn is not None:
                    yield self._check_unique_msisdn_per_day(historical_msisdn, data_set_msisdn)
                else:
                    self._logger.warning('Skipped historic MSISDN per day check due to lack of historic data')
        else:
            self._logger.warning('Skipped historic IMEI/IMSI/MSISDN checks due to command-line option')

    def _check_unique_imei_per_day(self, historic_average, data_set_average):
        """Compares the dump's average IMEI per day count again the historic average."""
        min_avg_imei = self._historic_imei_threshold * historic_average
        check_result = data_set_average >= min_avg_imei
        metric_key = 'historic_imei'
        return 'IMEI per day', check_result, data_set_average, historic_average, min_avg_imei, metric_key

    def _check_unique_imsi_per_day(self, historic_average, data_set_average):
        """Compares the dump's average IMSI per day count again the historic average."""
        min_avg_imsi = self._historic_imsi_threshold * historic_average
        check_result = data_set_average >= min_avg_imsi
        metric_key = 'historic_imsi'
        return 'IMSI per day', check_result, data_set_average, historic_average, min_avg_imsi, metric_key

    def _check_unique_msisdn_per_day(self, historic_average, data_set_average):
        """Compares the dump's average MSISD per day count again the historic average."""
        min_avg_msisdn = self._historic_msisdn_threshold * historic_average
        check_result = data_set_average >= min_avg_msisdn
        metric_key = 'historic_msisdn'
        return 'MSISDN per day', check_result, data_set_average, historic_average, min_avg_msisdn, metric_key

    def _copy_staging_data(self):
        """Overrides AbstractImporter._copy_staging_data."""
        #
        # We calculate the distinct connection dates here so that we can work out which monthly partitions we need
        # to insert into. Each partition can be done in paralllel since they are indepedendent and inserting into
        # different tables.
        #
        with self._conn, self._conn.cursor() as cursor:
            self._logger.info('Calculating min/max connection dates seen in staging table data...')
            cursor.execute(sql.SQL('SELECT MIN(connection_date), MAX(connection_date) FROM {0}')
                           .format(self._staging_tbl_identifier))
            self._min_connection_date, self._max_connection_date = cursor.fetchone()
        self._logger.info('Calculated min/max connection dates seen in staging table data')

        #
        # Go ahead and create any new monthly_network_triplets partitions
        #
        self._logger.info('Creating required new monthly_network_triplets partitions...')
        with self._conn as conn, self._conn.cursor() as cursor:
            for month, year in self._month_year_tuples_for_import():
                self._create_monthly_network_triplets_partitions(conn, month, year)
        self._logger.info('Created required new monthly_network_triplets partitions')

        #
        # Get before count by summing monthly_network_triplets_per_mno COUNTs for each month/year partition
        # for this operator
        #
        rows_before = 0
        with self._conn as conn, self._conn.cursor() as cursor:
            for month, year in self._month_year_tuples_for_import():
                imei_shard_name = partition_utils.monthly_network_triplets_per_mno_partition(
                    operator_id=self._operator_id,
                    month=month,
                    year=year
                )
                cursor.execute(sql.SQL('SELECT COUNT(*) FROM {0}').format(sql.Identifier(imei_shard_name)))
                rows_before += cursor.fetchone()[0]

        #
        # Parallelize updating of monthly_network_triplets and network_imeis tables
        #
        n_partitions = partition_utils.num_physical_imei_shards(self._conn)
        with futures.ThreadPoolExecutor(max_workers=self._max_db_connections) as executor:
            self._logger.info(
                'Simultaneously updating monthly_network_triplets and network_imeis using up to {0:d} workers...'
                .format(self._max_db_connections)
            )
            futures_to_cb = {}

            # Queue monthly_network_triplets jobs
            src_tbl_name = self._staging_tbl_name
            month_year_tuples = self._month_year_tuples_for_import()
            monthly_network_triplets_state = defaultdict(int)
            monthly_network_triplets_state['num_jobs'] = n_partitions * len(month_year_tuples)
            for month, year in self._month_year_tuples_for_import():
                for name, rstart, rend in partition_utils.physical_imei_shards(self._conn, tbl_name=src_tbl_name):
                    f = executor.submit(self._update_monthly_network_triplets, month, year, name, rstart, rend)
                    futures_to_cb[f] = partial(self._process_monthly_network_triplets_result,
                                               monthly_network_triplets_state,
                                               month,
                                               year)

            # Queue network_imeis jobs
            network_imeis_state = defaultdict(int)
            network_imeis_state['num_jobs'] = n_partitions
            for name, rstart, rend in partition_utils.physical_imei_shards(self._conn, tbl_name=src_tbl_name):
                f = executor.submit(self._update_network_imeis, name, rstart, rend)
                futures_to_cb[f] = partial(self._process_network_imeis_result, network_imeis_state)

            # Wait for all monthly_network_triplets and network_imeis jobs to complete
            for f in futures.as_completed(futures_to_cb):
                futures_to_cb[f](f)

            # Update the daily_per_mno_hll_sketches on main thread so that we don't update the same rows
            # from multiple transactions (causes deadlock)
            with self._conn, self._conn.cursor() as cursor:
                cursor.execute(sql.SQL("""
                    INSERT INTO daily_per_mno_hll_sketches AS target(data_date, operator_id, creation_date,
                                                                     triplet_hll, imei_hll, imsi_hll, msisdn_hll,
                                                                     imei_imsis_hll, imei_msisdns_hll,
                                                                     imsi_msisdns_hll)
                         SELECT data_date,
                                operator_id,
                                FIRST(creation_date),
                                hll_union_agg(triplet_hll),
                                hll_union_agg(imei_hll),
                                hll_union_agg(imsi_hll),
                                hll_union_agg(msisdn_hll),
                                hll_union_agg(imei_imsis_hll),
                                hll_union_agg(imei_msisdns_hll),
                                hll_union_agg(imsi_msisdns_hll)
                           FROM {0}
                       GROUP BY data_date, operator_id
                    ON CONFLICT (data_date, operator_id)
                      DO UPDATE
                            SET triplet_hll = target.triplet_hll || excluded.triplet_hll ,
                                imei_hll = target.imei_hll || excluded.imei_hll,
                                imsi_hll = target.imsi_hll || excluded.imsi_hll,
                                msisdn_hll = target.msisdn_hll || excluded.msisdn_hll,
                                imei_imsis_hll = target.imei_imsis_hll || excluded.imei_imsis_hll,
                                imei_msisdns_hll = target.imei_msisdns_hll || excluded.imei_msisdns_hll,
                                imsi_msisdns_hll = target.imsi_msisdns_hll || excluded.imsi_msisdns_hll,
                                creation_date = excluded.creation_date;""")  # noqa: Q441, Q449
                               .format(self._staging_hll_sketches_tbl_id))

            #
            # Calculate number of rows that were inserted/updated
            #
            rows_after = 0
            with self._conn as conn, self._conn.cursor() as cursor:
                for month, year in self._month_year_tuples_for_import():
                    imei_shard_name = partition_utils.monthly_network_triplets_per_mno_partition(
                        operator_id=self._operator_id,
                        month=month,
                        year=year
                    )
                    cursor.execute(sql.SQL('SELECT COUNT(*) FROM {0}').format(sql.Identifier(imei_shard_name)))
                    rows_after += cursor.fetchone()[0]

            inserted_triplet_count = rows_after - rows_before
            updated_triplet_count = monthly_network_triplets_state['num_inserted_or_updated'] - inserted_triplet_count

            #
            # ANALYZE the parent tables -- for partitioned tables, this will also ANALYZE the children.
            # by default the system will auto analyze the tables, if disabled the DBA should take care of the activity
            #
            if self._perform_auto_analyze:
                executor.submit(self._analyze_job, 'monthly_network_triplets_country')
                executor.submit(self._analyze_job, 'monthly_network_triplets_per_mno_{0}'.format(self._operator_id))
                executor.submit(self._analyze_job, 'network_imeis')
            else:
                self._logger.warning('Skipping auto analyze of associated historic tables...')
                self._logger.debug('Skipping auto analyze of monthly_network_triplets_country...\n'
                                   'Skipping auto analyze of monthly_network_triplets_country_per_mno_{0}...\n'
                                   'Skipping auto analyze of network_imeis...'.format(self._operator_id))

        return inserted_triplet_count, updated_triplet_count, 0

    def _analyze_job(self, tbl_name):
        """Helper function to ANALYZE a table in a separate process."""
        with create_db_connection(self._db_config) as conn, conn.cursor() as cursor:
            self._logger.debug('Running ANALYZE on {0}...'.format(tbl_name))
            cursor.execute(sql.SQL('ANALYZE {0}').format(sql.Identifier(tbl_name)))
            self._logger.debug('Finished running ANALYZE on {0}'.format(tbl_name))

    def _process_network_imeis_result(self, state, future):
        """Process a network_imeis future, mutating the passed state."""
        future.result()  # will throw exception if this one was thrown in thread
        state['num_processed'] += 1
        self._logger.info('Updated network_imeis table with unseen imeis [{0:d} of {1:d} partitions]'
                          .format(state['num_processed'], state['num_jobs']))

    def _process_monthly_network_triplets_result(self, state, month, year, future):
        """Process a monthly_network_triplet future, mutating the passed state."""
        inserted_or_updated_triplet_count = future.result()  # will throw exception if this one was thrown in thread
        state['num_processed'] += 1
        state['num_inserted_or_updated'] += inserted_or_updated_triplet_count
        self._logger.info('Updated monthly_network_triplet tables for {0:02d}/{1:d} [{2:d} of {3:d} partitions]'
                          .format(month, year, state['num_processed'], state['num_jobs']))

    def _update_monthly_network_triplets(self, month, year, src_partition, virt_imei_shard_start,
                                         virt_imei_shard_end):
        """Helper function to update the monthly_network_triplets tables (country and per-MNO)."""
        with create_db_connection(self._db_config) as conn, conn.cursor() as cursor:
            start_date, end_date = self._date_range_for_month_year(month, year)

            aggregated_data_temp_table = '{0}_aggregated_{1:02d}_{2:d}'.format(src_partition, month, year)
            cursor.execute(sql.SQL("""CREATE TEMPORARY TABLE {0} (LIKE monthly_network_triplets_per_mno)""")
                           .format(sql.Identifier(aggregated_data_temp_table)))

            if self._perform_msisdn_import:
                msisdn_output = sql.Identifier('msisdn_norm')
            else:
                msisdn_output = sql.SQL('NULL AS msisdn_norm')

            cursor.execute(sql.SQL("""
                INSERT INTO {0} (triplet_year, triplet_month, first_seen, last_seen, date_bitmask,
                                 triplet_hash, imei_norm, imsi, msisdn, operator_id, virt_imei_shard)
                     SELECT %s,
                            %s,
                            triplet_sq.first_seen,
                            triplet_sq.last_seen,
                            triplet_sq.date_bitmask,
                            triplet_sq.triplet_hash,
                            triplet_sq.imei_norm,
                            triplet_sq.imsi_norm,
                            triplet_sq.msisdn_norm,
                            %s,
                            calc_virt_imei_shard(triplet_sq.imei_norm)
                       FROM (SELECT hash_triplet(imei_norm, imsi_norm, msisdn_norm) AS triplet_hash,
                                    imei_norm,
                                    imsi_norm,
                                    {msisdn_output},
                                    MIN(connection_date) AS first_seen,
                                    MAX(connection_date) AS last_seen,
                                    bit_or(1 << (date_part('day', connection_date)::INT - 1)) AS date_bitmask
                               FROM (SELECT *
                                       FROM {1}
                                      WHERE connection_date >= %s
                                        AND connection_date < %s) op_data_sq
                           GROUP BY imei_norm, imsi_norm, msisdn_norm) triplet_sq""")
                           .format(sql.Identifier(aggregated_data_temp_table),
                                   sql.Identifier(src_partition),
                                   msisdn_output=msisdn_output),
                           [year, month, self._operator_id, start_date, end_date])

            # Save how many triplets actually got inserted for later on when we print stats
            self._distinct_triplet_count += cursor.rowcount

            # Need to insert into both country and per-MNO tables. Do country first
            base_partition = partition_utils.monthly_network_triplets_country_partition(month=month, year=year)
            dest_partition = partition_utils.imei_shard_name(base_name=base_partition,
                                                             virt_imei_range_start=virt_imei_shard_start,
                                                             virt_imei_range_end=virt_imei_shard_end)

            # The on conflict clause is common to both insertions
            on_conflict_sql = sql.SQL(
                """ON CONFLICT (triplet_hash)
                     DO UPDATE
                           SET first_seen = least(target.first_seen, excluded.first_seen),
                               last_seen = greatest(target.last_seen, excluded.last_seen),
                               date_bitmask = target.date_bitmask | excluded.date_bitmask
                         WHERE (target.date_bitmask | excluded.date_bitmask) != target.date_bitmask
                """  # noqa: Q441
            )

            cursor.execute(
                sql.SQL(
                    """INSERT INTO {0} AS target(triplet_year, triplet_month, first_seen, last_seen, date_bitmask,
                                                 triplet_hash, imei_norm, imsi, msisdn, virt_imei_shard)
                            SELECT triplet_year, triplet_month, first_seen, last_seen, date_bitmask, triplet_hash,
                                   imei_norm, imsi, msisdn, virt_imei_shard
                              FROM {1}
                                   {2}
                    """
                ).format(sql.Identifier(dest_partition), sql.Identifier(aggregated_data_temp_table), on_conflict_sql))

            # Now insert into the per-MNO table
            base_partition = partition_utils.monthly_network_triplets_per_mno_partition(month=month, year=year,
                                                                                        operator_id=self._operator_id)
            dest_partition = partition_utils.imei_shard_name(base_name=base_partition,
                                                             virt_imei_range_start=virt_imei_shard_start,
                                                             virt_imei_range_end=virt_imei_shard_end)
            cursor.execute(
                sql.SQL(
                    """INSERT INTO {0} AS target
                            SELECT *
                              FROM {1}
                                   {2}
                    """
                ).format(sql.Identifier(dest_partition), sql.Identifier(aggregated_data_temp_table), on_conflict_sql))

            inserted_or_updated_triplet_count = cursor.rowcount

            # Update daily_per_mno_hll_sketches table
            hll_partition_base_name = '{0}_{1:02d}_{2:d}'.format(self._staging_hll_sketches_tbl_name, month, year)
            hll_partition_name = partition_utils.imei_shard_name(base_name=hll_partition_base_name,
                                                                 virt_imei_range_start=virt_imei_shard_start,
                                                                 virt_imei_range_end=virt_imei_shard_end)
            cursor.execute(sql.SQL("""CREATE UNLOGGED TABLE {0} (LIKE {1}) INHERITS ({1})""")
                           .format(sql.Identifier(hll_partition_name),
                                   self._staging_hll_sketches_tbl_id))

            cursor.execute(sql.SQL("""
                INSERT INTO {0} (triplet_hll, imei_hll, imsi_hll, msisdn_hll, imei_imsis_hll, imei_msisdns_hll,
                                 imsi_msisdns_hll, creation_date, operator_id, data_date)
                     SELECT coalesce(hll_add_agg(hll_hash_text(hash_triplet(imei_norm, imsi, msisdn)::TEXT))
                                     filter(WHERE imei_norm IS NOT NULL
                                              AND imsi_norm IS NOT NULL
                                              AND msisdn_norm IS NOT NULL), hll_empty()) AS triplet_hll,
                            coalesce(hll_add_agg(hll_hash_text(imei_norm)), hll_empty()) AS imei_hll,
                            coalesce(hll_add_agg(hll_hash_text(imsi_norm)), hll_empty()) as imsi_hll,
                            coalesce(hll_add_agg(hll_hash_text(msisdn_norm)), hll_empty()) AS msisdn_hll,
                            coalesce(hll_add_agg(hll_hash_text(imei_norm||'$'||imsi_norm)), hll_empty())
                                AS imei_imsis_hll,
                            coalesce(hll_add_agg(hll_hash_text(imei_norm||'$'||msisdn_norm)), hll_empty())
                                AS imei_msisdns_hll,
                            coalesce(hll_add_agg(hll_hash_text(imsi_norm||'$'||msisdn_norm)), hll_empty())
                                AS imsi_msisdns_hll,
                            CURRENT_DATE AS creation_date,
                            %s AS operator_id,
                            make_date(%s, %s, date_part('day', connection_date)::INT) AS data_date
                       FROM {1}
                      WHERE connection_date >= %s
                        AND connection_date < %s
                   GROUP BY connection_date;
                """).format(sql.Identifier(hll_partition_name), # noqa Q441, Q447
                            sql.Identifier(src_partition)),
                           [self._operator_id, year, month, start_date, end_date])

        return inserted_or_updated_triplet_count

    def _month_year_tuples_for_import(self):
        """Helper function to return a set of month/year tuples for a given import."""
        return{(dt.month, dt.year)
               for dt in rrule(MONTHLY, dtstart=self._min_connection_date, until=self._max_connection_date)}

    def _date_range_for_month_year(self, month, year):
        """Helper function to return a pair of dates for start and end date given a month and year.

        The minimum allowed date in this partition is the first day of the year and month it represents.
        We then work out the maximum allowed date. It is actually easy to get the first date of the *next*
        month and use a less than constraint.
        """
        start_date = datetime.date(year, month, 1)
        if month == 12:
            # If we're in December, we actually need to increment a full year to get the next month (overflow)
            end_date = datetime.date(year + 1, 1, 1)
        else:
            # Otherwise just add once to the month to get the first of the next month
            end_date = datetime.date(year, month + 1, 1)
        return start_date, end_date

    def _create_monthly_network_triplets_partitions(self, conn, month, year):
        """Helper function to create the actual monthly_network_triplets_partitions partitions."""
        with conn.cursor() as cursor:
            imei_shard_name = partition_utils.monthly_network_triplets_country_partition(month=month, year=year)
            cursor.execute(table_exists_sql(), [imei_shard_name])
            partition_exists = cursor.fetchone()[0]
            if not partition_exists:
                partition_utils.create_monthly_network_triplets_country_partition(conn, month=month, year=year)
                indices = partition_utils.monthly_network_triplets_country_indices()
                partition_utils.add_indices(conn, tbl_name=imei_shard_name, idx_metadata=indices)

            imei_shard_name = partition_utils.monthly_network_triplets_per_mno_partition(operator_id=self._operator_id,
                                                                                         month=month, year=year)
            cursor.execute(table_exists_sql(), [imei_shard_name])
            partition_exists = cursor.fetchone()[0]
            if not partition_exists:
                op_id = self._operator_id
                partition_utils.create_monthly_network_triplets_per_mno_partition(conn, operator_id=op_id,
                                                                                  month=month, year=year)
                indices = partition_utils.monthly_network_triplets_per_mno_indices()
                partition_utils.add_indices(conn, tbl_name=imei_shard_name, idx_metadata=indices)

    def _update_network_imeis(self, src_partition, virt_imei_shard_start, virt_imei_shard_end):
        """Helper function to update the network_imeis table."""
        dest_partition = partition_utils.imei_shard_name(base_name='network_imeis',
                                                         virt_imei_range_start=virt_imei_shard_start,
                                                         virt_imei_range_end=virt_imei_shard_end)
        with create_db_connection(self._db_config) as conn, conn.cursor() as cursor:
            query = """INSERT INTO {0} AS target (first_seen, last_seen, seen_rat_bitmask,
                                                            virt_imei_shard, imei_norm)
                            SELECT MIN(connection_date),
                                   MAX(connection_date),
                                   bit_or(1 << rat_map.operator_rank),
                                   calc_virt_imei_shard(imei_norm),
                                   imei_norm
                              FROM (SELECT imei_norm,
                                           connection_date,
                                           regexp_split_to_table(COALESCE(rat_norm, ''), '\|') AS rat_code
                                      FROM {1}) exploded_rat
                         LEFT JOIN radio_access_technology_map rat_map
                             USING (rat_code)
                             WHERE imei_norm IS NOT NULL
                          GROUP BY imei_norm
                                   ON CONFLICT (imei_norm)
                                   DO UPDATE
                                         SET first_seen = LEAST(excluded.first_seen, target.first_seen),
                                             last_seen = GREATEST(excluded.last_seen, target.last_seen),
                                             seen_rat_bitmask = target.seen_rat_bitmask | excluded.seen_rat_bitmask
                                       WHERE excluded.first_seen < target.first_seen
                                          OR excluded.last_seen > target.last_seen
                                          OR (target.seen_rat_bitmask | excluded.seen_rat_bitmask)
                                                   != target.seen_rat_bitmask"""  # noqa: Q441, Q447, W605

            cursor.execute(sql.SQL(query).format(sql.Identifier(dest_partition), sql.Identifier(src_partition)))

    def _output_stats(self, rows_before, rows_inserted, rows_updated, row_deleted):
        """Overrides AbstractImporter._output_stats."""
        assert rows_before == -1 and 'rows_before should not be -1'
        assert row_deleted == 0 and 'rows_deleted should be 0'
        self._logger.info('Rows supplied in input file: {0}'.format(self.staging_row_count))

        if self._perform_msisdn_import:
            null_msisdn_filter = sql.SQL('OR msisdn_norm IS NULL')
        else:
            null_msisdn_filter = sql.SQL('')

        if self._perform_rat_import:
            null_rat_filter = sql.SQL('OR rat_norm IS NULL')
        else:
            null_rat_filter = sql.SQL('')

        with self._conn.cursor() as cursor:
            cursor.execute(sql.SQL("""SELECT COUNT(*)
                                        FROM {0}
                                       WHERE imei_norm IS NULL
                                          OR imsi_norm IS NULL
                                             {1}
                                             {2}
                                          OR is_unclean_imei(imei_norm, imei)
                                          OR is_unclean_imsi(imsi_norm)""")
                           .format(self._staging_tbl_identifier,
                                   null_msisdn_filter,
                                   null_rat_filter))
            invalid_imported_data_count = cursor.fetchone()[0]

        self._logger.info('\t[including {0} row(s) with NULL or unclean values imported as under tolerated thresholds]'
                          .format(invalid_imported_data_count))
        self._logger.info('Distinct triplets found in valid rows of input file: {0}'
                          .format(self._distinct_triplet_count))
        self._logger.info('Distinct triplets updated or inserted into monthly partitions: {0} ({1} new, {2} updated)'
                          .format(rows_inserted + rows_updated, rows_inserted, rows_updated))

        # Output StatsD metrics
        valid_row_count = self.staging_row_count - invalid_imported_data_count
        self._statsd.gauge('{0}input_records.valid'.format(self._metrics_import_root), valid_row_count)
        self._statsd.gauge('{0}input_records.invalid'.format(self._metrics_import_root), invalid_imported_data_count)
        self._statsd.gauge('{0}input_records.triplets'.format(self._metrics_import_root),
                           self._distinct_triplet_count)
        self._statsd.gauge('{0}imported_triplets.inserted'.format(self._metrics_import_root), rows_inserted)
        self._statsd.gauge('{0}imported_triplets.updated'.format(self._metrics_import_root), rows_updated)

        # Log stats to metadata table
        metadata.add_optional_job_metadata(self._metadata_conn, 'dirbs-import', self.import_id, input_stats={
            'num_records': self.staging_row_count,
            'num_records_invalid': invalid_imported_data_count,
            'num_records_valid': valid_row_count,
            'num_distinct_triplets': self._distinct_triplet_count

        })
        metadata.add_optional_job_metadata(self._metadata_conn, 'dirbs-import', self.import_id, output_stats={
            'num_distinct_triplets': rows_inserted + rows_updated,
            'num_distinct_triplets_inserted': rows_inserted,
            'num_distinct_triplets_updated': rows_inserted,
        })

    def _log_normalized_import_time_metrics(self, elapsed_time):
        """Overrides AbstractImporter._log_normalized_import_time_metrics."""
        if self._data_length > 0:
            # Only track normalized record time metrics if we worked out how many rows were in the source data and
            # it was greater than zero
            norm_factor = 1000000 / self._data_length
            self._statsd.gauge('{0}import_time.normalized_recs'.format(self._metrics_import_root),
                               elapsed_time * norm_factor)

        if self._distinct_triplet_count > 0:
            # Only track normalized trjplet time metrics if we worked out how many triplets were in the source data and
            # it was greater than zero
            norm_factor = 1000000 / self._distinct_triplet_count
            self._statsd.gauge('{0}import_time.normalized_triplets'.format(self._metrics_import_root),
                               elapsed_time * norm_factor)
