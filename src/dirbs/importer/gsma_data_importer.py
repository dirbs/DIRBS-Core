"""
Code for importing GSMA TAC data into DIRBS Core.

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

import json
from copy import copy

from psycopg2 import sql

import dirbs.importer.exceptions as exceptions
from dirbs.importer.base_delta_importer import BaseDeltaImporter


class GSMADataImporter(BaseDeltaImporter):
    """GSMA TAC DB Importer."""

    mandatory_fields = ['tac', 'manufacturer', 'model_name', 'bands', 'allocation_date', 'device_type']
    postgres_quote_char = '\x01'

    def __init__(self,
                 *args,
                 **kwargs):
        """Constructor."""
        super().__init__(*args, expected_suffix='.txt', **kwargs)

    @property
    def _import_type(self):
        """Overrides AbstractImporter._import_type."""
        return 'gsma_tac'

    @property
    def _import_relation_name(self):
        """Overrides AbstractImporter._import_relation_name."""
        return 'gsma_data'

    @property
    def _schema_file(self):
        """Overrides AbstractImporter._schema_file."""
        return 'GSMASchema.csvs'

    @property
    def _input_csv_field_names(self):
        """Overrides BaseImporter._input_csv_field_names."""
        return ['tac', 'manufacturer', 'bands', 'allocation_date', 'model_name', 'device_type',
                'optional_fields', 'rat_bitmask']

    @property
    def _extra_field_names(self):
        """Overrides BaseImporter._extra_field_names."""
        input_csv_field_names_copy = copy(self._input_csv_field_names)
        input_csv_field_names_copy.remove('tac')
        return input_csv_field_names_copy

    @property
    def _pk_field_names(self):
        """Overrides BaseImporter._pk_field_names."""
        return ['tac']

    @property
    def _staging_tbl_ddl(self):
        """Overrides BaseImporter._staging_tbl_ddl."""
        return """CREATE UNLOGGED TABLE {0} (
                                          row_id            BIGSERIAL,
                                          tac               TEXT,
                                          manufacturer      TEXT,
                                          bands             TEXT,
                                          allocation_date   DATE,
                                          model_name        TEXT,
                                          device_type       TEXT,
                                          optional_fields   JSONB,
                                          rat_bitmask       INTEGER
                                      ) WITH (autovacuum_enabled = false)"""

    @property
    def _staging_data_insert_trigger_name(self):
        """Overrides BaseImporter._staging_data_insert_trigger_name."""
        return None

    @property
    def _owner_role_name(self):
        """Overrides BaseImporter._owner_role_name."""
        return 'dirbs_core_import_gsma'

    def _upload_batch_to_staging_table_query(self):
        """Overrides AbstractImporter._upload_batch_to_staging_table_query."""
        return sql.SQL("""COPY {0} ({1},optional_fields)
                          FROM STDIN WITH CSV HEADER DELIMITER AS '|' QUOTE AS {2}""") \
            .format(self._staging_tbl_identifier,
                    sql.SQL(',').join(sql.Identifier(f) for f in self.mandatory_fields),
                    sql.Literal(self.postgres_quote_char))

    def _upload_batch_to_staging_table(self, string_buffer):
        """Overrides AbstractImporter._upload_batch_to_staging_table."""
        string_buffer = string_buffer.replace('""', '"')
        super()._upload_batch_to_staging_table(string_buffer)

    def _postprocess_staging_data(self):
        """Overrides AbstractImporter._postprocess_staging_data.

        Compute the RAT bitmask based on the bands field
        """
        super()._postprocess_staging_data()
        with self._conn, self._conn.cursor() as cursor:
            cursor.execute(sql.SQL("""UPDATE {0} SET rat_bitmask = translate_bands_to_rat_bitmask(bands)""")
                           .format(self._staging_tbl_identifier))

    def _preprocess_file(self, input_filename):
        """Overrides AbstractImporter._preprocess_file.

        We get a pipe separated text file and need to make sure it hasn't been converted to a csv.
        This function checks that the file hasn't got escaped quotes and isn't missing values.
        Then it converts it to csv and embeds the optional fields as JSON.
        """
        headers = []
        output_order = []
        self._logger.info('Preprocessing {0}...'.format(input_filename))
        output_filename = '{0}.preprocessed'.format(input_filename)
        with open(input_filename, mode='r') as ifile, open(output_filename, mode='w') as ofile:
            self._files_to_delete.append(output_filename)
            ofile.write('{0}|optional_fields\n'.format('|'.join(self.mandatory_fields)))
            for i, line in enumerate(ifile):
                fields = line.split('|')
                if i == 0:
                    col_count = len(fields)
                    headers = [x.lower().replace(' ', '_').strip() for x in fields]
                    for field in self.mandatory_fields:
                        if field not in headers:
                            raise exceptions.PreprocessorCheckException(
                                'Missing mandatory field {0} when preprocessing {1}'.format(field, input_filename),
                                statsd=self._statsd,
                                metrics_failures_root=self._metrics_failures_root
                            )
                        output_order.append(headers.index(field))
                    extra_cols = [x for x in range(0, col_count) if x not in output_order]
                else:
                    if col_count != len(fields):
                        raise exceptions.PreprocessorCheckException(
                            'Inconsistent number of fields per row on line {0:d} when '
                            'preprocessing {1}'.format(i, input_filename),
                            statsd=self._statsd,
                            metrics_failures_root=self._metrics_failures_root
                        )

                    # Replace any double-quote character with empty string
                    fields = [field.strip().replace('"', '') for field in fields]

                    # Add any extra fields to a JSON dict
                    json_dict = {}
                    for i in extra_cols:
                        val = fields[i].replace('\n', '')
                        json_dict[headers[i]] = None if len(val) == 0 else val

                    # Replace the \n they only appear at the end of the lines and aren't part of the field
                    # Add \x01 to start and end of Json field, these are treated as quotes by postgres and stop the
                    # copy operation from trying to parse the JSON.
                    # Swap the \" with \"" so that the preprocessed file will pass the validator , this is stripped
                    # out by the batch upload function.
                    output_string = '{0}{1}{2}{3}{4}\n'.format(
                                    '|'.join([fields[i].replace('\n', '')
                                              .replace('"', '""') for i in output_order]),
                                    '|',
                                    self.postgres_quote_char,
                                    json.dumps(json_dict, ensure_ascii=False).replace('\\"', '\\""'),
                                    self.postgres_quote_char)
                    ofile.write(output_string)

        self._logger.info('Preprocessed: {0}'.format(output_filename))
        return output_filename

    def _copy_staging_data(self):
        """Overrides BaseDeltaImporter._copy_staging_data."""
        rows_inserted, rows_updated, rows_deleted = super()._copy_staging_data()
        with self._conn, self._conn.cursor() as cursor:
            cursor.execute(sql.SQL('REFRESH MATERIALIZED VIEW CONCURRENTLY {0}')
                           .format(sql.Identifier(self._import_relation_name)))
        return rows_inserted, rows_updated, rows_deleted
