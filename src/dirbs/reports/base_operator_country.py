"""
DIRBS operator/country report base class.

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
import abc
import datetime
import logging
from collections import defaultdict

from jinja2 import Environment, PackageLoader
from psycopg2 import sql

from dirbs import report_schema_version, __version__
from dirbs.reports.exceptions import MissingStatsException
from dirbs.utils import format_datetime_for_report, JSONEncoder


class BaseOperatorCountryReport:
    """Base class for both operator and country reports."""

    def __init__(self, conn, data_id, config, month, year, template_name, operator_id, has_compliance_data=False):
        """Constructor."""
        self.conn = conn
        self.data_id = data_id
        self.has_compliance_data = has_compliance_data
        self.template_env = Environment(loader=PackageLoader('dirbs', 'templates'),
                                        trim_blocks=True,
                                        lstrip_blocks=True)
        self.month = month
        self.year = year
        self.config = config
        self.template_name = template_name
        self.logger = logging.getLogger('dirbs.report')
        self.start_date = datetime.date(self.year, self.month, 1)
        if self.month == 12:
            self.end_date = datetime.date(self.year + 1, 1, 1)
        else:
            self.end_date = datetime.date(self.year, self.month + 1, 1)
        self.end_date += datetime.timedelta(days=-1)
        self.operator_id = operator_id

    @abc.abstractmethod
    def gen_report_data(self):
        """Interface for generating the report data for this class.

        To be implemented by subclasses.
        """
        pass

    @abc.abstractproperty
    def report_title(self):
        """Property used for the HTML title for the report.

        To be implemented by subclasses.
        """
        pass

    def gen_html_report(self, data, css_filename, js_filename):
        """Generates an HTML report from a generated data dictionary.

        Returns an encoded byte array that can be written to disk.
        """
        chart_keys = ['has_data', 'end_date', 'historic_imei_counts', 'historic_imsi_counts',
                      'historic_msisdn_counts', 'compliance_breakdown', 'historic_compliance_breakdown',
                      'classification_conditions', 'conditions_breakdown', 'historic_conditions_breakdown']
        chart_data = {k: v for k, v in data.items() if k in chart_keys}
        chart_json = json.dumps(chart_data, cls=JSONEncoder)

        context = {
            'css': css_filename,
            'js': js_filename,
            'chart_json': chart_json,
            'data': data,
            'title': self.report_title
        }

        template = self.template_env.get_template(self.template_name)
        html = template.render(context)
        return html.encode('utf-8')

    def _gen_base_report_data(self):  # noqa: C901
        """Generates the base data for the country/operator report."""
        with self.conn.cursor() as cursor:
            cursor.execute("""SELECT *
                                FROM report_data_metadata
                               WHERE data_id = %s""",
                           [self.data_id])
            _metadata = cursor.fetchone()
            if _metadata is None:
                raise MissingStatsException('No metadata available for data_id {0:d}!'
                                            .format(self.data_id))

            cursor.execute("""SELECT *
                                FROM report_monthly_stats
                               WHERE data_id = %s
                                 AND operator_id = %s""",
                           [self.data_id, self.operator_id])
            _monthly_stats = cursor.fetchone()
            if _monthly_stats is None:
                raise MissingStatsException('report_monthly_stats entry missing for operator {0} and data_id {1:d}'
                                            .format(self.operator_id, self.data_id))

            cursor.execute("""SELECT *
                                FROM report_monthly_conditions
                               WHERE data_id = %s
                            ORDER BY sort_order""",
                           [self.data_id])
            _conditions = cursor.fetchall()
            if len(_conditions) == 0:
                self.logger.warning('No monthly condition config available for operator {0} and data_id {1:d}'
                                    .format(self.operator_id, self.data_id))

            classification_conditions = [{'label': c.cond_name,
                                          'blocking': c.was_blocking,
                                          'config': c.last_successful_config,
                                          'last_successful_run': format_datetime_for_report(c.last_successful_run)}
                                         for c in _conditions]
            cursor.execute("""SELECT *
                                FROM report_daily_stats
                               WHERE data_id = %s
                                 AND operator_id = %s
                            ORDER BY data_date""",
                           [self.data_id, self.operator_id])
            _daily_stats = cursor.fetchall()
            if len(_daily_stats) == 0:
                self.logger.warning('No daily stats available for operator {0} and data_id {1:d}'
                                    .format(self.operator_id, self.data_id))

            cursor.execute("""SELECT *
                                FROM report_monthly_condition_stats
                               WHERE data_id = %s
                                 AND operator_id = %s""",
                           [self.data_id, self.operator_id])
            _condition_stats = cursor.fetchall()
            if len(_condition_stats) == 0:
                self.logger.warning('No monthly condition stats available for operator {0} and data_id {1:d}'
                                    .format(self.operator_id, self.data_id))

            cursor.execute("""SELECT *
                                FROM report_monthly_top_models_imei
                               WHERE data_id = %s
                                 AND operator_id = %s
                            ORDER BY rank_pos""",
                           [self.data_id, self.operator_id])
            _top_models_imei = cursor.fetchall()
            if len(_top_models_imei) == 0:
                self.logger.warning('No monthly top models by IMEI available for operator {0} and data_id {1:d}'
                                    .format(self.operator_id, self.data_id))

            cursor.execute("""SELECT *
                                FROM report_monthly_top_models_gross_adds
                               WHERE data_id = %s
                                 AND operator_id = %s
                            ORDER BY rank_pos""",
                           [self.data_id, self.operator_id])
            _top_models_gross_adds = cursor.fetchall()
            if len(_top_models_gross_adds) == 0:
                self.logger.warning('No monthly top models by gross adds available for operator {0} and data_id {1:d}'
                                    .format(self.operator_id, self.data_id))

            cursor.execute("""SELECT *
                                FROM report_monthly_imei_imsi_overloading
                               WHERE data_id = %s
                                 AND operator_id = %s
                            ORDER BY seen_with_imsis""",
                           [self.data_id, self.operator_id])
            _imei_imsi_overloading = cursor.fetchall()
            if len(_imei_imsi_overloading) == 0:
                self.logger.warning('No monthly IMEI/IMSI overloading stats available for operator {0} and '
                                    'data_id {1:d}'.format(self.operator_id, self.data_id))

            cursor.execute("""SELECT *
                                FROM report_monthly_average_imei_imsi_overloading
                               WHERE data_id = %s
                                 AND operator_id = %s
                            ORDER BY bin_start""",
                           [self.data_id, self.operator_id])
            _daily_imei_imsi_overloading = cursor.fetchall()
            if len(_daily_imei_imsi_overloading) == 0:
                self.logger.warning(('No monthly average IMEI/IMSI overloading stats available for '
                                     'operator {0} and data_id {1:d}').format(self.operator_id, self.data_id))

            cursor.execute("""SELECT *
                                FROM report_monthly_imsi_imei_overloading
                               WHERE data_id = %s
                                 AND operator_id = %s
                            ORDER BY seen_with_imeis""",
                           [self.data_id, self.operator_id])
            _imsi_imei_overloading = cursor.fetchall()
            if len(_imsi_imei_overloading) == 0:
                self.logger.warning('No monthly IMSI/IMEI overloading stats available for operator {0} and '
                                    'data_id {1:d}'.format(self.operator_id, self.data_id))

            cursor.execute("""SELECT *
                                FROM report_monthly_condition_stats_combinations
                               WHERE data_id = %s
                                 AND operator_id = %s""",
                           [self.data_id, self.operator_id])
            _condition_combination_stats = cursor.fetchall()
            if len(_condition_combination_stats) == 0:
                self.logger.warning('No monthly condition combination stats available for operator {0} and '
                                    'data_id {1:d}'.format(self.operator_id, self.data_id))

        report_data = {
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat(),
            'creation_date': _metadata.data_date.isoformat(),
            'has_data': False,
            'report_schema_version': report_schema_version,
            'software_version': __version__,
        }
        if _monthly_stats.num_imeis == 0:
            self.logger.error('No data found for report - generating placeholder error report')
            return report_data

        historic_monthly_stats = self._historic_monthly_stats('report_monthly_stats')
        historic_condition_stats = self._historic_monthly_stats('report_monthly_condition_stats', as_list=True)
        num_recs_per_day = self._retrieve_daily_counts(_daily_stats, 'num_triplets')
        num_imeis_per_day = self._retrieve_daily_counts(_daily_stats, 'num_imeis')
        num_imsis_per_day = self._retrieve_daily_counts(_daily_stats, 'num_imsis')
        num_msisdns_per_day = self._retrieve_daily_counts(_daily_stats, 'num_msisdns')
        num_imeis_seen = _monthly_stats.num_imeis
        num_imsis_seen = _monthly_stats.num_imsis
        num_msisdns_seen = _monthly_stats.num_msisdns
        num_triplets_seen = _monthly_stats.num_triplets
        num_gross_adds = _monthly_stats.num_gross_adds

        compliance_breakdown = self._retrieve_compliance_breakdown(_monthly_stats)
        conditions_breakdown = self._retrieve_condition_results(_condition_stats, classification_conditions)

        top_models_imei = self._convert_top_models(_top_models_imei)
        top_models_imei_count = sum(m['count'] for m in top_models_imei)
        top_models_gross_adds = self._convert_top_models(_top_models_gross_adds)
        top_models_gross_adds_count = sum(m['count'] for m in top_models_gross_adds)

        historic_imei_counts = self._retrieve_historic_monthly_scalar(historic_monthly_stats,
                                                                      'num_imeis',
                                                                      num_imeis_seen)
        historic_imsi_counts = self._retrieve_historic_monthly_scalar(historic_monthly_stats,
                                                                      'num_imsis',
                                                                      num_imsis_seen)
        historic_msisdn_counts = self._retrieve_historic_monthly_scalar(historic_monthly_stats,
                                                                        'num_msisdns',
                                                                        num_msisdns_seen)
        historic_triplet_counts = self._retrieve_historic_monthly_scalar(historic_monthly_stats,
                                                                         'num_triplets',
                                                                         num_triplets_seen)

        historic_compliance_breakdown = [self._retrieve_compliance_breakdown(ms) for ms in historic_monthly_stats]
        historic_compliance_breakdown.append(compliance_breakdown)
        historic_conditions_breakdown = self._retrieve_historic_conditions_breakdown(conditions_breakdown,
                                                                                     historic_condition_stats,
                                                                                     classification_conditions)

        imei_imsi_overloading = [{'num_imeis': r.num_imeis, 'seen_with_imsis': r.seen_with_imsis}
                                 for r in _imei_imsi_overloading]
        imsi_imei_overloading = [{'num_imsis': r.num_imsis, 'seen_with_imeis': r.seen_with_imeis}
                                 for r in _imsi_imei_overloading]
        daily_imei_imsi_overloading = [{'num_imeis': r.num_imeis,
                                        'bin_start': r.bin_start,
                                        'bin_end': r.bin_end}
                                       for r in _daily_imei_imsi_overloading]

        report_data.update({
            'has_data': True,
            'recs_per_day': num_recs_per_day,
            'imsis_per_day': num_imsis_per_day,
            'msisdns_per_day': num_msisdns_per_day,
            'imeis_per_day': num_imeis_per_day,
            'total_imeis_seen': num_imeis_seen,
            'total_imsis_seen': num_imsis_seen,
            'total_msisdns_seen': num_msisdns_seen,
            'total_imei_imsis_seen': _monthly_stats.num_imei_imsis,
            'total_imei_msisdns_seen': _monthly_stats.num_imei_msisdns,
            'total_imsi_msisdns_seen': _monthly_stats.num_imsi_msisdns,
            'total_triplets_seen': num_triplets_seen,
            'total_records_seen': _monthly_stats.num_records,
            'total_null_imei_records': _monthly_stats.num_null_imei_records,
            'total_null_imsi_records': _monthly_stats.num_null_imsi_records,
            'total_null_msisdn_records': _monthly_stats.num_null_msisdn_records,
            'total_invalid_imei_imsis': _monthly_stats.num_invalid_imei_imsis,
            'total_invalid_imei_msisdns': _monthly_stats.num_invalid_imei_msisdns,
            'total_invalid_triplets': _monthly_stats.num_invalid_triplets,
            'historic_imei_counts': historic_imei_counts,
            'historic_imsi_counts': historic_imsi_counts,
            'historic_msisdn_counts': historic_msisdn_counts,
            'historic_triplet_counts': historic_triplet_counts,
            'total_gross_adds': num_gross_adds,
            'compliance_breakdown': compliance_breakdown,
            'historic_compliance_breakdown': historic_compliance_breakdown,
            'conditions_breakdown': conditions_breakdown,
            'historic_conditions_breakdown': historic_conditions_breakdown,
            'has_compliance_data': self.has_compliance_data,
            'top_models_imei': top_models_imei,
            'top_models_imei_count': top_models_imei_count,
            'top_models_gross_adds': top_models_gross_adds,
            'top_models_gross_adds_count': top_models_gross_adds_count,
            'classification_conditions': classification_conditions,
            'imei_imsi_overloading': imei_imsi_overloading,
            'imsi_imei_overloading': imsi_imei_overloading,
            'daily_imei_imsi_overloading': daily_imei_imsi_overloading,
            'condition_combination_table': self._retrieve_condition_combination_table(_condition_combination_stats,
                                                                                      classification_conditions)
        })
        return report_data

    def _previous_reporting_periods(self):
        """Returns a list of month/year tuples to use when generatic data for historic trends."""
        month = self.month
        year = self.year
        periods = []
        for i in range(5):
            month -= 1
            if month == 0:
                month = 12
                year -= 1
            periods.append((month, year))

        periods.reverse()
        return periods

    def _historic_monthly_stats(self, table_name, as_list=False):
        """Returns a list of historic table results for months previous to this one."""
        rv = []
        with self.conn.cursor() as cursor:
            for month, year in self._previous_reporting_periods():
                cursor.execute(sql.SQL("""SELECT *
                                            FROM {0}
                                           WHERE data_id = (SELECT MAX(data_id)
                                                              FROM report_data_metadata
                                                             WHERE report_month = %s
                                                               AND report_year = %s)
                                             AND operator_id = %s""").format(sql.Identifier(table_name)),
                               [month, year, self.operator_id])
                if not as_list:
                    # If this is None, it means there is no data. Historic monthly stats are expected to be
                    # None is there is no data for this month
                    rv.append(cursor.fetchone())
                else:
                    # For conditions, desired behaviour is the same. If no data exists for a month, we put None in
                    # the array. Otherwise, we put all the results in as a list (there is one result for every
                    # condition)
                    results = cursor.fetchall()
                    if len(results) == 0:
                        rv.append(None)
                    else:
                        rv.append(results)

        return rv

    def _retrieve_historic_conditions_breakdown(self, conditions_breakdown, historic_condition_stats,
                                                classification_conditions):
        """Retrieve the conditions sparklines data as a dict (condition label -> list of counts)."""
        historic_conditions_stats = [self._retrieve_condition_results(cs, classification_conditions)
                                     for cs in historic_condition_stats]
        historic_conditions_breakdown = defaultdict(list)
        for c in classification_conditions:
            cond_name = c['label']
            for hs in historic_conditions_stats:
                historic_conditions_breakdown[cond_name].append(hs[cond_name])
            historic_conditions_breakdown[cond_name].append(conditions_breakdown[cond_name])
        return historic_conditions_breakdown

    def _retrieve_historic_monthly_scalar(self, historic_monthly_stats, propname, current_count):
        """Returns a list of historic values for a given monthly metric."""
        rv = []
        for monthly_stat in historic_monthly_stats:
            if monthly_stat is None:
                rv.append(0)
            else:
                rv.append(getattr(monthly_stat, propname))
        rv.append(current_count)
        return rv

    def _retrieve_daily_counts(self, daily_stats, propname):
        """Returns a daily ID count (IMEI/IMSI/MSISDN) for a given function name."""
        return [{'date': res.data_date, 'count': getattr(res, propname)} for res in daily_stats]

    def _retrieve_compliance_breakdown(self, monthly_stats):
        """Converts DB results into reporting format."""
        compliance_breakdown = {
            'num_compliant_imeis': 0,
            'num_noncompliant_imeis': 0,
            'num_noncompliant_imeis_info_only': 0,
            'num_noncompliant_imeis_blocking': 0,
            'num_compliant_triplets': 0,
            'num_noncompliant_triplets': 0,
            'num_noncompliant_triplets_info_only': 0,
            'num_noncompliant_triplets_blocking': 0,
            'num_compliant_imei_imsis': 0,
            'num_noncompliant_imei_imsis': 0,
            'num_noncompliant_imei_imsis_blocking': 0,
            'num_noncompliant_imei_imsis_info_only': 0,
            'num_compliant_imei_msisdns': 0,
            'num_noncompliant_imei_msisdns': 0,
            'num_noncompliant_imei_msisdns_blocking': 0,
            'num_noncompliant_imei_msisdns_info_only': 0,
        }
        if monthly_stats is not None:
            for k in compliance_breakdown.keys():
                compliance_breakdown[k] = getattr(monthly_stats, k)

        return compliance_breakdown

    def _retrieve_condition_results(self, condition_stats, classification_conditions):
        """Converts DB results into reporting format."""
        if condition_stats is not None:
            condition_stats_lookup = {c.cond_name: c for c in condition_stats}
        else:
            condition_stats_lookup = {}
        rv = {}
        for c in classification_conditions:
            cond_name = c['label']
            rv[cond_name] = {
                'num_imeis': 0,
                'num_triplets': 0,
                'num_imei_imsis': 0,
                'num_imei_msisdns': 0,
                'num_imei_gross_adds': 0
            }
            cond_stat = condition_stats_lookup.get(cond_name, None)
            if cond_stat is not None:
                for k in rv[cond_name].keys():
                    rv[cond_name][k] = getattr(cond_stat, k)
        return rv

    def _convert_top_models(self, db_results):
        """Converts DB results into reporting format."""
        return [{'manufacturer': r.manufacturer, 'model': r.model, 'tech_generations': r.tech_generations,
                 'count': r.num_imeis} for r in db_results]

    def _retrieve_condition_combination_table(self, _condition_combination_stats, classification_conditions):
        """Helper function uses to generate combinatorial per-condition stats."""
        condition_names = [c['label'] for c in classification_conditions]
        results = []
        for stats in _condition_combination_stats:
            condition_combination = stats.combination
            row = {}
            row['combination'] = dict(zip(condition_names, condition_combination))
            for x in ['num_imeis', 'num_imei_gross_adds', 'num_imei_imsis', 'num_imei_msisdns', 'compliance_level',
                      'num_subscriber_triplets']:
                row[x] = getattr(stats, x)
            results.append(row)
        return results
