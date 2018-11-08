#! /usr/bin/env python3
"""
Script to parse machine readable JSON report and CSV compliance data and spit out some basic stats.

 Copyright (c) 2018 Qualcomm Technologies, Inc.

 All rights reserved.



 Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
 limitations in the disclaimer below) provided that the following conditions are met:


 * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 disclaimer.

 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 disclaimer in the documentation and/or other materials provided with the distribution.

 * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
 products derived from this software without specific prior written permission.

 NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
 THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 POSSIBILITY OF SUCH DAMAGE.
"""

import sys
import json
import csv
from collections import defaultdict


if len(sys.argv) != 2 and len(sys.argv) != 3:
    print('Usage: report_analyzer.py <json_filename> [<compliance_csv_filename>]')
    sys.exit(1)

with open(sys.argv[1], 'r', encoding='utf8') as f:
    json_data = json.loads(f.read())

    print('\nAnalysis of DIRBS Report')
    print('========================')
    start_date = json_data['start_date']
    end_date = json_data['end_date']
    if len(sys.argv) == 2:
        print('Filename: {0}'.format(sys.argv[1]))
    else:
        print('Filenames: {0}'.format(', '.join(sys.argv[1:3])))
    if json_data.get('operator_name') is not None:
        print('Operator: {0}'.format(json_data['operator_name']))
    elif json_data.get('country_name') is not None:
        print('Country: {0}'.format(json_data['country_name']))
    print('Period: {0} to {1}'.format(start_date, end_date))

    compliance_breakdown = json_data.get('compliance_breakdown')
    if compliance_breakdown is None:
        print('JSON data has no compliance breakdown or is a placeholder report generated when there is no data!')
        sys.exit(1)

    print('\nCompliance breakdown')
    print('====================')
    try:
        # Try version 4.0.0 keys
        meets_blocking = compliance_breakdown['meets_blocking']
        meets_non_blocking = compliance_breakdown['meets_non_blocking']
        meets_none = compliance_breakdown['meets_none']
    except KeyError:
        # Try version 4.1.0+ compliance keys
        meets_blocking = compliance_breakdown['num_noncompliant_imeis_blocking']
        meets_non_blocking = compliance_breakdown['num_noncompliant_imeis_info_only']
        meets_none = compliance_breakdown['num_compliant_imeis']
        total_imeis = meets_blocking + meets_non_blocking + meets_none

    total_imeis = meets_blocking + meets_non_blocking + meets_none
    print('Total IMEIs seen: {0:d}'.format(total_imeis))
    print('IMEIs meeting a blocking condition: {0:d}'.format(meets_blocking))
    print('IMEIs meeting only non-blocking condition: {0:d}'.format(meets_non_blocking))
    print('IMEIs meeting no conditions: {0:d}'.format(meets_none))

    blocked_imeis_pct = 0
    if total_imeis > 0:
        blocked_imeis_pct = (meets_blocking * 100) / total_imeis
    print('IMEIS meeting a blocking condition as a percentage of IMEIs seen in period: {0:.2f}'
          .format(blocked_imeis_pct))

    # If there is a CSV compliance file defined
    is_csv_data = False
    if len(sys.argv) == 3:
        with open(sys.argv[2], 'r', encoding='utf8') as f:
            csvr = csv.reader(f)
            compliance_data = list(csvr)
            is_csv_data = True
    elif json_data.get('compliance_data') is not None:
        compliance_data = json_data.get('compliance_data')
    else:
        print('JSON data does not contain per-TAC compliance data and no CSV filename specified!')
        sys.exit(1)

    headers = compliance_data[0]
    ncolunns = len(headers)
    per_tac_data = compliance_data[1:]
    imei_count_idx = headers.index('IMEI count')
    condition_names = headers[1:imei_count_idx]

    # Need to do proper type conversion here as CSV will have been converted into strings
    if is_csv_data:
        for row in per_tac_data:
            for col_idx in range(1, ncolunns):
                val = row[col_idx]
                if col_idx < imei_count_idx:
                    val = row[col_idx]
                    assert val in ['False', 'True']
                    if val == 'False':
                        row[col_idx] = False
                    else:
                        row[col_idx] = True
                else:
                    row[col_idx] = int(row[col_idx])

    try:
        try:
            sub_count_idx = headers.index('Subscriber triplet count')
        except ValueError:
            # Try pre-4.1.0 old header name
            sub_count_idx = headers.index('Subscriber count')
    except ValueError:
        print('WARNING: JSON data does not contain subscriber triplets data (generated on Release 1D or earlier)')
        sub_count_idx = -1

    if sub_count_idx != -1:
        total_triplets = sum([x[sub_count_idx] for x in per_tac_data])
        print('Total IMEI/IMSI/MSISDN triplets seen: {0:d}'.format(total_triplets))
        total_blocked_triplets = sum([x[sub_count_idx] for x in per_tac_data if x[-1] == 0])
        print('Total IMEI/IMSI/MSISDN triplets meeting blocking condition: {0:d}'.format(total_blocked_triplets))
        blocked_triplets_pct = 0
        if total_triplets > 0:
            blocked_triplets_pct = (total_blocked_triplets * 100) / total_triplets
        print('Triplets meeting blocking condition as a percentage of triplets seen in period: {0:.2f}'
              .format(blocked_triplets_pct))

    print('\nConditions breakdown')
    print('====================')
    n_conditions = len(condition_names)
    n_global_single_cond_blocking = 0
    for idx, cname in enumerate(condition_names):
        n_imeis = 0
        n_triplets = 0
        n_overlaps = defaultdict(int)
        n_single_cond = 0
        n_multiple_conds = 0
        for row in per_tac_data:
            if row[idx + 1] is True:
                n_imeis += row[imei_count_idx]
                if sum(row[1:1 + n_conditions]) == 1:
                    n_single_cond += row[imei_count_idx]
                    assert row[-1] != 2
                    if row[-1] == 0:
                        n_global_single_cond_blocking += row[imei_count_idx]
                else:
                    n_multiple_conds += row[imei_count_idx]

                if sub_count_idx != -1:
                    n_triplets += row[sub_count_idx]

                for idx2 in [x for x in range(1, n_conditions) if idx != x and row[x + 1] is True]:
                    n_overlaps[condition_names[idx2 - 1]] += row[imei_count_idx]

        if sub_count_idx != -1:
            print('Condition {0} has {1:d} matching IMEIs, {2:d} matching triplets'.format(cname, n_imeis, n_triplets))
        else:
            print('Condition {0} has {1:d} matching IMEIs'.format(cname, n_imeis))

        print('\t{0:d} of these IMEIs were only classified by this condition'.format(n_single_cond))
        print('\t{0:d} of these IMEIs were classified by multiple conditions'.format(n_multiple_conds))

        for k, v in n_overlaps.items():
            print('\t\t{0:d} of these IMEIs were also classified by condition {1}'.format(v, k))

    n_global_multiple_conds_blocking = 0
    for row in per_tac_data:
        if sum(row[1:1 + n_conditions]) > 1:
            assert row[-1] != 2
            if row[-1] == 0:
                n_global_multiple_conds_blocking += row[imei_count_idx]

    print('\nGlobally, there were {0:d} IMEIs classified by only one blocking condition'
          .format(n_global_single_cond_blocking))
    print('Globally, there were {0:d} IMEIs classified by more than one blocking condition'
          .format(n_global_multiple_conds_blocking))
    print('Adding these together, there were {0:d} total IMEIs classified by a blocking condition'
          .format(n_global_single_cond_blocking + n_global_multiple_conds_blocking))
