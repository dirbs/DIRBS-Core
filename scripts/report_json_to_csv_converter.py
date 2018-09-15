#! /usr/bin/env python3
"""
Script to extract pre-DIRBS 5.0.0 JSON compliance data and save as a CSV file in DIRBS 5.0.0 format.

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

from os import path
import sys
import json
import csv


if len(sys.argv) != 3:
    print('Usage: ./report_json_to_csv_converter.py <input_json_filename> <output_csv_filename>')
    sys.exit(1)

with open(sys.argv[1], 'r', encoding='utf8') as input_file, open(sys.argv[2], 'w', encoding='utf8') as output_file:
    json_data = json.loads(input_file.read(), encoding='utf8')
    compliance_data = json_data.get('compliance_data')
    if compliance_data is None:
        print('JSON data does not contain per-TAC compliance data!')
        sys.exit(1)

    print('Outputting JSON data in report to CSV file {0}...'.format(path.abspath(sys.argv[2])))
    writer = csv.writer(output_file)
    for row in compliance_data:
        writer.writerow(row)
