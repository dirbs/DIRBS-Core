#! /usr/bin/env python3
"""
POC DIRBS script for operator data validation.

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

import csv
import re
import sys

import click


# Compile some REs once for performance
name_RE = re.compile(r'^[\w,.\s]+$')
date_RE = re.compile(r'^20[0-9]{2}((0[13578]|1[02])31|(01|0[3-9]'
                     r'|1[1-2])(29|30)|(0[1-9]|1[0-2])(0[1-9]|1[0-9]|2[0-8]))'
                     r'|20([02468][048]|[13579][26])0229$')
hex_RE = re.compile(r'^[0-9A-F]+$')


class Dialect(csv.Dialect):
    """Small CSV Dialect subclass to setup CSV Dialect."""

    def __init__(self):
        """Constructor."""
        self.delimiter = ','
        self.doublequote = True,
        self.escapeChar = None
        self.lineterminator = '\r\n'
        self.quotechar = '"'
        self.quoting = csv.QUOTE_MINIMAL
        self.skipinitialspace = True
        self.strict = True
        super(Dialect, self).__init__()


@click.command()
@click.argument('input_file', type=click.File('r', encoding='utf-8'))
@click.option('--max-errors',
              default=-1,
              type=int,
              help='Maximum number of errors before bailing (default: none).')
def cli(input_file, max_errors):
    """Check that DIRBS operator data matches the required schema."""
    schema = {
        'operator_name': lambda x: (len(x) >= 1 and len(x) < 128 and name_RE.match(x)),
        'date': lambda x: date_RE.match(x),
        'imei': lambda x: (len(x) == 0 or (len(x) >= 1 and len(x) <= 16 and hex_RE.match(x))),
        'imsi': lambda x: (len(x) == 0 or (len(x) >= 1 and len(x) <= 15 and hex_RE.match(x))),
        'msisdn': lambda x: (len(x) == 0 or (len(x) >= 1 and len(x) < 20 and hex_RE.match(x))),
    }

    reader = csv.reader(input_file, dialect=Dialect())
    header = [x.lower() for x in next(reader)]
    if set(header) != set(schema.keys()):
        print('Header not present or containing incorrect columns: {0}'.format(', '.join(header)))

    errors = 0
    for row_num, r in enumerate(reader):
        for col_num, col in enumerate(r):
            if not schema[header[col_num]](col):
                print('ERROR: Value \"{0}\" failed validation for column type \"{1}\" (row {2:d}, col {3:d})'
                      .format(col, header[col_num], row_num + 2, col_num + 1))
                errors += 1
                if max_errors > 0 and errors >= max_errors:
                    print('Reached maximum number of {0:d} errors...exiting'.format(max_errors))
                    sys.exit(1)

    if errors > 0:
        print('{0:d} errors found in CSV'.format(errors))
    else:
        print('CSV file validated successfully')
    sys.exit(errors == 0)


if __name__ == '__main__':
    cli()
