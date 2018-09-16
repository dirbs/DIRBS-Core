#! /usr/bin/env python3
"""
Script to remove the operator name column from 1B and earlier data.

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
import sys

import click


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
@click.argument('output', type=click.File('w', encoding='utf-8'))
def cli(input_file, output):
    """Removes operator name from existing 1B and earlier operator test data."""
    reader = csv.reader(input_file, dialect=Dialect())
    writer = csv.writer(output, dialect=Dialect())
    header = [x.lower() for x in next(reader)]

    if header[0] != 'operator_name':
        print('Input file does not have operator name present!')
        sys.exit(1)

    # Write out header without the operator name
    writer.writerow(header[1:])

    for row in reader:
        writer.writerow(row[1:])


if __name__ == '__main__':
    cli()
