"""
Generate the operator data in csv file.

CSV file Encoding : UTF-8
CSV Table Fields
Operator_Name   Text string
Date    8 digits YYYYMMDD
IMEI    14-16 digits
IMSI    14-15 digits
MSISDN  Up to 20 digits

IMSI
Pakistan

MCC	MNC	Network	Operator or brand name	Status
410	1  	Mobilink-PMCL	Mobilink	Operational
410	3  	Pakistan Telecommunication Mobile Ltd	Ufone	Operational
410	4  	China Mobile	Zong	Operational
410	6  	Telenor Pakistan	Telenor	Operational
410	7  	WaridTel	Warid	Operational

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

import os
import sys
import csv
import codecs
import logging


def _utf8_csv_adaptor(f, encoding='utf-8'):
    for cur_line in f:
        yield cur_line.encode(encoding)


def _csv_reader(csv_file_path, encoding='utf-8'):
    with codecs.open(csv_file_path, 'rb', encoding=encoding) as f:
        reader = csv.reader(_utf8_csv_adaptor(f), delimiter=b',', quotechar=b'')

        first_row = True
        for row in reader:
            if first_row:
                first_row = False
                continue
            else:
                yield row


class OperatorTable:
    """OperatorTable class."""

    operator_index = ''
    operator_list = ['Mobilink', 'Telenor']
    operators = {}

    def __init__(self):
        """Constructor."""
        self.table_header = ['Operator_Name', 'Date', 'IMEI', 'IMSI', 'MSISDN']
        mobilink = {'name': 'Mobilink', 'mcc': '', 'mnc': '', 'msisdn_cc': '92', 'msisdn_ndc': '345'}
        telenor = {'name': 'Telenor Pakistan', 'mcc': '', 'mnc': '', 'msisdn_cc': '92', 'msisdn_ndc': '300'}
        self.operators = {'Mobilink': mobilink, 'Telenor': telenor}
        self.buffer_size = 1000
        self.output_buffer = []
        self.output_csv_name = 'default.out.csv'
        self.input_csv_name = ''
        self.output = None
        self.index = 0
        self.delimiter = ','

    def _generate_operator_name(self):

        return self.operators[self.operator_index]['name']
        # return u"Telenor Pakistan"

    # length(1,20) regex("^[0-9A-F]+$")

    # current length of msisdn of Pakistan operators is 12 digits

    def convert_table(self, input_csv, dest_csv):
        r"""Convert the dump data from netvision to formated operator table.

        1. change the title
        2. change end line "\n" to "\r\n"
        """
        self.input_csv_name = input_csv
        self.output_csv_name = dest_csv
        self._new_output(self.output_csv_name)

        # self.table_header = [u'Operator_Name', u'Date', u'IMEI', u'IMSI', u'MSISDN']
        #   Operator_Name   Text string
        #   Date    8 digits YYYYMMDD
        #   IMEI    14-16 digits
        #   IMSI    14-15 digits
        #   MSISDN  Up to 20 digits

        # source csv file format
        # 'OPERATOR_NAME,DATE,IMEI,IMSI,MSISDN'
        #   0,              1,  2,   3,    4

        # csv line end is "0A" ==> u'/r/n'

        with codecs.open(input_csv, 'rb', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')

            first_row = True

            for row in reader:
                if first_row:
                    self.index = 0
                    first_row = False
                    # write header
                    self.output_buffer.append(self.delimiter.join(self.table_header))
                    continue
                else:
                    self.index += 1
                    # print self.index, row
                    new_row = row
                    # self.output_buffer.append((self.delimiter).join(items))
                    # operator_name
                    # new_row.append(row[0])
                    # # date
                    # new_row.append(row[1])
                    # # imei
                    # new_row.append(row[2])
                    # # imsi
                    # new_row.append(row[3])
                    # # msisdn
                    # new_row.append(row[4])

                    self.output_buffer.append(self.delimiter.join(new_row))
                    # flush buffer to disk
                    if len(self.output_buffer) > self.buffer_size:
                        self._write_buffer()
                if self.index % 10000 == 0:
                    print(('Finished ' + str(self.index) + ' records.'))

        self._close_output()
        print('Total ' + str(self.index) + ' records finished')

    # open csv file
    def _new_output(self, output_file, encoding='utf-8'):
        self.index = 0
        self.output_csv_name = output_file

        # output file handle
        try:
            self.output = codecs.open(self.output_csv_name, 'w', encoding=encoding)
        except:  # noqa: E722
            logging.error('Failed to open the output file: ' + self.output_csv_name)
            raise

    # flush the output buffer and close the csf file
    #
    def _close_output(self):
        # close the existing output
        if self.output is not None:
            # close buffer
            if len(self.output_buffer) != 0:
                out_str = '\r\n'.join(self.output_buffer) + '\r\n'
                self.output.write(out_str)
                self.output_buffer = []
            # close csv
            self.output.close()
            logging.info('Converted from ' + self.input_csv_name + ' to ' + self.output_csv_name)
            logging.info('Records Number : ' + str(self.index))

    def _write_buffer(self):
        """Write records from buffer to the output file."""
        self.output.write('\r\n'.join(self.output_buffer) + '\r\n')
        self.output_buffer = []


def main():
    """CLI entrypoint."""
    input_csv = sys.argv[1].decode('utf-8')

    head, tail = os.path.split(input_csv)
    dest_csv = head + os.sep + tail.split('.')[0] + '_new' + '.' + 'csv'
    g = OperatorTable()

    g.convert_table(input_csv, dest_csv)

    # logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
    #                     filename="." + os.sep + 'dg.log', level=logging.DEBUG)


if __name__ == '__main__':
    main()
