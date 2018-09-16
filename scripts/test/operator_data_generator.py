"""
Generate the operator data in csv file.

CSV file Encoding : UTF-8
CSV Table Fields
Operator_Name   Text string
Date    8 digits YYYYMMDD
IMEI    14-16 digits
IMSI    14-15 digits
MSISDN  Up to 15 digits

IMSI
Pakistan

MCC MNC Network Operator or brand name  Status
410 1   Mobilink-PMCL   Mobilink    Operational
410 3   Pakistan Telecommunication Mobile Ltd   Ufone   Operational
410 4   China Mobile    Zong    Operational
410 6   Telenor Pakistan    Telenor Operational
410 7   WaridTel    Warid   Operational

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
import datetime
import codecs
import argparse
import logging
import random


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

    operator_name_index = ''
    operator_list = ['Mobilink', 'PTCL', 'Ufone', 'Zong', 'SCO Mobile 1', 'Telenor', 'Warid Pakistan', 'SCO Mobile 2']
    operators = {}

    def __init__(self):
        """Constructor."""
        self.table_header = [u'operator_name', u'msisdn', u'imei', u'imsi', u'date']
        mobilink = {'name': 'Mobilink', 'mcc': '410', 'mnc': '01', 'msisdn_cc': '92', 'msisdn_ndc': '345'}
        ptcl = {'name': 'PTCL', 'mcc': '410', 'mnc': '02', 'msisdn_cc': '92', 'msisdn_ndc': '344'}
        ufone = {'name': 'Ufone', 'mcc': '410', 'mnc': '03', 'msisdn_cc': '92', 'msisdn_ndc': '343'}
        zong = {'name': 'Zong', 'mcc': '410', 'mnc': '04', 'msisdn_cc': '92', 'msisdn_ndc': '342'}
        sco1 = {'name': 'SCO Mobile 1', 'mcc': '410', 'mnc': '06', 'msisdn_cc': '92', 'msisdn_ndc': '341'}
        telenor = {'name': 'Telenor Pakistan', 'mcc': '410', 'mnc': '06', 'msisdn_cc': '92', 'msisdn_ndc': '300'}
        warid = {'name': 'Warid Pakistan', 'mcc': '410', 'mnc': '07', 'msisdn_cc': '92', 'msisdn_ndc': '200'}
        sco2 = {'name': 'SCO Mobile 2', 'mcc': '410', 'mnc': '08', 'msisdn_cc': '92', 'msisdn_ndc': '100'}
        self.operators = {'Mobilink': mobilink,
                          'PTCL': ptcl,
                          'Ufone': ufone,
                          'Zong': zong,
                          'SCO Mobile 1': sco1,
                          'Telenor': telenor,
                          'Warid Pakistan': warid,
                          'SCO Mobile 2': sco2}
        self.buffer_size = 2000
        self.output_buffer = []
        self.output_csv_name = 'default.out.csv'
        self.input_csv_name = ''
        self.output = None
        self.index = 0
        self.delimiter = u','
        self.file_header = ''

    def _generate_operator_name(self):
        return self.operators[self.operator_name_index]['name']
        # return u'Telenor Pakistan'

    def _convert_imsi(self, imsi):
        mcc = self.operators[self.operator_name_index]['mcc']
        mnc = self.operators[self.operator_name_index]['mnc']
        imsi_part = imsi[6:]
        new_imsi = mcc + mnc + ''.join(random.sample(imsi_part, len(imsi_part)))

        return new_imsi

    @staticmethod
    def _convert_imei(imei):
        # if imei_length==14: # old IMEI without "Luhn checksum" digit
        #
        #     pass
        #
        # elif imei_length == 15: # IMEI with "Luhn checksum" digit
        #     pass
        #
        # elif imei_length==16: # IMEI with software version 2 digits in the end
        #     pass

        # remove the leading '0' ?
        # imei=imei.lstrip("0")
        imei_length = len(imei)

        if imei_length < 14:
            return imei

        tac = imei[:8]
        sn = imei[8:]
        new_imei = tac + ''.join(random.sample(sn, len(sn)))

        return new_imei

    # length(1,15) regex("^[0-9A-F]+$")

    # current length of msisdn of Pakistan operators is 12 digits
    #
    def _generate_msisdn(self, index):

        #
        # CC + NDC +SN
        # CC        Pakistan 92
        # NDC        Maxis        17
        # SN        Subscriber's number	11 digits

        # https://www.itu.int/oth/T0202.aspx?parent=T0202

        msisdn_cc_pakistan = self.operators[self.operator_name_index]['msisdn_cc']
        msisdn_ndc = self.operators[self.operator_name_index]['msisdn_ndc']
        items = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0']
        random.shuffle(items)
        msisdn = ''.join(items)

        msisdn = msisdn_cc_pakistan + msisdn_ndc + msisdn

        index_length = len(str(index))
        msisdn = msisdn[:-index_length] + str(index)
        return msisdn

    # YYYYMMDD
    # date:  regex('^20[0-9]{2}((0[13578]|1[02])31|(01|0[3-9]|1[1-2])(29|30)|(0[1-9]|1[0-2])(0[1-9]|1[0-9]|2[0-8]))
    # |20([02468][048]|[13579][26])0229$')
    @staticmethod
    def _generate_date():
        i = datetime.datetime.now()
        day_items = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15']
        # month_items=['01','02','03','04','05','06','07','08','09','10','11','12']
        # get last month as the month
        last_month = i.month - 1
        month_result = 0
        if last_month <= 0:
            last_month = 12
        if last_month < 10:
            month_result = '0' + str(last_month)

        random.shuffle(day_items)
        random_date = str(i.year) + month_result + str(day_items[0])
        return random_date

    def generate_table(self, source_csv, output_file_name, operator_name):
        """Main public function generating operator table."""
        self.input_csv_name = source_csv
        self.output_csv_name = output_file_name
        self.operator_name_index = operator_name
        self._new_output(self.output_csv_name)

        # self.file_header = [u'operator_name', u'date', u'imei', u'imsi', u'msisdn', ]
        self.file_header = [u'date', u'imei', u'imsi', u'msisdn', ]
        #   Operator_Name   Text string
        #   Date    8 digits YYYYMMDD
        #   IMEI    14-16 digits
        #   IMSI    14-15 digits
        #   MSISDN  Up to 20 digits

        # source csv file format
        # 'IMSI,IMEI,FETCH_DATE_TIME,VOICE_OR_DATA,DATE_LOGGED,OPERATOR,NETVISION_FILE_ID'
        # 0,    1,      2,              3,              4,          5,      6

        # source csv file format
        # 'OPERATOR_NAME,DATE,IMEI,IMSI,MSISDN
        # 0,                1,   2,    3,    4
        with codecs.open(source_csv, 'rb', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')

            first_row = True

            for row in reader:
                if first_row:
                    self.index = 0
                    first_row = False
                    # write header
                    self.output_buffer.append(self.delimiter.join(self.file_header))
                    continue
                else:
                    self.index += 1
                    # print self.index, row
                    new_row = []
                    # self.output_buffer.append((self.delimiter).join(items))
                    # operator_name
                    # new_row.append(self._generate_operator_name())
                    # date
                    new_row.append(self._generate_date())
                    # imei
                    new_row.append(self._convert_imei(row[2]))
                    # imsi
                    new_row.append(self._convert_imsi(row[3]))
                    # msisdn
                    new_row.append(self._generate_msisdn(self.index))

                    self.output_buffer.append(self.delimiter.join(new_row))
                    # flush buffer to disk
                    if len(self.output_buffer) > self.buffer_size:
                        self._write_buffer()

                    if self.index % 10000 == 0:
                        print('Generated ' + str(self.index) + ' records.')
        print('Total generated ' + str(self.index) + ' records.')
        self._close_output()

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
                out_str = u'\r\n'.join(self.output_buffer)
                self.output.write(out_str)
                self.output_buffer = []
            # close csv
            self.output.close()
            logging.info('Convert from ' + self.input_csv_name + ' to ' + self.output_csv_name)

    def _write_buffer(self):
        """Write records from buffer to the output file."""
        self.output.write(u'\r\n'.join(self.output_buffer) + u'\r\n')
        self.output_buffer = []


def main():
    """CLI entrypoint."""
    g = OperatorTable()

    arg_parser = argparse.ArgumentParser(description='Generate operator data by other operator data source '
                                                     '\n command example: operator_data_gen '
                                                     '--source operator_souce_csv '
                                                     '--operator_id 1 '
                                                     '--out new_operator_csv  ')
    arg_parser.add_argument('--source', dest='source_operator_csv', required=True,
                            help='source operator data in csv format  with header sequence '
                                 '\'OPERATOR_NAME,DATE,IMEI,IMSI,MSISDN\'')
    arg_parser.add_argument('--out', dest='output_operator_csv', required=True,
                            help='name of the generated operator data')
    arg_parser.add_argument('--operator', dest='operator_id', required=False,
                            help='operator id from 0 to ' + str(len(g.operator_list) - 1) +
                                 '\nif operator is not set the program will pick a randon operator id')
    # input_csv = sys.argv[1].decode('utf-8')
    args = arg_parser.parse_args()

    # operator_name_index=2 # u'Telenor Pakistan'
    # g.generate_table('AVANTEL-Datos-20151101-1K.csv','Mobilink')
    if args.operator_id == '':
        operator_name = g.operator_list[random.randint(0, len(g.operator_list) - 1)]
    else:
        operator_name = g.operator_list[int(args.operator_id)]
    # g.generate_table('CLARO-Datos-20151101-1K.csv',operator_name)
    g.generate_table(args.source_operator_csv, args.output_operator_csv, operator_name)

    # logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', filename='dg.log', level=logging.DEBUG)


if __name__ == '__main__':
    main()
