"""
Reformatter to take NetVision GSMA table output, and match it to the sample file we received from CACF.

usage: reformatter.py inputfile outputfile

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
import csv

infile = open(sys.argv[1], 'r', encoding='UTF-8')
outfile = open(sys.argv[2], 'w', encoding='UTF-8')

# for proper case formatting of header row:
capstrings = ['TAC', 'NFC', 'WLAN']

# csv read/write settings here largely a result of trial and error, but seem to be OK
myreader = csv.reader(infile, dialect='excel')
# use a quote character (unassigned unicode character) that should never appear in the data to ensure no quote attempts
mywriter = csv.writer(outfile, delimiter='|', quotechar='\u218c', quoting=csv.QUOTE_NONE, lineterminator='\n')
rownum = 0

for row in myreader:
    newrow = []
    # i.e. we are at the header
    if rownum == 0:
        for colname in row:
            # these headers are all caps
            if colname in capstrings:
                newrow.append(colname)
            else:
                # other header fields are Proper Case, with underscores removed
                newrow.append((colname.title()).replace('_', ' '))
    else:
        # non-header rows:
        for colvalue in row:
            # get rid of NetVision backslash escaping of backslashes and commas
            colvalue = colvalue.replace('\\\\', '\\')
            colvalue = colvalue.replace('\\,', ',')
            newrow.append(colvalue)
    mywriter.writerow(newrow)
    rownum += 1

infile.close()
outfile.close()
