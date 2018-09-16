"""
Script to anony gsma data dumps.

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
import hashlib

import CaesarCipher

file_read = sys.argv[1]
file_read_split = file_read.split('.')
file_read_split[-2] = file_read_split[-2] + '_anonymized'
file_output = '.'.join(file_read_split)
print('\033[0;32m' + 'Anonymizing GSMA TAC DB and MNO data dumps' + '\033[0;38m\n')
# https://confluence.qualcomm.com/confluence/pages/viewpage.action?pageId=184477904

[cipher, cipher_reverse] = CaesarCipher.caesar_cipher()

first_line = True
tac_col_idx = -1  # sentinel value
alloc_date_col_idx = -1  # sentinel value
with open(file_read) as f:
    with open(file_output, 'w') as fop:
        for line in f:
            line_split = line.split('|')
            if first_line:
                headers = [x.lower().replace(' ', '_').strip() for x in line_split]
                tac_col_idx = headers.index('tac')
                alloc_date_col_idx = headers.index('allocation_date')
                fop.write(line)
                first_line = False
            else:
                new_line = []
                for idx, col in enumerate(line_split):
                    if idx == tac_col_idx:
                        new_line.append(col.translate(cipher))
                    elif idx == alloc_date_col_idx:
                        new_line.append('01-Jan-2001')
                    else:
                        new_line.append(hashlib.sha1(col.encode('utf-8')).hexdigest())
                fop.write('|'.join(new_line) + '\n')
