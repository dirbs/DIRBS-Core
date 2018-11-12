#!/usr/bin/env python3
"""
Module for checking whether a date regex matches any valid date.

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

import re
import datetime

startdate = datetime.date(2000, 1, 1)
enddate = datetime.date(2100, 1, 1)

regex = re.compile('^(20[0-9]{2}((0[13578]|1[02])31|(01|0[3-9]|1[0-2])(29|30)'
                   '|(0[1-9]|1[0-2])(0[1-9]|1[0-9]|2[0-8]))|20([02468][048]|[13579][26])0229)$')

d = startdate
while d < enddate:
    dstr = d.strftime('%Y%m%d')
    if not regex.search(dstr):
        print('Failed test for {0}'.format(dstr))
    for s in ['x' + dstr, dstr + 'x']:
        if regex.search(s):
            print('False match for {0}'.format(s))
    d += datetime.timedelta(days=1)

    if d.day == 1:
        ld = d - datetime.timedelta(days=1)
        dstr = ld.strftime('%Y%m') + '{0:02d}'.format(ld.day + 1)
        if regex.search(dstr):
            print('Failed test for invalid date {0}'.format(dstr))
