"""
DIRBS REST-ful Pagination module.

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


class Pagination:
    """DIBRS Pagination module class implementation."""

    @staticmethod
    def paginate(data, offset=1, limit=10):
        """Method to paginate data-set based on offset and limit.
        :param data: input data to paginate
        :param offset: to start with
        :param limit: limit of data
        :return: paginated data
        """
        if offset is None:
            offset = 1

        if limit is None:
            limit = 10
        result_size = len(data)

        keys = {
            'offset': offset,
            'limit': limit,
            'previous_key': offset,
            'next_key': '',
            'result_size': result_size
        }

        if result_size is not 0:
            if offset < 1 or offset > result_size:
                return {'keys': keys, 'data': []}
            else:
                if result_size < offset:
                    return {'keys': keys, 'data': data}
                if offset == 1:
                    keys['previous_key'] = ''
                else:
                    offset_copy = max(1, offset - limit)
                    limit_copy = offset - 1
                    keys['previous_key'] = '?offset={offset}&limit={limit}'.format(
                        offset=offset_copy, limit=limit_copy)
                if offset + limit > result_size:
                    keys['next_key'] = ''
                else:
                    offset_copy = offset + limit
                    keys['next_key'] = '?offset={offset}&limit={limit}'.format(offset=offset_copy, limit=limit)
                paginated_data = data[(offset - 1):(offset - 1 + limit)]
            return {'keys': keys, 'data': paginated_data}
        return {'keys': keys, 'data': []}
