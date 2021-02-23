"""
DIRBS module for utility classes and functions.

Copyright (c) 2018-2021 Qualcomm Technologies, Inc.

All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
limitations in the disclaimer below) provided that the following conditions are met:

- Redistributions of source code must retain the above copyright notice, this list of conditions and the following
  disclaimer.
- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
  disclaimer in the documentation and/or other materials provided with the distribution.
- Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
  products derived from this software without specific prior written permission.
- The origin of this software must not be misrepresented; you must not claim that you wrote the original software.
  If you use this software in a product, an acknowledgment is required by displaying the trademark/logo as per the
  details provided here: https://www.qualcomm.com/documents/dirbs-logo-and-brand-guidelines
- Altered source versions must be plainly marked as such, and must not be misrepresented as being the original
  software.
- This notice may not be removed or altered from any source distribution.

NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
"""

import io
import os
import zipfile
import subprocess
from os.path import basename, splitext
import datetime

import dirbs.importer.exceptions as exceptions


def extract_csv_from_zip(filename):
    """Function to extract the csv file contained within the zip file."""
    try:
        with zipfile.ZipFile(filename, 'r') as zf:
            contents = zf.namelist()
            if len(contents) != 1:
                raise zipfile.BadZipFile('There is more than 1 file in the .zip archive.')

            zipfile_basename, zipfile_ext = splitext(basename(filename))
            contents_filename = basename(contents[0])
            contents_basename, contents_ext = splitext(contents_filename)

            if zipfile_basename != contents_basename:
                raise zipfile.BadZipFile('Wrong file name in archive {0} - the filename should match the .zip '
                                         'filename with a different file extension.'.format(filename))

            return zf.open(contents[0], mode='r')

    except zipfile.BadZipFile as e:
        raise zipfile.BadZipFile('Input file is not a valid .zip file: {0}'.format(str(e)))


def split_file(input_file, lines, output_dir, logger, output_file_basename='split_file'):
    """Method to split an input file into multiple files and return the split filenames."""
    batch_filename_base = os.path.join(output_dir, output_file_basename)
    batch_line_count = 0
    batch_num = 0

    # Read the header from the file
    header = input_file.readline()

    if not header.rstrip():
        # If file is empty, just return a new empty file to allow importers that accept empty files to work
        filename = '{0}.{1:d}'.format(batch_filename_base, batch_num)
        with open(filename, 'wb'):
            pass
        yield filename
        # Indicate that we are done with this generated
        return

    buf = io.BytesIO()
    buf.write(header)
    for line in input_file:
        buf.write(line)
        batch_line_count += 1

        # If we're starting a new batch, open a file
        if batch_line_count % lines == 0:  # noqa: S001
            filename = '{0}.{1:d}'.format(batch_filename_base, batch_num)
            write_buffer_to_file(buf, filename)
            buf = io.BytesIO()
            buf.write(header)
            batch_num += 1
            batch_line_count = 0
            if batch_num > 0 and batch_num % 50 == 0:
                logger.info('Written {0} split files'.format(batch_num))

            # Yield filename back
            yield filename

    # Handle final batch. We need to check if line count is > 0 (ie. there is content to write). If we never
    # wrote a batch we still need to make sure we emit the header so we have an or check for batch_num == 0
    if batch_line_count > 0 or batch_num == 0:
        filename = '{0}.{1:d}'.format(batch_filename_base, batch_num)
        write_buffer_to_file(buf, filename)
        yield filename

    # Return to indicate that we are done
    return


def write_buffer_to_file(buffer, filename):
    """Write a batch buffer to a file."""
    with open(filename, 'wb') as of:
        buffer.seek(0)
        of.write(buffer.read())
    return filename


def prevalidate_file(input_file, schema_file, validator='/opt/validator/bin/validate',
                     schema_dir='/opt/dirbs/etc/schema'):
    """Method which pre-validates the file using an external CSV validator against a CSV schema."""
    try:
        result = subprocess.check_output([validator, '-f', '1', input_file, os.path.join(schema_dir, schema_file)])
        if result.find(b'PASS') == -1:
            raise exceptions.PrevalidationCheckRawException('Pre-validation failed: {0}'.format(result))
        return input_file
    except subprocess.CalledProcessError as err:
        raise exceptions.PrevalidationCheckRawException('Pre-validation failed: {0}'.format(err.stdout))


def perform_operator_filename_checks(input_filename):
    """Perform filename check on the operator filename."""
    try:
        expected_start_date, expected_end_date = operator_expected_file_dates(input_filename)
    except Exception:
        raise exceptions.FilenameCheckRawException(
            'Invalid filename - must be in format <operator_id>_<YYYYMMDD>_<YYYYMMDD>.zip instead received {0}'
            .format(input_filename))

    if expected_end_date < expected_start_date:
        raise exceptions.FilenameCheckRawException(
            'Invalid filename - start date later than end date in filename {0}'.format(input_filename))

    if expected_start_date > datetime.date.today():
        raise exceptions.FilenameCheckRawException(
            'Invalid filename - start date in the future in filename {0}'.format(input_filename))


def operator_expected_file_dates(input_filename):
    """Returns the expected date range for this import based solely on the filename."""
    filename = os.path.basename(os.path.splitext(input_filename)[0])
    start, end = filename.split('_')[-2:]
    return datetime.datetime.strptime(start, '%Y%m%d').date(), datetime.datetime.strptime(end, '%Y%m%d').date()
