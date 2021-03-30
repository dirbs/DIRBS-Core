"""
Package for DIRBS REST-ful API common handlers package.

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

from typing import Callable

from werkzeug.exceptions import BadRequest
from marshmallow import fields, validate


def validate_error(error) -> BadRequest:
    """
    Transform marshmallow validation errors to custom responses to maintain backward-compatibility.

    Arguments:
        error: intercepted http error object
    Returns:
        custom http error message
    """
    field_name = error.exc.field_names[0]
    field_value = error.exc.data[field_name] if error.exc.data else None
    field_type = error.exc.fields[0]
    if isinstance(field_type, fields.List) and field_value and isinstance(error.exc.messages, list):
        field_type = error.exc.fields[0].container
        field_value = error.exc.data[field_name][next(iter(error.exc.messages[field_name]))]
    else:
        field_value = error.exc.messages[field_name]
    return BadRequest(description=get_error_desc(field_type, field_name, field_value))


def get_error_desc(field: fields, name: str, value: str) -> str:
    """
    Helper function to construct error description.

    Arguments:
        field -- evaluated argument
        name -- name of the evaluated argument
        value -- evaluated value of the argument
    Returns:
        error_desc -- custom description of the error
    """
    error_desc = "Bad \'{0}\':\'{1}\' argument format.".format(name, value)
    if isinstance(field, fields.Integer):
        try:
            if isinstance(value, str):
                int(value)
                msg_allow_zero = 'or equal to ' if field.validate.min < 1 else ''
                error_desc = "Param \'{0}\':\'{1}\' must be greater than {2}0".format(name, value, msg_allow_zero)
            else:
                raise ValueError
        except ValueError:
            error_desc = "Bad \'{0}\':\'{1}\' argument format. Accepts only integer".format(name, value)
    if isinstance(field, fields.Boolean):
        allowed_values = ['0', '1', 'true', 'false']
        error_desc = "Bad \'{0}\':\'{1}\' argument format. Accepts only one of {2}".format(name, value, allowed_values)
    if isinstance(field, fields.String) and isinstance(field.validate, validate.OneOf):
        error_desc = "Bad \'{0}\':\'{1}\' argument format. Accepts only one of {2}".format(name, value,
                                                                                           field.validate.choices)
    if isinstance(field, fields.DateTime):
        dateformat = 'YYYYMMDD' if field.dateformat == '%Y%m%d' else field.dateformat
        error_desc = "Bad \'{0}\':\'{1}\' argument format. Date must be in \'{2}\' format."\
            .format(name, value, dateformat)
    return error_desc


def disable_options_method() -> Callable:
    """Decorator function to disable OPTIONS method on the endpoint."""
    def wrapper(f):
        f.provide_automatic_options = False
        return f
    return wrapper
