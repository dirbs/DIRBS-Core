"""
DIRBS package for logging and metrics.

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

import logging
import logging.config
from logging.handlers import RotatingFileHandler
import re
import io
import sys
import os.path

from .statsd import StatsClient  # noqa:F401 -- this is simply re-exported as public interface


# A default format string that is used before the config can be loaded
DEFAULT_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


class LoggingException(Exception):
    """Custom class for logging-related exceptions."""

    pass


class LogFormatter(logging.Formatter):
    """Class used to filter logs to obfuscate sensitive information (IMEI/IMSI/MSISDN)."""

    def __init__(self, enable_scrubbing=False, *args, **kwargs):
        """Constructor."""
        super().__init__(*args, **kwargs)
        # Compile regex pattern to match 9 or more consecutive digits for IMEI/MSISDN match
        self.scrubber_regex = re.compile(r'([0-9]){9,}')
        # Whether or not to apply scrubbing to formatted messages
        self.enable_scrubbing = enable_scrubbing

    def format(self, record):  # noqa: A003
        """Implements logging.Formatter interface."""
        # Delete all occurences of pattern matching confidential information
        formatted_msg = super().format(record)
        if self.enable_scrubbing:
            formatted_msg = self.scrubber_regex.sub('<<SCRUBBED>>', formatted_msg)
        return formatted_msg


class InfoAndBelowFilter(logging.Filter):
    """Filter used to select only info level and below messages so that these can be directed to stdout."""

    def filter(self, rec):  # noqa: A003
        """Overrides logging.Filter.filter."""
        return rec.levelno <= logging.INFO


class WarningAndAboveFilter(logging.Filter):
    """Filter used to select only warning and above messages so that these can be directed to stderr."""

    def filter(self, rec):  # noqa: A003
        """Overrides logging.Filter.filter."""
        return rec.levelno >= logging.WARNING


def setup_initial_logging():
    """Function to initialize logging for DIRBS."""
    # We create a buffer for our initial log input in case we have a file configured. This way we catch any early
    # output and make sure it is written to the file
    buffer_stream = io.StringIO()
    dict_config = {
        'version': 1,
        'formatters': {
            'dirbs_default': {
                'class': 'logging.Formatter',
                'format': DEFAULT_FORMAT
            }
        },
        'handlers': {
            'dirbs_stdout': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'formatter': 'dirbs_default',
                'stream': sys.stdout,
                'filters': ['info_and_below']
            },
            'dirbs_stderr': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'formatter': 'dirbs_default',
                'filters': ['warning_and_above']
            },
            'dirbs_buffer': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'stream': buffer_stream,
                'formatter': 'dirbs_default'
            }
        },
        'loggers': {
            'dirbs': {
                'level': 'DEBUG',
                'propagate': False,
                'handlers': ['dirbs_stdout', 'dirbs_stderr', 'dirbs_buffer']
            },
            'werkzeug': {
                'level': 'DEBUG',
                'handlers': ['dirbs_stdout', 'dirbs_stderr']
            }
        },
        'filters': {
            'info_and_below': {
                '()': 'dirbs.logging.InfoAndBelowFilter',
            },
            'warning_and_above': {
                '()': 'dirbs.logging.WarningAndAboveFilter',
            }
        }
    }

    for log_instance in ['sql', 'flask', 'statsd', 'config', 'exception', 'classify', 'db',
                         'listgen', 'import', 'report', 'prune', 'catalog']:
        dict_config['loggers']['dirbs.' + log_instance] = {
            'level': 'DEBUG',
            'propagate': True
        }

    logging.config.dictConfig(dict_config)


def _configure_handler(handler, log_config):
    """Helper function to configure a handler."""
    level = logging._nameToLevel.get(log_config.level.upper(), logging.INFO)
    handler.setFormatter(LogFormatter(enable_scrubbing=log_config.enable_scrubbing,
                                      fmt=log_config.format))
    level = logging._nameToLevel.get(log_config.level.upper(), logging.INFO)
    handler.setLevel(level)
    return handler


def configure_logging(log_config):
    """Alter the configuration of logging once the log config has been loaded."""
    logger = logging.getLogger('dirbs')
    for h in logger.handlers:
        _configure_handler(h, log_config)

    if not log_config.show_statsd_messages:
        logging.getLogger('dirbs.statsd').propagate = False

    if not log_config.show_werkzeug_messages:
        del logging.getLogger('werkzeug').handlers[:]

    if not log_config.show_sql_messages:
        logging.getLogger('dirbs.sql').propagate = False


def setup_file_logging(log_config, filename_root):
    """Logging helper function that gets called *after* import ID, etc. is generated.

    It is a separate function so that we can generate a more meaningful filename for our logs.
    """
    logger = logging.getLogger('dirbs')
    # Get all the early log content we've been buffering to write to a file
    buffer_handlers = [hdlr for hdlr in logger.handlers if hdlr.name == 'dirbs_buffer']
    # If this assert fails, it means we either didn't call setup_initial_logging() or
    # this function got called twice
    assert len(buffer_handlers) == 1
    buffer_handler = buffer_handlers[0]
    buffer_stream = buffer_handler.stream
    buffer_contents = buffer_stream.getvalue()

    # Check if we have a log directory configured. If not, we do nothing
    log_directory = log_config.log_directory
    if log_directory is None:
        return

    # Check whether we can write to the logdir
    log_dir_path = os.path.abspath(log_directory)
    if not os.path.isdir(log_dir_path):
        raise LoggingException('log_directory {0} specified in logging but is not a valid directory!'
                               .format(log_dir_path))

    try:
        log_file_prefix = log_config.file_prefix
        if log_file_prefix is not None:
            filename = os.path.join(log_dir_path, '{0}_{1}.log'.format(log_file_prefix, filename_root))
        else:
            filename = os.path.join(log_dir_path, '{0}.log'.format(filename_root))

        # Flush out our saved buffer
        with open(filename, mode='a', encoding='utf-8') as of:
            of.write(buffer_contents)

        # Create handler and add to loggers
        handler = RotatingFileHandler(filename,
                                      maxBytes=log_config.file_rotation_max_bytes,
                                      backupCount=log_config.file_rotation_backup_count,
                                      encoding='utf-8')
        _configure_handler(handler, log_config)
        logger.addHandler(handler)
        if log_config.show_werkzeug_messages:
            logging.getLogger('werkzeug').addHandler(handler)

        # Now remove the buffer handler from both werkzeug and dirbs loggers
        logger.removeHandler(buffer_handler)

        return True
    except IOError as ex:
        raise LoggingException(str(ex))
