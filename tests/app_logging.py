"""
Logging unit tests.

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
import os

import pytest
import click
from click.testing import CliRunner

from dirbs.cli import common
from dirbs.logging import configure_logging, setup_file_logging
from dirbs.config import LoggingConfig
from _fixtures import *  # noqa: F403, F401
from _helpers import logger_stream_contents, logger_stream_reset


def test_exception_logging_api(logger, flask_app):
    """Test Depot ID TBD.

    Verify that exception is logged when an unhandled exception is encountered in an API.
    """
    flask_app.application.config['PROPAGATE_EXCEPTIONS'] = False
    rv = flask_app.get('/test_errors')
    assert rv.status_code == 500
    assert 'DIRBS encountered an uncaught software exception' in logger_stream_contents(logger)


def test_exception_logging_cli(logger, mocked_config):
    """Test Depot ID TBD.

    Verify that exception is logged when an unhandled exception is encountered in an API.
    """
    @click.command()
    @click.pass_context
    @common.unhandled_exception_handler
    def test_click_command(ctxt):
        """Test click program."""
        raise ValueError('Testing!')

    with pytest.raises(ValueError):
        runner = CliRunner()
        runner.invoke(test_click_command, catch_exceptions=False, obj={'APP_CONFIG': mocked_config})
    assert 'DIRBS encountered an uncaught software exception' in logger_stream_contents(logger)


def test_scrubbed_logs(logger):
    """Test Depot ID TBD.

    Verify if sensitive messages are getting scrubbed.
    """
    config = LoggingConfig(ignore_env=True, **{'enable_scrubbing': True})
    configure_logging(config)
    for msg in ['123456789', '123445677888', '11aa123456789a']:
        logger.info(msg)
        assert '<<SCRUBBED>>' in logger_stream_contents(logger)
        # Clear the stream after each iteration so we have clean stream
        logger_stream_reset(logger)

        # Also check the same thing works for a child logger
        logging.getLogger('dirbs.import').info(msg)
        assert '<<SCRUBBED>>' in logger_stream_contents(logger)
        logger_stream_reset(logger)


def test_unscrubbed_logs(logger):
    """Test Depot ID TBD.

    Verify if insensitive messages are nog getting scrubbed.
    """
    config = LoggingConfig(ignore_env=True, **{'enable_scrubbing': False})
    configure_logging(config)
    for msg in ['12345678', '123445a677888', '12345678.1']:
        logger.info(msg)
        assert '<<SCRUBBED>>' not in logger_stream_contents(logger)
        assert msg in logger_stream_contents(logger)
        # Clear the stream after each iteration so we have clean stream
        logger_stream_reset(logger)

        # Also check the same thing works for a child logger
        logging.getLogger('dirbs.import').info(msg)
        assert msg in logger_stream_contents(logger)
        logger_stream_reset(logger)


def test_file_logging(logger, tmpdir):
    """Test Depot ID TBD.

    Test that log content ends up in a file when set a logdir.
    """
    log_config = LoggingConfig(ignore_env=True, **{'log_directory': str(tmpdir)})
    setup_file_logging(log_config, 'log_file')
    message = 'test_file_logging'
    logger.error(message)
    expected_filename = os.path.join(str(tmpdir), 'log_file.log')
    with open(expected_filename, 'r') as f:
        assert message in f.read()


def test_file_logging_rotation(logger, tmpdir):
    """Test Depot ID TBD.

    Test that log content ends up in a file when set a logdir.
    """
    # Rotate log file every 20 bytes
    log_config = LoggingConfig(ignore_env=True,
                               **{'log_directory': str(tmpdir),
                                  'file_rotation_max_bytes': 20,
                                  'file_rotation_backup_count': 2})
    setup_file_logging(log_config, 'log_file')

    # These two lines are greater than file_rotation_max_bytes, so first message should get rotated
    message1 = 'test_file_logging_part1'
    message2 = 'test_file_logging_part2'

    # Log error message, should go into log_file.log
    logger.error(message1)
    logger.error(message2)

    expected_filename1 = os.path.join(str(tmpdir), 'log_file.log')
    with open(expected_filename1, 'r') as f:
        assert message2 in f.read()
        assert message1 not in f.read()

    expected_filename2 = os.path.join(str(tmpdir), 'log_file.log.1')
    with open(expected_filename2, 'r') as f:
        assert message1 in f.read()
        assert message2 not in f.read()


def test_file_logging_prefix(logger, tmpdir):
    """Test Depot ID TBD.

    Test that file_prefix works in logging config
    """
    log_config = LoggingConfig(ignore_env=True, **{'log_directory': str(tmpdir), 'file_prefix': 'foo'})
    setup_file_logging(log_config, 'log_file')
    message = 'test_file_logging'
    logger.error(message)
    expected_filename = os.path.join(str(tmpdir), 'foo_log_file.log')
    with open(expected_filename, 'r') as f:
        assert message in f.read()
