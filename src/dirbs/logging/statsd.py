"""
DIRBS implementation of StatsD metrics using statsd.

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
import multiprocessing

import statsd
import statsd.client


# Create a multiprocessing lock so that StatsClient can be use by mulitprocessing/concurrent futures
_lock = multiprocessing.RLock()

# Create shared logger
_logger = logging.getLogger('dirbs.statsd')


class StatsClient:
    """Small wrapper class around statsd.StatsClient, largely for logging purposes."""

    def __init__(self, statsd_config):
        """Constructor."""
        self._config = statsd_config
        self._client = statsd.StatsClient(host=statsd_config.hostname,
                                          port=statsd_config.port,
                                          prefix=statsd_config.prefix)

    def timer(self, stat, rate=1):
        """Logging, thread-safe, multiprocess-safe version of StatsClient.timer.

        Make sure we pass in self here so that the eventual timing() call goes through this wrapper class
        """
        with _lock:
            return statsd.client.Timer(self, stat, rate)

    def timing(self, stat, delta, rate=1):
        """Logging, thread-safe, multiprocess-safe version of StatsClient.timing."""
        with _lock:
            _logger.info('Sending StatsD timing stat %s with value %d', stat, delta)
            return self._client.timing(stat, delta, rate)

    def incr(self, stat, count=1, rate=1):
        """Logging, thread-safe, multiprocess-safe version of StatsClient.incr."""
        with _lock:
            _logger.info('Incrementing StatsD counter %s by count %d', stat, count)
            return self._client.incr(stat, count, rate)

    def decr(self, stat, count=1, rate=1):
        """Logging, thread-safe, multiprocess-safe version of StatsClient.decr."""
        with _lock:
            _logger.info('Decrementing StatsD counter %s by count %d', stat, count)
            return self._client.decr(stat, count, rate)

    def gauge(self, stat, value, rate=1, delta=False):
        """Logging, thread-safe, multiprocess-safe version of StatsClient.gauge."""
        with _lock:
            if delta:
                _logger.info('Incrementing StatsD gauge %s by delta %d', stat, value)
            else:
                _logger.info('Setting StatsD gauge %s to value %d', stat, value)
            return self._client.gauge(stat, value, rate, delta)

    def set(self, stat, value, rate=1):  # noqa: A003
        """Logging, thread-safe, multiprocess-safe version of StatsClient.set."""
        with _lock:
            self._logger.info('Setting StatsD stat %s to value %d', stat, value)
            self._client.set(stat, value, rate)
