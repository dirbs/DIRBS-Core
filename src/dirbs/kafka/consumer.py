"""
DIRBS Core Kafka high-level consumer module.

Copyright (c) 2018-2020 Qualcomm Technologies, Inc.

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
  If you use this software in a product, an acknowledgment is required by displaying the trademark/log as per the
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

import json

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable


class KConsumer:
    """Class responsible for consuming data from the assign topic."""

    # TODO: add support for multiple topics

    def __init__(self, config, kafka_host, kafka_port, kafka_topic, logger):
        """Constructor."""
        self._config = config
        self._host = kafka_host
        self._port = kafka_port
        self._topic = kafka_topic
        self._logger = logger

    @property
    def kafka_host(self):
        """Property to return the current kafka host address."""
        return self._host

    @property
    def kafka_port(self):
        """Property to return the current kafka server port."""
        return self._port

    @property
    def kafka_topic(self):
        """Property to return the current topic name."""
        return self._topic

    @property
    def bootstrap_server_addr(self):
        """Property to formulate bootstrap server address from self."""
        return '{0}:{1}'.format(self.kafka_host, self.kafka_port)

    def _deserialize_value(self, value):
        """Method to return deserialized json values, should be passed to kafka consumer instance only."""
        try:
            return json.loads(value.decode('utf-8'))
        except json.decoder.JSONDecodeError:
            return json.loads('{"type": "unrecognised"}')

    def create_consumer(self):
        """Method to create a new consumer to a kafka topic."""
        try:
            self._logger.info('Creating new KAFKA consumer on host {0} using topic {1}'
                              .format(self.bootstrap_server_addr, self.kafka_topic))

            return KafkaConsumer(self.kafka_topic, bootstrap_servers=[self.bootstrap_server_addr],
                                 group_id=None, auto_offset_reset='latest',
                                 value_deserializer=self._deserialize_value)
        except NoBrokersAvailable as e:
            self._logger.info('No Broker {0} available, check if hostname and port is right...'
                              .format(self.bootstrap_server_addr))
            raise e
        except KafkaError as e:
            self._logger.info('Exception encountered during creation to new KAFKA consumer instance, '
                              'see the log trace below...')
            self._logger.info(str(e))
            raise e
        except Exception as e:
            self._logger.info('Exception encountered during creation to new KAFKA consumer instance')
            raise e
