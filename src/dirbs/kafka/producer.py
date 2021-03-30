"""
DIRBS Core Kafka high-level producer module.

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

import ssl
import json

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


class KProducer:
    """Class responsible for producing data to an assigned topic."""

    # TODO: add support for multiple topics

    def __init__(self, config, kafka_host, kafka_port, logger, security_protocol='PLAINTEXT',
                 client_certificate=None, client_key=None, caroot_certificate=None, skip_tls_verifications=False):
        """Constructor.

        Arguments:
            config: DIRBS required config object
            kafka_host: hostname or ip address of the kafka server
            kafka_port: port of the kafka host
            logger: DIRBS logger object
            security_protocol: security protocol for communication, currently only PLAIN & SSL are supported
            client_certificate: client certificate (.pem) file signed by the CA
            client_key: client private key (.pem)
            caroot_certificate: CARoot certificate (.pem) file
            skip_tls_verifications: (True/False), should only be used in dev env, typically for self signed cert error
        """
        self._config = config
        self._host = kafka_host
        self._port = kafka_port
        self._logger = logger
        self._security_protocol = security_protocol
        self._client_cert = client_certificate
        self._client_key = client_key
        self._caroot_cert = caroot_certificate
        self._skip_tls_verification = skip_tls_verifications

    @property
    def kafka_host(self):
        """Property to return the current kafka host address."""
        return self._host

    @property
    def kafka_port(self):
        """Property to return the current kafka port."""
        return self._port

    @property
    def bootstrap_server_addr(self):
        """Property to formulate and return bootstrap server address."""
        return '{0}:{1}'.format(self.kafka_host, self.kafka_port)

    def create_producer(self):
        """Method to create a new producer to a kafka topic."""
        try:
            self._logger.info('Creating new KAFKA producer on host {0}'.format(
                self.bootstrap_server_addr))

            if self._security_protocol == 'PLAINTEXT':
                return KafkaProducer(bootstrap_servers=self.bootstrap_server_addr,
                                     security_protocol=self._security_protocol,
                                     value_serializer=lambda m: json.dumps(m).encode('utf-8'))
            else:
                ssl_ctx = ssl.create_default_context()
                ssl_ctx.options &= ssl.OP_NO_TLSv1
                ssl_ctx.options &= ssl.OP_NO_TLSv1_1

                # this is strictly for dev purposes, where we need the self signed cert to be working
                if self._skip_tls_verification:
                    ssl_ctx.check_hostname = False
                    ssl_ctx.verify_mode = ssl.CERT_NONE

                return KafkaProducer(bootstrap_servers=self.bootstrap_server_addr,
                                     security_protocol=self._security_protocol,
                                     ssl_context=ssl_ctx,
                                     ssl_cafile=self._caroot_cert,
                                     ssl_certfile=self._client_cert,
                                     ssl_keyfile=self._client_key,
                                     value_serializer=lambda m: json.dumps(m).encode('utf-8'))
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
