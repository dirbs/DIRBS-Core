"""
KAFKA unit tests.

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

import pytest
from kafka.errors import NoBrokersAvailable

from dirbs.kafka.producer import KProducer
from dirbs.kafka.consumer import KConsumer
from _fixtures import mocked_config, logger  # noqa: F401


def test_props(mocked_config, logger):  # noqa: F811
    """Verify that a consumer instance have its own several properties and returns."""
    host = 'abc_host'
    port = 23345
    topic = 'abc_topic'

    # verifies consumer properties
    consumer = KConsumer(config=mocked_config, kafka_host=host, kafka_port=port,
                         kafka_topic=topic, logger=logger)
    assert consumer.kafka_host == host
    assert consumer.kafka_port == port
    assert consumer.kafka_topic == topic
    assert consumer.bootstrap_server_addr == '{0}:{1}'.format(host, port)

    # verifies producer properties
    producer = KProducer(config=mocked_config, kafka_host=host, kafka_port=port, logger=logger)
    assert producer.kafka_host == host
    assert producer.kafka_port == port
    assert producer.bootstrap_server_addr == '{0}:{1}'.format(host, port)


def test_no_broker_available(mocked_config, logger):  # noqa: F811
    """Verifies that object throws proper exception when broker is not available."""
    # verify consumer
    with pytest.raises(NoBrokersAvailable):
        consumer = KConsumer(config=mocked_config, kafka_host='localhost', kafka_port=9092,
                             kafka_topic='dirbs', logger=logger)
        assert consumer.create_consumer()

    # verify producer
    with pytest.raises(NoBrokersAvailable):
        producer = KProducer(config=mocked_config, kafka_host='localhost', kafka_port=9092, logger=logger)
        assert producer.create_producer()
