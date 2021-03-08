"""
DIRBS CLI for Whitelisting Process. Installed by setuptools as a dirbs-classify console script.

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

import re
import sys
import json
import select
import logging

import click
from psycopg2 import sql

import dirbs.cli.common as common
import dirbs.metadata as metadata
from dirbs.kafka import consumer, producer


def create_kafka_consumer(logger, config):
    """Method to create a single high level KAFKA Consumer.

    Arguments:
        logger: DIRBS logger object
        config: DIRBS config object
    """
    kafka_config = config.broker_config.kafka
    logger.info("Creating a single high level consumer on topic \'{0}\'".format(kafka_config.topic))

    if kafka_config.security_protocol == 'PLAINTEXT':
        cons = consumer.KConsumer(config=config,
                                  kafka_host=kafka_config.hostname,
                                  kafka_port=kafka_config.port,
                                  kafka_topic=kafka_config.topic,
                                  logger=logger)
    else:
        cons = consumer.KConsumer(config=config,
                                  kafka_host=kafka_config.hostname,
                                  kafka_port=kafka_config.port,
                                  kafka_topic=kafka_config.topic,
                                  logger=logger,
                                  security_protocol=kafka_config.security_protocol,
                                  client_certificate=kafka_config.client_certificate,
                                  client_key=kafka_config.client_key,
                                  caroot_certificate=kafka_config.caroot_certificate,
                                  skip_tls_verifications=kafka_config.skip_tls_verifications)
    return cons.create_consumer()


def create_kafka_producer(logger, config):
    """Method to create a high level producer.

    Arguments:
        logger: DIRBS logger object
        config: DIRBS config object
    """
    kafka_config = config.broker_config.kafka
    logger.info("Creating a high level producer on KAFKA host \'{0}\'".format(kafka_config.hostname))

    if kafka_config.security_protocol == 'PLAINTEXT':
        prod = producer.KProducer(config=config,
                                  kafka_host=kafka_config.hostname,
                                  kafka_port=kafka_config.port,
                                  logger=logger)
    else:
        prod = producer.KProducer(config=config,
                                  kafka_host=kafka_config.hostname,
                                  kafka_port=kafka_config.port,
                                  logger=logger,
                                  security_protocol=kafka_config.security_protocol,
                                  client_certificate=kafka_config.client_certificate,
                                  client_key=kafka_config.client_key,
                                  caroot_certificate=kafka_config.caroot_certificate,
                                  skip_tls_verifications=kafka_config.skip_tls_verifications)
    return prod.create_producer()


def broadcast_notification(imei_norm, operator_id, producer, broadcast_type, operator_config, logger):
    """Broadcast IMEI association message to operators other than the operator_id.

    Arguments:
        imei_norm: normalized IMEI to notify about
        operator_id: ID of operator not to be notified
        producer: kafka producer object
        broadcast_type: type of the broadcast [association, de-association]
        operator_config: kafka operator config object
        logger: DIRBS logger object
    """
    if broadcast_type == 'association':
        message = {
            'type': 'imei_association_notification',
            'imei': imei_norm,
            'message': '{0} has been associated, is not available anymore'.format(imei_norm)
        }
    else:
        message = {
            'type': 'imei_de_association_notification',
            'imei': imei_norm,
            'message': '{0} has been de_associated, is available now'.format(imei_norm)
        }

    # now producing message for each operator on their respective topics
    for op in operator_config:
        if op.id != operator_id:
            logger.debug("Dispatching {0} notification to {1} on topic \'{2}\'"
                         .format(broadcast_type, operator_id, op.topic))
            producer.send(op.topic, message)


def calc_imei_norm(imei):
    """Method to validate and calculate normalized IMEI."""
    if imei is not None and len(imei) <= 16:
        if re.match(r'^\d{14}', imei):
            imei_norm = imei[:14]
        else:
            imei_norm = imei.upper()
        return imei_norm
    return None


def validate_operator(operator_id, operator_config):
    """Method to validate operator_id recevd against defined operators in config."""
    operator_ids = [op.id for op in operator_config]
    return True if operator_id in operator_ids else False


def whitelist_processing_job(consumer, producer, operator_config, conn, logger):
    """Method to perform processing on historic_whitelist table.

    :param consumer consumes messages from the main topic, process them according to their type
    and send back notifications using broadcast_notifications method and :param producer.
    """
    # consuming messages one by one, currently we are only supporting one consumer
    # but we can support multiple consumers as well to overcome the potential delay created
    # by this portion of the code. I am adding a TODO NOTE here for later tracking.
    # here is an active trello card link: https://trello.com/c/woJKXIJQ
    for msg in consumer:
        msg_type = msg.value.get('type', '')

        if msg_type == 'imei_association':
            logger.debug('Consumed a new message: {0}'.format(msg.value))
            logger.debug('Processing IMEI Association request...')
            imei_norm = calc_imei_norm(msg.value.get('imei'))
            operator_id = msg.value.get('operator_id')

            if imei_norm is not None and validate_operator(operator_id, operator_config):
                with conn, conn.cursor() as cursor:
                    cursor.execute(sql.SQL("""SELECT 1
                                                FROM available_whitelist
                                               WHERE imei_norm = %(imei_norm)s
                                                 AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)"""),
                                   {'imei_norm': imei_norm})
                    res = cursor.fetchone()
                    if res is not None:
                        cursor.execute(sql.SQL("""UPDATE historic_whitelist
                                                     SET associated = TRUE, eir_id = %(eir_id)s
                                                   WHERE imei_norm = %(imei_norm)s
                                                     AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)"""),
                                       {'eir_id': operator_id, 'imei_norm': imei_norm})
                        logger.debug("Broadcasting Association notifications for IMEI \'{0}\'...".format(imei_norm))
                        broadcast_notification(imei_norm, operator_id, producer, 'association',
                                               operator_config, logger)
                    else:
                        logger.debug('IMEI {0} is already associated or not in whitelist, request ignored.'
                                     .format(imei_norm))
                        pass
            else:
                logger.debug('IMEI is not in correct format or invalid operator, request ignored.')
        elif msg_type == 'imei_de_association':
            logger.debug('Consumed a new message: {0}'.format(msg.value))
            logger.debug('Processing IMEI De-Association request...')
            imei_norm = calc_imei_norm(msg.value.get('imei'))
            operator_id = msg.value.get('operator_id')

            if imei_norm is not None and validate_operator(operator_id, operator_config):
                with conn, conn.cursor() as cursor:
                    cursor.execute(sql.SQL("""SELECT 1
                                                FROM whitelist
                                               WHERE imei_norm = %(imei_norm)s
                                                 AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)
                                                 AND associated IS TRUE
                                                 AND eir_id = %(operator_id)s"""),
                                   {'imei_norm': imei_norm, 'operator_id': operator_id})
                    res = cursor.fetchone()
                    if res is not None:
                        cursor.execute(sql.SQL("""UPDATE historic_whitelist
                                                     SET associated = FALSE, eir_id = NULL
                                                   WHERE imei_norm = %(imei_norm)s
                                                     AND virt_imei_shard = calc_virt_imei_shard(%(imei_norm)s)
                                                     AND eir_id = %(operator_id)s"""),
                                       {'imei_norm': imei_norm, 'operator_id': operator_id})
                        logger.debug("Broadcasting De-Association notification for IMEI \'{0}\'".format(imei_norm))
                        broadcast_notification(imei_norm, operator_id, producer,
                                               broadcast_type='de_association', operator_config=operator_config,
                                               logger=logger)
                    else:
                        logger.debug("IMEI \'{0}\' is not associated, request ignored.".format(imei_norm))
            else:
                logger.debug('IMEI is not in correct format or invalid operator, request ignored.')
        else:
            logger.debug('Unrecognised request type, request ignored.')


def whitelist_sharing_job(h_producer, operator_config, conn, logger):
    """Whitelist distribution job method.

    This method listens to a specific database notifications events which are generated when the
    historic_whitelist table is update or inserted with records. It than transmit those changes to operators.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute('LISTEN distributor_updates')
            time_passed = 0
            while 1:
                conn.commit()
                if select.select([conn], [], [], 5) == ([], [], []):
                    time_passed += 5
                    logger.debug('Listening to notification still after {0} seconds...'.format(time_passed))
                else:
                    time_passed = 0
                    imei_adds = []
                    imei_updates = []
                    imei_deletes = []

                    update_msg = {
                        'type': 'whitelist_update',
                        'content': {
                            'adds': imei_adds,
                            'updates': imei_updates,
                            'deletes': imei_deletes
                        }
                    }

                    conn.poll()
                    conn.commit()
                    while conn.notifies:
                        notification = conn.notifies.pop()
                        logger.debug('Notification: {0}, {1}, {2}'
                                     .format(notification.pid, notification.channel, notification.payload))
                        payload = json.loads(notification.payload)
                        imei_norm = payload.get('imei_norm')
                        imei_adds.append(imei_norm) if payload.get('end_date') is None \
                            else imei_deletes.append(imei_norm)

                    logger.debug('Dispatching whitelist to each operator update...')
                    logger.debug(update_msg)
                    for op in operator_config:
                        h_producer.send(op.topic, update_msg)
    except Exception as e:
        logger.info('DIRBS encountered an exception during whitelist distribution job. See below for details')
        logger.error(str(e))
        sys.exit(1)


@click.group(no_args_is_help=False)
@common.setup_initial_logging
@click.version_option()
@common.parse_verbosity_option
@click.pass_context
@common.configure_logging
def cli(ctx):
    """DIRBS Script to run whitelist jobs."""
    logger = logging.getLogger('dirbs.whitelist')
    config = common.ensure_config(ctx)

    # check if whitelist mode is enabled before running any command
    if config.operational_config.activate_whitelist is False:
        logger.info('Whitelist operation mode is currently not enabled, exiting...')
        sys.exit(1)


@cli.command()  # noqa: C901
@click.pass_context
@common.unhandled_exception_handler
@common.cli_wrapper(command='dirbs-whitelist', subcommand='process', required_role='dirbs_core_white_list')
def process(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root):
    """Start whitelist processing job."""
    logger.info('Initiating Whitelist processing job...')

    operator_config = config.broker_config.operators
    kafka_config = config.broker_config.kafka
    h_consumer = create_kafka_consumer(logger, config)
    h_producer = create_kafka_producer(logger, config)

    # Store metadata
    metadata.add_optional_job_metadata(metadata_conn, command, run_id,
                                       kafka={'host': kafka_config.hostname, 'port': kafka_config.port,
                                              'topic': kafka_config.topic},
                                       operators=[{'operator': op.id, 'topic': op.topic} for op in operator_config])

    whitelist_processing_job(consumer=h_consumer, producer=h_producer,
                             operator_config=operator_config, conn=conn, logger=logger)


@cli.command()  # noqa: C901
@click.pass_context
@common.unhandled_exception_handler
@common.cli_wrapper(command='dirbs-whitelist', subcommand='distribute', required_role='dirbs_core_white_list')
def distribute(ctx, config, statsd, logger, run_id, conn, metadata_conn, command, metrics_root, metrics_run_root):
    """Start whitelist distribution job."""
    logger.info('Initialising Whitelist distributor job...')
    operator_config = config.broker_config.operators
    h_producer = create_kafka_producer(logger, config)

    # check if whitelist is not restricted before going for operation
    if config.operational_config.restrict_whitelist is False:
        logger.info('Whitelist distributor job initialised.')
        whitelist_sharing_job(h_producer, operator_config, conn, logger)
    else:
        logger.info('Whitelist sharing with operators is restricted in config.yml file, exiting...')
        sys.exit(1)
