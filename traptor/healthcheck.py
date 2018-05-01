import redis
import settings
import time
import signal
import os
import traceback
import socket

from threading import Thread
from scutils.log_factory import LogFactory
from dog_whistle import dw_config, dw_callback
from pykafka import KafkaClient
from apiclient.discovery import build
from apiclient.errors import HttpError


class HealthCheck:
    running = True
    logger = None

    redis_thread = None
    kafka_thread = None

    def __init__(self):
        self.logger = LogFactory.get_instance(json=os.getenv('LOG_JSON', 'True') == 'True',
                                         name=os.getenv('LOG_NAME', 'traptor-healthcheck'),
                                         stdout=os.getenv('LOG_STDOUT', 'False') == 'True',
                                         level=os.getenv('LOG_LEVEL', 'INFO'),
                                         dir=os.getenv('LOG_DIR', 'logs'),
                                         file=os.getenv('LOG_FILE', 'traptor-healthcheck.log'))

        # Set the default timeout for all sockets
        socket.setdefaulttimeout(int(settings.HEALTHCHECK_TIMEOUT))

        if settings.DW_ENABLED:
            self.logger.debug("Enabling dogwhistle")
            dw_config(settings.DW_SETTINGS)
            self.logger.register_callback('>=INFO', dw_callback)

        signal.signal(signal.SIGINT, self.close)
        signal.signal(signal.SIGTERM, self.close)

    def _check_redis(self):
        redis_client = None

        while self.running:
            try:
                self.logger.info('checking_redis')

                if redis_client is None:
                    redis_client = redis.Redis(host=os.getenv('REDIS_HOST', settings.REDIS_HOST),
                                         port=os.getenv('REDIS_PORT', settings.REDIS_PORT),
                                         db=os.getenv('REDIS_DB', settings.REDIS_DB))

                if redis_client.ping():
                    self.logger.info('redis_ping')
                else:
                    self.logger.error('redis_ping_error')
            except Exception as ex:
                redis_client = None
                self.logger.error('redis_ping_error',
                                  extra={"exception": str(ex),
                                         "ex": traceback.format_exc()})

            time.sleep(int(settings.HEALTHCHECK_REDIS_SLEEP))

    def _check_kafka(self):
        kafka_client = None

        while self.running:
            try:
                self.logger.info('checking_kafka')

                if kafka_client is None:
                    kafka_client = KafkaClient(hosts=os.getenv('KAFKA_HOSTS', settings.KAFKA_HOSTS))
                    topic = kafka_client.topics[os.getenv('KAFKA_TOPIC', settings.KAFKA_TOPIC)]
                else:
                    kafka_client.update_cluster()

                if kafka_client:
                    self.logger.info('kafka_ping')
                else:
                    self.logger.info('kafka_ping_error')

            except Exception as ex:
                #reset kafka so we can try to reopen it!
                kafka_client = None
                self.logger.error('kafka_ping_error',
                                  extra={"exception": str(ex),
                                         "ex": traceback.format_exc()})

            time.sleep(int(settings.HEALTHCHECK_KAFKA_SLEEP))

    def run(self):
        self.logger.debug('Waiting before starting, to let kafka/redis in catch up')
        time.sleep(10)

        if int(settings.HEALTHCHECK_REDIS_SLEEP) > 0:
            self.redis_thread = Thread(target=self._check_redis)
            self.redis_thread.setDaemon(True)
            self.redis_thread.start()

        if int(settings.HEALTHCHECK_KAFKA_SLEEP) > 0:
            self.kafka_thread = Thread(target=self._check_kafka)
            self.kafka_thread.setDaemon(True)
            self.kafka_thread.start()

        heartbeat_period = int(settings.HEARTBEAT_PERIOD)
        if heartbeat_period < 1:
            heartbeat_period = 30

        while self.running:
            self.logger.info('heartbeat')
            time.sleep(heartbeat_period)

    def close(self, signum, frame):
        self.logger.info('closing')
        self.running = False


if __name__ == '__main__':
    HealthCheck().run()
