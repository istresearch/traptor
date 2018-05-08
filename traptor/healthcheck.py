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

from birdy.twitter import StreamClient


class HealthCheck:
    running = True
    logger = None

    redis_thread = None
    kafka_thread = None
    twitter_thread = None

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
            default_tags = {
                "tags": [
                    "traptor_type:{}".format(os.getenv('TRAPTOR_TYPE')),
                    "traptor_id:{}".format(os.getenv('TRAPTOR_ID'))
                ]
            }
            dw_settings = dict(settings.DW_CONFIG.items() + default_tags.items())
            dw_config(dw_settings)
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
                    status = {'int': 1, 'string': 'success' }
                    self.logger.info('redis_ping_status', extra={"status": status})
                else:
                    status = {'int': 0, 'string': 'failure' }
                    self.logger.error('redis_ping_status', extra={"status": status})
            except Exception as ex:
                redis_client = None
                status = {'int': 0, 'string': 'failure' }
                self.logger.error('redis_ping_status', extra={
                    "exception": str(ex),
                     "ex": traceback.format_exc(),
                     "status": status
                 })

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
                    status = {'int': 1, 'string': 'success' }
                    self.logger.info('kafka_ping_status', extra={"status": status})
                else:
                    status = {'int': 0, 'string': 'failure' }
                    self.logger.error('kafka_ping_status', extra={"status": status})

            except Exception as ex:
                #reset kafka so we can try to reopen it!
                kafka_client = None
                status = {'int': 0, 'string': 'failure' }
                self.logger.error('kafka_ping_status', extra={
                    "exception": str(ex),
                     "ex": traceback.format_exc(),
                     "status": status
                 })

            time.sleep(int(settings.HEALTHCHECK_KAFKA_SLEEP))

    def _check_twitter(self):
        twitter_stream = None

        while self.running:
            try:
                self.logger.info('checking_twitter')

                if twitter_stream is None:
		    twitter_client = StreamClient(
                        os.getenv('CONSUMER_KEY'),
                        os.getenv('CONSUMER_SECRET'),
                        os.getenv('ACCESS_TOKEN'),
                        os.getenv('ACCESS_TOKEN_SECRET')
                    )
                    twitter_resource = twitter_client.stream.statuses.filter.post(track='twitter')
                    twitter_stream = twitter_resource.stream()

                tweet_data = next(twitter_stream)
                if tweet_data:
                    status = {'int': 1, 'string': 'success' }
                    self.logger.info('twitter_ping_status', extra={"status": status})
                else:
                    status = {'int': 0, 'string': 'failure' }
                    self.logger.error('twitter_ping_status', extra={"status": status})

            except Exception as ex:
                #reset twitter so we can try to reopen it!
                twitter_stream = None
                status = {'int': 0, 'string': 'failure' }
                self.logger.error('twitter_ping_status', extra={
                    "exception": str(ex),
                     "ex": traceback.format_exc(),
                     "status": status
                 })

            time.sleep(int(settings.HEALTHCHECK_TWITTER_SLEEP))

    def run(self):
        self.logger.info('Traptor healthcheck start up')
        self.logger.debug('Waiting before starting, to let kafka/redis in catch up')
        time.sleep(10)

        if int(settings.HEALTHCHECK_REDIS_SLEEP) > 0:
            self.logger.info('Traptor Redis healthcheck started')
            self.redis_thread = Thread(target=self._check_redis)
            self.redis_thread.setDaemon(True)
            self.redis_thread.start()

        if int(settings.HEALTHCHECK_KAFKA_SLEEP) > 0:
            self.logger.info('Traptor Kafka healthcheck started')
            self.kafka_thread = Thread(target=self._check_kafka)
            self.kafka_thread.setDaemon(True)
            self.kafka_thread.start()

        if int(settings.HEALTHCHECK_TWITTER_SLEEP) > 0:
            self.logger.info('Traptor Twitter healthcheck started')
            self.twitter_thread = Thread(target=self._check_twitter)
            self.twitter_thread.setDaemon(True)
            self.twitter_thread.start()

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
