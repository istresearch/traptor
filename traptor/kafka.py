from abc import ABCMeta, abstractmethod
from confluent_kafka.cimpl import Producer, KafkaError

import json


class KafkaProducerInterface(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def send(self, topic, message, callback=None):
        pass

    @abstractmethod
    def poll(self):
        pass

    @abstractmethod
    def close(self):
        pass


class ConfluentKafkaProducer(KafkaProducerInterface):

    def __init__(self, config, logger):
        super(ConfluentKafkaProducer, self).__init__()

        self.logger = logger

        conf = {
            'bootstrap.servers': ','.join(config['kafka_bootstrap_servers']),
            'broker.version.fallback': config['kafka_broker_version_fallback'],
            'api.version.request': config['kafka_api_version_request'],
            'queue.buffering.max.ms': config['kafka_producer_batch_linger_ms'],
            'queue.buffering.max.kbytes': config['kafka_producer_buffer_kbytes'],
            'message.send.max.retries': 3,
            'default.topic.config': {
                'request.required.acks': 1
            }
        }

        self.logger.info("Creating a Confluent Kafka Producer", {"config": json.dumps(conf, indent=4)})
        self.producer = Producer(dict(conf, **{'error_cb': self.error_callback}), logger=logger.logger)

        # Service any logging
        self.producer.poll(0.25)

    def error_callback(self, error):
        """
        :param error:
        :type error: KafkaError
        :param message:
        :param datum:
        :return:
        """
        if error:
            datum = {}
            datum['success'] = False
            datum['exception'] = error.name()
            datum['description'] = error.str()
            self.logger.error("Kafka error", datum if datum else {})

    def send(self, topic, message, callback=None):

        self.producer.produce(topic, json.dumps(message).encode('utf-8'), callback=callback)

        # Service the delivery callback queue.
        self.producer.poll(0)

    def poll(self):
        self.producer.poll(0)

    def close(self):
        self.producer.flush()
        self.producer.poll(0)