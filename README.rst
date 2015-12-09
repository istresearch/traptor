*************************************
Traptor -- A distributed Twitter feed
*************************************

**traptor** is a framework to help manage your twitter data collection.  What differentiates **traptor** from the many other Twitter libraries out there is that it does *real-time distributed streaming* of data based on rule sets using the Twitter Streaming API.

It uses a combination of `Kafka`_, `Redis`_, and the excellent `birdy`_ module.  The goal is to have a convenient way to aggregate all of your twitter application data into one data stream and (optionally) a database.  It uses birdy to make Twitter API connections, redis to handle the rule management among different traptor instances, and kafka to handle the data streams.

Please see http://traptor.readthedocs.com for documentation and the Quick Start guide.

.. _Kafka: http://http://kafka.apache.org/
.. _Redis: http://redis.io
.. _birdy: https://github.com/inueni/birdy