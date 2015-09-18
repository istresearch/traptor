############################################
Traptor -- A distributed Twitter feed
############################################

The genesis for the name **traptor** comes from the combination of the words *twitter* and *raptor*.

***************
Getting Started
***************

Overview
========

**traptor** is a framework to help manage your twitter data collection.  It uses a combination of `Kafka`_, `Redis`_, and the excellent `birdy`_ module.  The goal is to have a convenient way to aggregate all of your twitter application data into one data stream and (optionally) a database.  It uses birdy to make Twitter API connections, redis to handle the rule management among different traptor instances, and kafka to handle the data streams.

.. _Kafka: http://http://kafka.apache.org/
.. _Redis: http://redis.io
.. _birdy: https://github.com/inueni/birdy

Dependencies
------------

Please see ``requirements.txt`` for pip package dependencies.

Components required to run a **traptor** cluster:

- Python 2.7: https://www.python.org/downloads/
- Redis: http://redis.io/
- Zookeeper: https://zookeeper.apache.org/
- Kafka: http://kafka.apache.org/

Optional Dependencies
---------------------

Depending on your implementation, you may want to consider also using these tools:

- Ansible: http://www.ansible.com/
- MySQL: https://www.mysql.com/
- ELK Stack: https://www.elastic.co/

Server provisioning
"""""""""""""""""""

To provision your servers, it is helpful to use one of the many provisioning tools available, such as Ansible, Fabric, Chef, or Salt.  Ansible and Fabric are both Python based and both work well -- Fabric is preferred for simple deployments, Ansible for complex ones.

Data management
"""""""""""""""

To manage your Twitter rule set, you may want to use a relational database system and parse out rules appropriately to your Redis database, which only stores key/value pairs of rules for each **traptor** type such as *track*, *follow* or *geo*.  For storing the collected dataset, you may want to use one of many available NoSQL open source databases.  Common choices are ElasticSearch (Lucene) and MongoDB.

In a currently deployed application, I am using a MySQL database for "user facing" rules, and the ELK stack (ElasticSeach, Logstash, Kibana) to do the data aggregation and visualization.  A production level deployment data flow might look like this:

MySQL --> Redis --> Traptor --> Kafka --> Logstash --> Elasticsearch --> Kibana

Quick Start
-----------

When first starting out with **traptor**, it is recommended that you test on your local machine to get a feel for how it works.  To get a local **traptor** running, you will need to at minimum have:

- Python 2.7.x
- A local Redis server running

  
To set up a local redis server, check out the quickstart guide at http://redis.io/topics/quickstart.  Start up the redis server by running ``redis-server`` in a tmux session or open terminal window.
  
Optionally, if you wish to run send the data though Kafka (like production), you'll need:

- A local zookeeper node running
- A local kafka broker running
  
To set up a local zookeeper and kafka, check out the quickstart guide at http://kafka.apache.org/documentation.html#quickstart.  You can stop after "Start the Server" unless you want to mess around with Kafka a bit more.

OK, now we've gotten that out of the way, time to install **traptor**!

.. tip:: It is recommended that you install traptor inside a virtual environment for testing.

#.  To pull the latest code, clone the repo via ``git clone git@github.com:istresearch/traptor``
#.  Inside your virtual environment, run ``pip install -r requirements.txt`` to install pip dependencies.
#.  Inside the base repo directory, ``cd traptor`` and then ``cp settings.py localsettings.py`` to create your ``localsettings.py`` file.  Remove the "Local Overrides" section from the ``localsettings.py`` file.
#.  Add your local settings to the file.  If you are running redis and/or kafka on the default ports, all you need to change is your Twitter API keys and the ``TRAPTOR_TYPE``.
#.  Add your ruleset to Redis.  This can be done any number of ways depending on where you are keeping your rules.  In the in the ``scripts/rule-extract.py`` file there are examples of how to extract rules from a GNIP ruleset file, and a MySQL database.  You may wish to add a custom function to parse out rules from other sources.

.. important:: Be sure to insert your rules as a ``set`` redis data type with the key format of ``traptor-<type>:<id>``.

Congratulations.  You are all set to run **traptor**!

Running Traptor
"""""""""""""""

To start, run it *without kafka* by running ``python traptor --test`` from the command line.  The ``--test`` flag tells **traptor** to skip sending data to kafka and just print debug output at the **INFO** level.  This will print out only the tweet text field.

Once that is working successfully, try writing your data to kafka by running ``python traptor``.  You can tail the Kafka output by running ``bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic traptor`` in your Kafka installation directory.

.. hint:: Check out `kafkacat <https://github.com/edenhill/kafkacat>`_  for a handy kafka debugging tool.

#######################
Deploying to production
#######################

Coming soon...
