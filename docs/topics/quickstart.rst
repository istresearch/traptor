Quick Start
===========

Get up and running quickly by running a local **traptor** stream.

Setup
-----

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
#.  Add your ruleset to Redis.  This can be done any number of ways depending on where you are keeping your rules.  In the in the ``scripts/rule-extract.py`` file there are examples of how to extract rules from a GNIP ruleset file and a MySQL database.  You may wish to add a custom function to parse out rules from other sources.

.. important:: Be sure to insert your rules as a ``set`` redis data type with the key format of ``traptor-<type>:<id>``.

Congratulations.  You are all set to run **traptor**!

Running Traptor
---------------

To start, run it *without kafka* by running ``python traptor --test`` from the command line.  The ``--test`` flag tells **traptor** to skip sending data to kafka and just print debug output at the **INFO** level.  This will print out only the tweet text field.

Once that is working successfully, try writing your data to kafka by running ``python traptor``.  You can tail the Kafka output by running the following command in your Kafka installation directory::

    bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic traptor

.. hint:: Check out `kafkacat <https://github.com/edenhill/kafkacat>`_  for a handy kafka debugging tool.
