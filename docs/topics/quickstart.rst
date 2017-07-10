Quick Start
===========

When first starting out with **traptor**, it is recommended that you test on your local machine to get a feel for how it works.  To get a local **traptor** running, you will need to at minimum have:

- Python 2.7.x
- Redis
- Kafka (optional for testing)
  
You have the option of setting Redis and/or Kafka on your local machine or server, or using a pre-built vagrant machine for testing.  I recommend using the Vagrant machine if you are testing on your local machine.  If you are on a remote server somewhere (such as EC2), you will need to set up Redis and Kafka on that instance or somewhere else.
  
Traptor Test Environment
------------------------

To set up a pre-canned Traptor test environment, make sure you have the latest Virtualbox + Vagrant >= 1.7.4 installed. Vagrant will automatically mount the base traptor directory to the /vagrant directory, so any code changes you make will be visible inside the VM.

Steps to launch the Vagrant VM:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#.  ``git clone git@github.com:istresearch/traptor`` to pull down the latest code.
#.  ``cd traptor``
#.  ``vagrant up`` in base **traptor** directory.
#.  ``vagrant ssh`` to ssh into the VM.
#.  ``sudo su`` to change to root user.
#.  ``supervisorctl status`` to check that everything is running.
#.  ``cd /vagrant`` to get to the **traptor** directory.

    
Now you will have all your dependencies installed and will be running Redis and Kafka on the default ports inside the VM.

Configuring Traptor
-------------------

#.  ``pip install -r requirements.txt`` to install Traptor dependencies.
#.  ``cd traptor`` to get inside the module folder.
#.  ``cp settings.py localsettings.py`` to create your ``localsettings.py`` file.
#.  Remove the "Local Overrides" section from the ``localsettings.py`` file.
#.  Fill in the ``APIKEYS`` and  ``TRAPTOR_TYPE`` fields.
#.  Optionally update the kafka and redis connection information if you are not running locally.
#.  Optionally add a Redis pubsub channel if you are using pubsub to automatically refresh the rules Traptor uses.
#.  Add your ruleset to Redis.  This can be done any number of ways depending on where you are keeping your rules.  In the in the ``scripts/rule-extract.py`` file there are examples of how to extract rules from a GNIP ruleset file and a MySQL database.  You may wish to add a custom function to parse out rules from other sources.

.. important:: Be sure to insert each rule as a ``hashmap`` data type with the key format of ``traptor-<traptor_type>:<traptor_id>:<rule_id>``.

Congratulations.  You are all set to run **traptor**!


Running Traptor
---------------

To start, run it *without kafka* by running ``python traptor.py --test`` from the command line.  The ``--test`` flag tells **traptor** to skip sending data to kafka and just print to stdout.  You can pipe the output into jq (https://stedolan.github.io/jq/) like this ``python traptor.py --test | jq .`` to get a nicely colored JSON output.

**traptor** also accepts a ``--loglevel=info`` or ``--loglevel=debug`` argument if you wish to print out logging information.

Once that is working successfully, try writing your data to kafka by running ``python traptor.py``.  You can tail the Kafka output by running the following command in your Kafka installation directory::

    bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic traptor

.. tip:: Check out `kafkacat <https://github.com/edenhill/kafkacat>`_  for a handy kafka debugging tool.
