Production
==========

Running a collection of distributed Traptor streams.

Planning
--------

To run **traptor** in a distributed environment, you'll need to figure out approximately what your collection needs are.  The Twitter API offers different limits for different types of rules.  As of this writing the following API limits are in place for the Public Streaming API.

- follow: 5000 rules
- track: 400 rules
- location: 25 rules
  
This means that you are *limited by how many rules you can add per traptor application*.  For example, if you have 5,500 "follow" rules and 352 "track" rules, you will need 3 **traptor** connections (2 for "follow", 1 for "track").  These should be different API keys with different connection IP addresses.

Ansible
-------

To handle a distributed deployment, you can use Ansible.  Ansible lets you dynamically configure inventories based on roles to do semi-automated deployments.  

Inventory
"""""""""

Using the example from above, my Ansible inventory may look something like this::

    [traptor-follow-nodes]
    server01
    server02

    [traptor-track-nodes]
    server03

    traptor-location-nodes]
    server04
    server05

    [traptor-nodes:children]
    traptor-follow-nodes
    traptor-track-nodes
    traptor-location-nodes

Group_vars
""""""""""

The best way to manage a pool of API keys is in a ``traptor-nodes`` groups_vars file.  Since both ``traptor-track-nodes`` and ``traptor-follow-nodes`` are children of ``traptor-nodes``, the API keys can be either by *any* traptor type.  Continuing with the example above, the file might look like this::

    ---

    traptor_kafka_topic: 'my_traptor'

    apikeys:
      - consumer_key: 'YOUR_INFO'
        consumer_secret: 'YOUR_INFO'
        access_token: 'YOUR_INFO'
        access_token_secret: 'YOUR_INFO'
      - consumer_key: 'YOUR_INFO'
        consumer_secret: 'YOUR_INFO'
        access_token: 'YOUR_INFO'
        access_token_secret: 'YOUR_INFO'
      - consumer_key: 'YOUR_INFO'
        consumer_secret: 'YOUR_INFO'
        access_token: 'YOUR_INFO'
        access_token_secret: 'YOUR_INFO'

The ``traptor_kafka_topic`` is links to the traptor ``localsettings`` template to override the default ``traptor`` topic name with one of your choosing.  The ``apikeys`` dictionary contains 3 sets of API connection info, one for each traptor node.

Tasks
"""""

Coming soon... how to set up Ansible tasks (link to sample code)


Redis PubSub for Automatic Rule Refresh
---------------------------------------

When your Twitter rule set changes, the Traptor to which rules have been either added or deleted can be automatically restarted. While running, Traptor continuously checks a Redis pubsub channel for a message for itself, in the following format:

<traptor-type>:<traptor-id>

An example message is:

track:0

In order to use this functionality, add a message as formatted above to the Redis pubsub channel for each Traptor for which the rules changed.