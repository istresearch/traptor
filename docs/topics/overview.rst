Overview
==========

Dependencies
------------

Components required to run a **traptor** cluster:

- Python 2.7: https://www.python.org/downloads/
- Redis: http://redis.io/
- Zookeeper: https://zookeeper.apache.org/
- Kafka: http://kafka.apache.org/
  
Please see ``requirements.txt`` for pip package dependencies.

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