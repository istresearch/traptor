"""
Traptor integration tests.

This test file is used to ensure that a Traptor is able to pull rules from Redis and begin collection.

Requirements:
* Docker
* Python 2.7 environment with Redis installed

To run this test do the following:

* Create and activate a virtual environment (venv or Anaconda)
* Rename traptor.env.example to traptor.env and fill in all fields. Some defaults are provided.
* Ensure that the Redis database you're using in this file matches the docker-compose.yml file (should be 2)
* Install the requirements: `pip install -r requirements.txt`
* Start Traptor and Redis: `docker-compose up --build -d`
* Tail the logs: `tail -f logs/traptor.log
* Run the integration test file: `python tests/traptor_integration_tests.py`

If everything is working you should see Traptor get it's rules from Redis and begin processing tweets.

If you'd like to see more of how the sausage is made, change the log level from `INFO` to `DEBUG`.

"""

import os
from redis import StrictRedis, ConnectionError
from time import sleep

HOST_FOR_TESTING = os.getenv('REDIS_HOST', 'localhost')
TRAPTOR_TYPE = os.getenv('TRAPTOR_TYPE', 'track')
TRAPTOR_ID = int(os.getenv('TRAPTOR_ID', 0))

# Create a connection to Redis

redis_connection = None

try:
    redis_connection = StrictRedis(host=HOST_FOR_TESTING, port=6379, db=2)
except ConnectionError as ce:
    print("Unable to connect to {}. Error: {}".format(HOST_FOR_TESTING, ce))

# If we have a connection to Redis, wait 15 seconds and add a rule
if redis_connection is not None:
    print("Giving you 15 seconds so you can start tailing the logs")

    sleep(15)

    # Collect on tweets with the keyword `python`
    # Change the rule value if you want to collect on a different keyword or hashtag. Go nuts!
    test_track_rule = {
      "tag": "Traptor.Test",
      "type": "track",
      "value": "python",
      "description": "Test track rule for python"
    }

    try:
        redis_key = "traptor-{}:{}:{}".format(TRAPTOR_TYPE, TRAPTOR_ID, 123)
        redis_connection.hmset(redis_key, test_track_rule)

        print("Rule added")
        print("Redis Key: {}".format(redis_key))
        print("Rule: {}".format(test_track_rule))
    except ConnectionError as ce:
        print("Unable to add rule to Redis: {}".format(ce))
