#!/usr/bin/env python
# encoding: utf-8
import redis

location_rules = [{
    "status":"active",
    "rule_type":"locations",
    "orig_type":"geo",
    "description":"Tweets from CONUS",
    "project_version_id":"None",
    "campaign_id":"default",
    "value":"-125.551758,24.726875,-66.401367,49.439557",
    "tag":"Pulse.united_states",
    "appid":"ist-dev",
    "date_added":"2021-01-19 18:37:28",
    "project_id":"default",
    "rule_id":"1",
    "node_id":"None"
}]

track_rules = [
    {
        "status":"active",
        "rule_type":"track",
        "orig_type":"keyword",
        "description":"test",
        "project_version_id":"None",
        "campaign_id":"default",
        "value":"usa",
        "tag":"Pulse.united_states",
        "appid":"ist-dev",
        "date_added":"2021-01-19 18:37:28",
        "project_id":"default",
        "rule_id":"10",
        "node_id":"None"
    },
    {
        "status":"active",
        "rule_type":"track",
        "orig_type":"keyword",
        "description":"news",
        "project_version_id":"None",
        "campaign_id":"default",
        "value":"news",
        "tag":"noise",
        "appid":"ist-dev",
        "date_added":"2021-01-19 18:37:28",
        "project_id":"default",
        "rule_id":"11",
        "node_id":"None"
    }
]

follow_rules = [{
    "status":"active",
    "rule_type":"follow",
    "orig_type":"username",
    "description":"news",
    "project_version_id":"None",
    "campaign_id":"default",
    "value":"cnn",
    "tag":"news",
    "appid":"ist-dev",
    "date_added":"2021-01-19 18:37:28",
    "project_id":"default",
    "rule_id":"20",
    "node_id":"None"
}]

r = redis.Redis('localhost', db=1)
r.info()

traptor_id = 0

for item in location_rules:
    r.hmset('traptor-locations:{}:{}'.format(traptor_id, item['rule_id']), item)

for item in track_rules:
    r.hmset('traptor-track:{}:{}'.format(traptor_id, item['rule_id']), item)

for item in follow_rules:
    r.hmset('traptor-follow:{}:{}'.format(traptor_id, item['rule_id']), item)
