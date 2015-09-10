#!/usr/bin/env python

import sys
import json
import re
import redis
import pymysql
import logging

from settings import mysql_settings, redis_settings

logging.basicConfig(level=logging.DEBUG)

def find_unqiue_ids():
    def wrapper(values):
        rules = []
        for v in values:
            ids = re.findall(r'from:(\d+)', v)
            for x in ids:
                rules.append(x)
        return rules
    return wrapper


def parse_gnip_rules():
    """ Parse gnip prod-darpa rules, return tweet ids"""
    rules = []
    for fname in sys.argv[1:]:
        with open(fname) as f:
            jreq = json.load(f)
        for i in jreq:
            for j in jreq['rules']:
                ids = re.findall(r'from:(\d+)', j['value'])
                for x in ids:
                    rules.append(x)
    # Get rid of the dupicates
    rules = set(rules)

    return rules


def parse_ctd_rules(traptor_type,
                    host=mysql_settings['HOST'],
                    port=mysql_settings['PORT'],
                    user=mysql_settings['USER'],
                    passwd=mysql_settings['PASSWD'],
                    db=mysql_settings['DB'],
                    ):
    """ Parse CTD prod-darpa rules"""

    conn = pymysql.connect(
                           host=host,
                           port=port,
                           user=user,
                           passwd=passwd,
                           db=db
                           )
    cursor = conn.cursor()
    if traptor_type == 'follow':
        query = "select value from rules where type = 'username' and rule_set = 'prod-darpa'"
        cursor.execute(query)
        values = [row[0] for row in cursor]
        rules = []
        for v in values:
            ids = re.findall(r'from:(\d+)', v)
            for x in ids:
                rules.append(x)
    elif traptor_type == 'track':
        query = "select value from rules where type = 'keyword' and rule_set = 'prod-darpa'"
        cursor.execute(query)
        values = [row[0] for row in cursor]
        rules = []
        for v in values:
            if re.match(r'url_contains', v):
                # Make compatible with Twitter API
                # https://dev.twitter.com/streaming/overview/request-parameters
                v = re.sub(r'url_contains:\s?"?([\w\.]+)"?', r'\1', v)
                v = re.sub(r'\.', ' ', v)
            rules.append(v)
    else:
        pass

    # Get rid of the dupicates
    rules = set(rules)

    return rules


def send_to_redis(traptor_type,
                  rules,
                  host=redis_settings['HOST'],
                  port=redis_settings['PORT'],
                  db=redis_settings['DB']
                  ):
    """ Send rules to Redis"""

    # Set up API limitation checks
    if traptor_type == 'follow':
        rule_max = 5000
    elif traptor_type == 'track':
        rule_max = 400
    else:
        sys.exit('That rule type is not supported')

    r = redis.StrictRedis(host=host, port=port, db=db)

    for idx, i in enumerate(rules):
        crawler_num = idx / rule_max
        print idx, crawler_num
        r.sadd('traptor-{0}:{1}'.format(traptor_type, crawler_num), i.encode('utf-8'))


if __name__ == '__main__':

    the_rules = parse_ctd_rules('track')
    print the_rules
    send_to_redis('track', the_rules, host='localhost')
