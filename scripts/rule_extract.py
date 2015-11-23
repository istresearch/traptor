#!/usr/bin/env python

import sys
import json
import re
import redis
import pymysql
import logging

from settings import mysql_settings, redis_settings

logging.basicConfig(level=logging.INFO)


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
    """ Parse gnip ***REMOVED*** rules, return tweet ids"""
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


class CooperRules(object):
    """
    Class to handle a Cooper MySQL connection and parse out rules in a
    traptor friendly format
    """
    def __init__(self,
                 host=mysql_settings['HOST'],
                 port=mysql_settings['PORT'],
                 user=mysql_settings['USER'],
                 passwd=mysql_settings['PASSWD'],
                 db=mysql_settings['DB']
                 ):

        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db

    def connect(self):
        self.conn = pymysql.connect(host=self.host,
                                    port=self.port,
                                    user=self.user,
                                    passwd=self.passwd,
                                    db=self.db
                                    )
        self.cursor = self.conn.cursor()

    def parse_ctd_rules(self, traptor_type):
        """
            Parse CTD ***REMOVED*** rules.  Returns a list of dictionaries that
            contain {tag:, value:} pairs.
        """
        if traptor_type == 'follow':
            query = "select tag, value from rules where type = 'username' and rule_set = '***REMOVED***'"
        elif traptor_type == 'track':
            query = "select tag, value from rules where type = 'keyword' and rule_set = '***REMOVED***'"
        else:
            raise ValueError('{} is not a valid traptor_type'.format(traptor_type))

        raw_rules = self._sql_dict(query)

        if traptor_type == 'follow':
            fixed_rules = self._fix_follow(raw_rules)
        if traptor_type == 'track':
            fixed_rules = self._fix_track(raw_rules)

        return fixed_rules

    def _sql_dict(self, query):
        """
        Run the MySQL query and return a dictionary without duplicate values.
        """
        self.cursor.execute(query)
        # Make the SQL results into a dictionary
        rules = [{'tag': tag, 'value': value} for (tag, value) in self.cursor]
        # De-duplicate the data
        return {r['value']: r for r in rules}.values()

    @staticmethod
    def _fix_follow(raw_rules):
        """ Custom fixes to convert Cooper rules to Traptor rules. """
        new_rules = []
        for idx, d in enumerate(raw_rules):
            # Twitter rules only only a single twitter id
            m = re.search(r'(\d{7,})', d['value'])
            if m:
                d['value'] = m.group(1)
                new_rules.append(d)
            else:
                logging.warning('No twitter id found in {}'.format(d['value']))

        return new_rules

    @staticmethod
    def _fix_track(raw_rules):
        """ Custom fixes to convert Cooper rules to Traptor rules. """
        new_rules = []
        for d in raw_rules:
            if re.match(r'url_contains', d['value']):
                # Make compatible with Twitter API
                # https://dev.twitter.com/streaming/overview/request-parameters
                d['value'] = re.sub(r'url_contains:\s?"?([\w\.]+)"?', r'\1', d['value'])
            d['value'] = re.sub(r'\.', ' ', d['value'])
            new_rules.append(d)

        return new_rules


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
        raise ValueError('{} is not a valid traptor_type'.format(traptor_type))

    r = redis.StrictRedis(host=host, port=port, db=db)

    for idx, d in enumerate(rules):
        crawler_num = idx / rule_max
        logging.debug('idx: {}, crawler_num: {}'.format(idx, crawler_num))
        r.hmset('traptor-{0}:{1}:{2}'.format(traptor_type, crawler_num, idx), d)


if __name__ == '__main__':
    """ To put rules in redis, run python rule_extract.py <track|follow> """

    cooper = CooperRules()
    cooper.connect()
    rules = cooper.parse_ctd_rules(sys.argv[1])

    for i in rules:
        logging.debug(i)
    send_to_redis(sys.argv[1], rules)
