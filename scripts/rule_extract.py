#!/usr/bin/env python

import sys
import json
import re
import redis
import pymysql
import logging

import click

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
    """ Parse gnip rules, return tweet ids"""
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


class SQLRules(object):
    """
    Class to handle a MySQL connection and parse out rules in a
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
            Parse SQL rules.  Returns a list of dictionaries that
            contain {tag:, value:} pairs.
        """
        if traptor_type == 'follow':
            query = "select tag, value from rules where type = 'username'"
        elif traptor_type == 'track':
            query = "select tag, value from rules where type = 'keyword'"
        elif traptor_type == 'locations':
            query = "select tag, value from rules where type = 'geo'"
        else:
            raise ValueError('{} is not a valid traptor_type'.format(traptor_type))

        raw_rules = self._sql_dict(query)

        if traptor_type == 'follow':
            fixed_rules = self._fix_follow(raw_rules)
        if traptor_type == 'track':
            fixed_rules = self._fix_track(raw_rules)
        if traptor_type == 'locations':
            fixed_rules = self._fix_locations(raw_rules)

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
        """ Custom fixes to convert SQL rules to Traptor rules. """
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
        """ Custom fixes to convert SQL rules to Traptor rules. """
        new_rules = []
        for d in raw_rules:
            if re.match(r'url_contains', d['value']):
                # Make compatible with Twitter API
                # https://dev.twitter.com/streaming/overview/request-parameters
                d['value'] = re.sub(r'url_contains:\s?"?([\w\.]+)"?', r'\1', d['value'])
            d['value'] = re.sub(r'\.', ' ', d['value'])
            new_rules.append(d)

        return new_rules

    @staticmethod
    def _fix_locations(raw_rules):
        """ Custom fixes to convert SQL rules to Traptor rules. """
        new_rules = []
        for d in raw_rules:
            logging.debug(d)
            if d['value']:
                # Take out brackets
                d['value'] = re.sub(r'\[|\]', '', d['value'])
                # Add commas
                d['value'] = re.sub(r'\s', ',', d['value'])
                new_rules.append(d)

        return new_rules


class RulesToRedis(object):
    """ Class to connect to redis and send traptor rules. """
    def __init__(self, redis_conn):
        self.redis_conn = redis_conn

    def rule_max(self, traptor_type):
        """ Send the rule_max based on what traptor_type is passed in. """
        if traptor_type == 'follow':
            self._rule_max = 5000
        elif traptor_type == 'track':
            self._rule_max = 400
        elif traptor_type == 'locations':
            self._rule_max = 25
        else:
            raise ValueError('{} is not a valid traptor_type'.format(
                             traptor_type))

        return self._rule_max

    def send_rules(self, traptor_type, rules):
        """ Send rules out to Redis with the appropriate key, value format. """
        for idx, d in enumerate(rules):
            crawler_num = idx / self.rule_max(traptor_type)
            logging.debug('idx: {}, crawler_num: {}'.format(idx, crawler_num))
            self.redis_conn.hmset('traptor-{0}:{1}:{2}'.format(
                                  traptor_type, crawler_num, idx), d)


@click.group()
def cli():
    pass


@click.command()
@click.option('--settings', default=mysql_settings, help='dictionary of sql settings')
@click.argument('traptor_type')
def sqldb(settings, traptor_type):
    """ Connect to SQL database, send to Redis """
    sql = SQLRules(host=settings['HOST'],
                   port=settings['PORT'],
                   user=settings['USER'],
                   passwd=settings['PASSWD'],
                   db=settings['DB']
                   )
    sql.connect()
    rules = sql.parse_ctd_rules(traptor_type)

    # Send to Redis
    conn = redis.StrictRedis(host=redis_settings['HOST'],
                             port=redis_settings['PORT'],
                             db=redis_settings['DB']
                             )
    rc = RulesToRedis(conn)
    rc.send_rules(traptor_type, rules)


@click.command()
@click.argument('traptor_type')
@click.argument('filename')
@click.option('--conn', default=None)
def jsonfile(traptor_type, filename, conn):
    """ Reads JSON rules from file, send to Redis """
    with open(filename) as f:
        rules = [json.loads(line) for line in f]

    # Send to Redis
    if not conn:
        conn = redis.StrictRedis(host=redis_settings['HOST'],
                                 port=redis_settings['PORT'],
                                 db=redis_settings['DB']
                                 )
    rc = RulesToRedis(conn)
    rc.send_rules(traptor_type, rules)


if __name__ == '__main__':
    cli.add_command(sqldb)
    cli.add_command(jsonfile)
    cli()
