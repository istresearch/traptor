from unittest import TestCase
import json
import redis
from scripts.rule_extract import jsonfile, sqldb
from click.testing import CliRunner
import pymysql


class TestJsonFile(TestCase):

    def setUp(self):
        self.redis_conn = redis.StrictRedis(host='localhost', port=6379, db=9)
        # Potentially dangerous if you have data you need to save here
        self.redis_conn.flushall()

    def test_track(self):
        """ Grab rules from file, put into local redis. """
        runner = CliRunner()
        result = runner.invoke(jsonfile, ['track', 'tests/track_rules.json'])

        # Ensure Click CLI command worked
        assert result.exit_code == 0

        # Check redis values
        self.assertDictEqual({'tag': 'test', 'value': 'happy'},
                             self.redis_conn.hgetall('traptor-track:0:0')
                             )
        self.assertDictEqual({'tag': 'test', 'value': 'love people'},
                             self.redis_conn.hgetall('traptor-track:0:1')
                             )

    def test_follow(self):
        """ Grab rules from file, put into local redis. """
        runner = CliRunner()
        result = runner.invoke(jsonfile, ['follow', 'tests/follow_rules.json'])

        assert result.exit_code == 0

        self.assertDictEqual({'tag': 'test', 'value': '1630833338'},
                             self.redis_conn.hgetall('traptor-follow:0:0')
                             )

    def tearDown(self):
        self.redis_conn.flushall()


class TestSqlDb(TestCase):

    def setUp(self):

        with open('tests/track_rules.json') as f:
            track_rules = [json.loads(line) for line in f]
        with open('tests/follow_rules.json') as f:
            follow_rules = [json.loads(line) for line in f]

        self.settings = {
            'HOST': 'localhost',
            'PORT': 3306,
            'USER': 'root',
            'PASSWD': '',
            'DB': 'test_traptor'
        }

        self.connection = pymysql.connect(host=self.settings['HOST'],
                                     user=self.settings['USER'],
                                     passwd='',
                                     )

        with self.connection.cursor() as cursor:
            cursor.execute('drop database if exists test_traptor;')
            cursor.execute('create database test_traptor;')
            cursor.execute('use test_traptor;')
            cursor.execute("""CREATE TABLE `rules` (
                            `id` int(11) NOT NULL AUTO_INCREMENT,
                            `user_id` int(11) DEFAULT NULL,
                            `rule_set` varchar(40) DEFAULT NULL,
                            `tag` varchar(100) DEFAULT NULL,
                            `type` varchar(100) DEFAULT NULL,
                            `value` varchar(100) CHARACTER SET utf8 DEFAULT NULL,
                            `description` varchar(100) DEFAULT NULL,
                            `status` varchar(100) DEFAULT NULL,
                            `detasking` varchar(100) DEFAULT NULL,
                            `date_added` datetime DEFAULT NULL,
                            `notes` varchar(100) DEFAULT NULL,
                            `appid` varchar(100) DEFAULT NULL,
                            PRIMARY KEY (`id`),
                            KEY `user_id` (`user_id`)
                          ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""")
        self.connection.commit()

        with self.connection.cursor() as cursor:
            for rule in track_rules:
                sql = "insert into `rules` (`tag`, `value`, `type`) values (%s, %s, %s)"
                cursor.execute(sql, (rule.get('tag'), rule.get('value'), 'keyword'))
            for rule in follow_rules:
                sql = "insert into `rules` (`tag`, `value`, `type`) values (%s, %s, %s)"
                cursor.execute(sql, (rule.get('tag'), rule.get('value'), 'username'))
        self.connection.commit()

    def sqldb_track(self):
        """ Grab rules from SQL, put into local redis, """
        runner = CliRunner()
        result = runner.invoke(sqldb, ['--settings', self.settings, 'track'])

        assert result.exit_code == 0

        self.assertDictEqual({'tag': 'test', 'value': 'happy'},
                             self.redis_conn.hgetall('traptor-track:0:0')
                             )
        self.assertDictEqual({'tag': 'test', 'value': 'love people'},
                             self.redis_conn.hgetall('traptor-track:0:1')
                             )

    def sqldb_follow(self):
        """ Grab rules from SQL, put into local redis, """
        runner = CliRunner()
        result = runner.invoke(sqldb, ['--settings', self.settings, 'follow'])

        assert result.exit_code == 0

        self.assertDictEqual({'tag': 'test', 'value': '1630833338'},
                             self.redis_conn.hgetall('traptor-follow:0:0')
                             )

    def tearDown(self):
        with self.connection.cursor() as cursor:
            cursor.execute('drop database if exists test_traptor;')
        self.redis_conn.flushall()