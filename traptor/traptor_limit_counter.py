from scutils.stats_collector import RollingTimeWindow


class TraptorLimitCounter(RollingTimeWindow, object):
    def __init__(self, key='rolling_time_window_counter', cycle_time=5, window=60):
        super(TraptorLimitCounter, self).__init__(key, cycle_time, window)

    def increment(self, limit_count):
        # Add the limit count to Redis
        now = self._time()
        self.redis_conn.zadd(self.key, now, limit_count)