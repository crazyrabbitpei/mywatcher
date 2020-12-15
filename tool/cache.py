import redis
import os
from collections import defaultdict

GLOBAL_KEYWORD_TTL = 60*60*24*5


class Cache:
    pool = None
    def __init__(self, url):
        self.pool = redis.ConnectionPool.from_url(url, decode_responses=True)

    def get_subcribed_keywords(self, tablename='line_keyword'):
        '''
        return [(keyword_id, keyword, count), (...)]
        '''
        keywords = None


        return keywords

    def get_user_keyword_info_to_be_noticed(self, keyword_ids: tuple):
        '''
        return [(user_id1, keyword_id1), (user_id2, keyword_id2)...]
        '''
        user_keyword_info = None

        return user_keyword_info

    def get_keyword_last_fetch_time(self, keyword):
        r = redis.StrictRedis(connection_pool=self.pool)
        last_fetch_time = r.get(f'last_fetch_time:{keyword}')
        self.refresh_keyword_last_fetch_time_ttl(keyword)
        return last_fetch_time

    def set_keyword_last_fetch_time(self, keyword, fetch_time):
        r = redis.StrictRedis(connection_pool=self.pool)
        r.set(f'last_fetch_time:{keyword}', fetch_time)
        self.refresh_keyword_last_fetch_time_ttl(keyword)

    def refresh_keyword_last_fetch_time_ttl(self, keyword):
        r = redis.StrictRedis(connection_pool=self.pool)
        r.expire(f'last_fetch_time:{keyword}', GLOBAL_KEYWORD_TTL)
