import redis
import os
from collections import defaultdict

GLOBAL_KEYWORD_TTL = 60*60*24*5

pool = redis.ConnectionPool.from_url(os.getenv('REDIS_URL'), decode_responses=True)

def get_keyword_last_fetch_time(keyword):
    r = redis.StrictRedis(connection_pool=pool)
    last_fetch_time = r.get(f'last_fetch_time:{keyword}')
    refresh_keyword_last_fetch_time_ttl(r, keyword)
    return last_fetch_time


def set_keyword_last_fetch_time(keyword, fetch_time):
    r = redis.StrictRedis(connection_pool=pool)
    r.set(f'last_fetch_time:{keyword}', fetch_time)
    refresh_keyword_last_fetch_time_ttl(r, keyword)

def refresh_keyword_last_fetch_time_ttl(r, keyword):
    r.expire(f'last_fetch_time:{keyword}', GLOBAL_KEYWORD_TTL)
