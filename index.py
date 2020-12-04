import logging
import logging.config
import configparser
from yaml import safe_load, safe_dump
from dotenv import load_dotenv
load_dotenv()

import pytz
tw_tz = pytz.timezone('Asia/Taipei')
from datetime import datetime, timedelta, timezone
import os, sys
import httpx
import asyncio
from collections import defaultdict

import tool.auth as Auth
from tool.rds import Rds
from tool.es import Es
from tool.line import push_message
from tool.message import format_push_message

config = configparser.ConfigParser()
with open('settings.ini') as f:
    config.read_file(f)

with open('logconfig.yaml', 'r') as f:
    log_config = safe_load(f)
    logging.config.dictConfig(log_config)


es_log = logging.getLogger("elasticsearch")
es_log.setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

USER_NOTICED_INFO = {} # 使用者和要通發送的文章和對應關鍵字
POST_INFO = {} # 文章和細節
KEYWORD_INFO = defaultdict(list)  # keyword_id和有該keyword的文章對應
KEYWORD_VALUE = {} # keyword_id和keyword名的對應

NOW = 'now-10m'

loop = asyncio.get_event_loop()


async def main(*, rds, es):
    global NOW
    # 拿取rds關鍵字清單
    try:
        result = rds.get_subcribed_keywords()
    except:
        logging.error('關鍵字拿取失敗')
        raise
    else:
        keyword_ids, keywords, counts = list((zip(*result)))
        keyword_infos = tuple(zip(keyword_ids, keywords))
        KEYWORD_VALUE = dict(keyword_infos)

    logger.debug(keyword_infos)

    last_time = NOW

    # es查詢關鍵字結果
    logger.debug(f'Search time greater then {last_time}')
    tasks = [asyncio.create_task(es.find(
        keyword_id, keyword, last_time=last_time)) for keyword_id, keyword in keyword_infos]

    try:
        result = await asyncio.gather(*tasks)

    except:
        logging.error('關鍵字搜尋失敗')
        raise
    else:
        create_post_and_keyword_info(result)

    keyword_ids = tuple(KEYWORD_INFO.keys())
    logger.debug(keyword_ids)
    if len(keyword_ids) == 0:
        clean_result()
        await asyncio.sleep(int(config['WATCHER']['interval']))
        return

    now = datetime.now()
    tw_now = now.astimezone(tw_tz)
    NOW = tw_now.isoformat()
    # rds查詢關鍵字訂閱者
    try:
        result = rds.get_user_keyword_info_to_be_noticed(keyword_ids)
    except:
        logger.error('搜尋訂閱使用者失敗')
        raise
    else:
        create_user_notice_info(result)
        logger.debug(USER_NOTICED_INFO)
    # 發送訂閱內容
    messages = format_push_message(user_notice=USER_NOTICED_INFO, keyword_info=(KEYWORD_INFO, KEYWORD_VALUE), post_info=POST_INFO)
    logger.debug(messages)
    tasks = [asyncio.to_thread(push_message, **{'user_id': user_id, 'message': msg}) for user_id, msg in messages.items()]

    try:
        result = await asyncio.gather(*tasks)
    except:
        logger.error('主動通知失敗')
        raise

    logger.info(result)
    clean_result()
    await asyncio.sleep(int(config['WATCHER']['interval']))


def clean_result():
    global USER_NOTICED_INFO # 使用者和要通發送的文章和對應關鍵字
    USER_NOTICED_INFO = {}
    global POST_INFO # 文章和細節
    POST_INFO = {}
    global KEYWORD_INFO # keyword_id和有該keyword的文章對應
    KEYWORD_INFO = defaultdict(list)
    global KEYWORD_VALUE # keyword_id和keyword名的對應
    KEYWORD_VALUE = {}

def create_post_and_keyword_info(result):
    '''
    POST_INFO: dict, {post_id1: {category, title, time, url, keyword_id}, post_id2: {category, title, time, url, keyword_id}, ...}
    KEYWORD_INFO: dict(list), {keyword_id1: [post_id1, post_id2, ...], keyword_id2: [post_id1, post_id2, ...], ...}
    '''
    for r in result:
        for post_id, info in r.items():
            if post_id not in POST_INFO:
                POST_INFO[post_id] = info
            keyword_id = info['keyword_id']
            KEYWORD_INFO[keyword_id].append(post_id)


def create_user_notice_info(result):
    '''
    USER_NOTICED_INFO: dict(dict(list)), {user_id1: {post_id1: [keyword_id1, keyword_id2, ...], post_id2: [keyword_id1, keyword_id2, ...], ...}, user_id2: {...}, ...}
    '''
    for user_id, keyword_id in result:
        if user_id not in USER_NOTICED_INFO:
            USER_NOTICED_INFO[user_id] = {}
        for post_id in KEYWORD_INFO[keyword_id]:
            if post_id not in USER_NOTICED_INFO[user_id]:
                USER_NOTICED_INFO[user_id][post_id] = []
            USER_NOTICED_INFO[user_id][post_id].append(keyword_id)


if __name__ == '__main__':
    try:
        rds = Rds(host=os.getenv('RDS_HOST'), dbname=os.getenv('RDS_DBNAME'), user=os.getenv(
            'RDS_USER'), password=os.getenv('RDS_PASSWD'))
    except:
        logging.error('rds連線失敗', exc_info=True)
        sys.exit(0)

    try:
        auth = Auth.get()
        es = Es(auth=auth, hosts=os.getenv('ES_HOSTS').split(','), port=os.getenv('ES_PORT'), region=os.getenv('ES_REGION'))
    except:
        logging.error('es連線失敗', exc_info=True)
        sys.exit(0)

    now = None
    while True:
        try:
            loop.run_until_complete(main(rds=rds, es=es))
            #asyncio.run(main(rds=rds, es=es))
        except:
            logger.error('系統運行失敗', exc_info=True)
            break

    loop.close()
    rds.close()
