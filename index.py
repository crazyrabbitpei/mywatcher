import logging
import logging.config
import configparser
from yaml import safe_load, safe_dump
from dotenv import load_dotenv
load_dotenv()

import pytz
tw_tz = pytz.timezone('Asia/Taipei')
from datetime import datetime, timedelta, timezone
import os, sys, time
import json
import httpx
import elasticsearch
import asyncio
import argparse
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
KEYWORD_POSTS = defaultdict(list)  # keyword_id和有該keyword的文章對應
KEYWORD_VALUE = {} # keyword_id和keyword名的對應
KEYWORD_LAST_FETCH_TIME = {} # 該keyword被蒐集到的最新時間

loop = asyncio.get_event_loop()


async def main(*, rds, es, is_test=False):
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

    if is_test:
        keyword_infos = (keyword_infos[0], )
        KEYWORD_VALUE = dict(keyword_infos)
        logger.warning(f'目前為測試模式, 會隨便拿一個keyword，實際上也並不會用此key去搜尋文章內容，而是拿最近前5篇文章')

    logger.debug(keyword_infos)

    # es查詢關鍵字結果
    retry = True
    timestamp = None
    while retry:
        try:
            result, timestamp = await es.find(index=os.getenv('ES_INDEX'), keyword_infos=keyword_infos, keyword_last_fetch_time=KEYWORD_LAST_FETCH_TIME, is_test=is_test)

        except elasticsearch.TransportError as e:
            logger.error(f"搜尋失敗, {e.error}: {e.status_code}, {json.dumps(e.info)}")
            logger.error(f"{int(config['REQUEST']['retry_after'])} 秒後重新搜尋")
            time.sleep(int(config['REQUEST']['retry_after']))
        except:
            logging.error('關鍵字搜尋失敗')
            raise
        else:
            create_post_and_keyword_info(result)
            update_keyword_last_fetech_time(result, timestamp)

            retry = False

    keyword_ids = tuple(KEYWORD_POSTS.keys())
    logger.debug(keyword_ids)
    # 沒有任何關鍵字結果
    if len(keyword_ids) == 0:
        clean_result()
        await asyncio.sleep(int(config['WATCHER']['interval']))
        return

    # rds查詢關鍵字訂閱者
    try:
        result = rds.get_user_keyword_info_to_be_noticed(keyword_ids)
    except:
        logger.error('搜尋訂閱使用者失敗')
        raise

    if is_test:
        result = [(os.getenv('MY_LINE_TEST_USER'), keyword_ids[0])]
        logger.warning(f'目前為測試模式, 測試對象為: {os.getenv("MY_LINE_TEST_USER")}, 關鍵字為: {keyword_infos[0][1]}')

    # 產生使用者和訂閱資訊
    create_user_notice_info(result)
    logger.debug(USER_NOTICED_INFO)

    # 發送訂閱內容
    messages = format_push_message(user_notice=USER_NOTICED_INFO, keyword_info=(KEYWORD_POSTS, KEYWORD_VALUE), post_info=POST_INFO, is_test=is_test)
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
    global KEYWORD_POSTS # keyword_id和有該keyword的文章對應
    KEYWORD_POSTS = defaultdict(list)
    global KEYWORD_VALUE # keyword_id和keyword名的對應
    KEYWORD_VALUE = {}

def create_post_and_keyword_info(result):
    '''
    POST_INFO: dict, {post_id1: {category, title, time, url, keyword_id}, post_id2: {category, title, time, url, keyword_id}, ...}
    KEYWORD_POSTS: dict(list), {keyword_id1: [post_id1, post_id2, ...], keyword_id2: [post_id1, post_id2, ...], ...}
    '''
    for r in result:
        for post_id, info in r.items():
            if post_id not in POST_INFO:
                POST_INFO[post_id] = info
            keyword_id = info['keyword_id']
            KEYWORD_POSTS[keyword_id].append(post_id)

def update_keyword_last_fetech_time(result, timestamp):
    '''
    KEYWORD_LAST_FETCH_TIME: dict, {keyword: last_fetch_time}
    '''
    for r in result:
        for post_id, info in r.items():
            keyword_id = info['keyword_id']
            KEYWORD_LAST_FETCH_TIME[KEYWORD_VALUE[keyword_id]] = timestamp


def create_user_notice_info(result):
    '''
    USER_NOTICED_INFO: dict(dict(list)), {user_id1: {post_id1: [keyword_id1, keyword_id2, ...], post_id2: [keyword_id1, keyword_id2, ...], ...}, user_id2: {...}, ...}
    '''
    for user_id, keyword_id in result:
        if user_id not in USER_NOTICED_INFO:
            USER_NOTICED_INFO[user_id] = {}
        for post_id in KEYWORD_POSTS[keyword_id]:
            if post_id not in USER_NOTICED_INFO[user_id]:
                USER_NOTICED_INFO[user_id][post_id] = []
            USER_NOTICED_INFO[user_id][post_id].append(keyword_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true', help='測試模式狀態會隨便拿取一個keyword並且拿取最近前五篇文章(不會搜尋該keyword)，發送通知對象會是開發者的line。會忽略ssl驗證')
    args = parser.parse_args()
    try:
        rds = Rds(host=os.getenv('RDS_HOST'), dbname=os.getenv('RDS_DBNAME'), user=os.getenv(
            'RDS_USER'), password=os.getenv('RDS_PASSWD'))
    except:
        logging.error('rds連線失敗', exc_info=True)
        sys.exit(0)

    try:
        auth = Auth.get()
        es = Es(auth=auth, hosts=os.getenv('ES_HOSTS').split(','), port=os.getenv('ES_PORT'), region=os.getenv('ES_REGION'), is_test=args.test)
    except:
        logging.error('es連線失敗', exc_info=True)
        sys.exit(0)

    now = None
    while True:
        try:
            loop.run_until_complete(main(rds=rds, es=es, is_test=args.test))
            #asyncio.run(main(rds=rds, es=es))
        except:
            logger.error('系統運行失敗', exc_info=True)
            break

        if args.test:
            break

    loop.close()
    rds.close()
