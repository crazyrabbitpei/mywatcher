from .ptt import parse_post_basic_info
from .cache import Cache
from elasticsearch import Elasticsearch, AsyncElasticsearch, RequestsHttpConnection, AIOHttpConnection, TransportError
import boto3
import pytz
tw_tz = pytz.timezone('Asia/Taipei')
from datetime import datetime, timedelta, timezone
import configparser
import logging, os
import json

config = configparser.ConfigParser()
config.read(os.environ.get('SETTING', 'settings.ini'))

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

service = 'es'

class Es:
    client = None
    filters = [

    ]
    patterns = [

    ]

    def __init__(self, *, auth, hosts=None, port=443, region=None, is_test=False):
        self.client = AsyncElasticsearch(
            http_auth=auth,
            hosts=hosts or ['127.0.0.1'],
            use_ssl=not is_test,
            verify_cert=not is_test,
            ssl_show_warn=not is_test,
            connection_class=AIOHttpConnection,
            timeout=int(config['REQUEST']['timeout']),
            max_retries=int(config['REQUEST']['max_retries']),
            retry_on_timeout=True
        )

    async def find(self, *, cache, index, keywords, is_test=False):
        '''
        return [{post_id: {category, title, time, url, keyword}}, {}]
        '''
        body = gen_body(cache=cache, index=index, keywords=keywords, is_test=is_test)

        now = datetime.now()
        tw_now = now.astimezone(tw_tz)
        now = tw_now.isoformat()
        try:
            result = await self.client.msearch(index=index, body=body)
        except TransportError as e:
            logger.error(f'搜尋失敗: {json.dumps(body, ensure_ascii=False)}')
            raise

        data = tuple(zip(result['responses'], keywords))
        try:
            result = parse_post_basic_info(data)
        except:
            logger.error(f'搜尋結果解析失敗: {json.dumps(data, ensure_ascii=False)}')
            raise

        return result, now


def gen_body(*, cache, index, keywords, is_test):
    last_time = 'now-1d'
    if is_test:
        myindex = {'index': index}
        myquery = {
            "size": 5,
            "sort": [
                {
                    "time": {
                        "order": "desc"
                    }
                }
            ],
            "query": {
                "match_all": {}
            }
        }
        return f'{json.dumps(myindex)}\n{json.dumps(myquery, ensure_ascii=False)}\n'

    body = ''
    for keyword in keywords:
        # 該關鍵字有上一次蒐集結果，此次搜尋範圍為上一次搜尋時間之後
        last_time = cache.get_keyword_last_fetch_time(keyword) or last_time

        myindex = {'index': index}
        myquery = {
            "size": 150,
            "sort": [
                {
                    "time": {
                        "order": "desc"
                    }
                }
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                            "match_phrase": {
                                "content": {
                                    "query": keyword,
                                }
                            }
                        },
                    ],
                    "filter": [
                        {
                            'range': {
                                'time': {
                                    'gte': f'{last_time}',
                                }
                            }
                        }
                    ]
                }
            }
        }
        body += f'{json.dumps(myindex)}\n{json.dumps(myquery, ensure_ascii=False)}\n'
    return body
