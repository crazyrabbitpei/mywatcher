from .ptt import parse_post_basic_info
from elasticsearch import Elasticsearch, AsyncElasticsearch, RequestsHttpConnection, AIOHttpConnection, TransportError
import boto3
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
            port=port,
            connection_class=AIOHttpConnection,
            timeout=int(config['REQUEST']['timeout']),
            max_retries=int(config['REQUEST']['max_retries']),
            retry_on_timeout=True
        )

    async def find(self, *, index, keyword_infos, last_time, is_test=False):
        '''
        return [{post_id: {category, title, time, url, keyword_id}}, {}]
        '''
        body = gen_body(index=index, keywords=list(zip(*keyword_infos))[1], last_time=last_time, is_test=is_test)

        try:
            result = await self.client.msearch(index=index, body=body)
        except TransportError as e:
            logger.error(f'搜尋失敗: {json.dumps(body)}')
            raise

        data = tuple(zip(result['responses'], keyword_infos))
        return parse_post_basic_info(data)


def gen_body(*, index, keywords, last_time, is_test):
    if is_test:
        myindex = {'index': index}
        myquery = {
            "size": 5,
            "query": {
                "match_all": {}
            }
        }
        return f'{json.dumps(myindex)}\n{json.dumps(myquery, ensure_ascii=False)}\n'

    body = ''
    for keyword in keywords:
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
