from .ptt import parse_post_basic_info
from elasticsearch import Elasticsearch, AsyncElasticsearch, RequestsHttpConnection, AIOHttpConnection, TransportError
import boto3
import configparser
import logging, os

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

    async def find(self, *, index, keyword_id, keyword, last_time, is_test=False):
        '''
        return [{post_id: {category, title, time, url, keyword_id}}, {}]
        '''
        body = gen_body(keyword=keyword, last_time=last_time, is_test=is_test)

        try:
            result = await self.client.search(index=index, body=body)
        except TransportError as e:
            raise TransportError(e)

        return parse_post_basic_info(keyword_id, keyword, result)


def gen_body(*, keyword, last_time, is_test=False):
    if not is_test:
        return {
            "sort": [
                {
                    "time": {
                        "order": "desc"
                    }
                }
            ],
            'query': {
                'bool': {
                    'must': [
                        {
                            'match_phrase': {
                                'content': {
                                    'query': keyword
                                }
                            }
                        }
                    ],
                    'filter': [
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
    else:
        return {
            "size": 5,
            "query": {
                "match_all": {}
            }
        }
