from .ptt import parse_post_basic_info
from elasticsearch import Elasticsearch, AsyncElasticsearch, RequestsHttpConnection, AIOHttpConnection
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

    def __init__(self, *, auth, hosts=None, port=443, region=None):
        self.client = AsyncElasticsearch(
            http_auth=auth,
            hosts=hosts or ['127.0.0.1'],
            use_ssl=True,
            verify_cert=True,
            ssl_show_warn=False,
            scheme='https',
            port=port,
            connection_class=AIOHttpConnection,
            timeout=int(config['REQUEST']['timeout']),
            max_retries=int(config['REQUEST']['max_retries']),
            retry_on_timeout=True
        )

    async def find(self, index, keyword_id, keyword, last_time):
        '''
        return [{post_id: {category, title, time, url, keyword_id}}, {}]
        '''
        body = {
            'query': {
                'bool': {
                    'must': [
                        {
                            'match': {
                                'content': {
                                    'operator': 'and', 'query': keyword
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

        result = await self.client.search(index=index, body=body)
        return parse_post_basic_info(keyword_id, keyword, result)
