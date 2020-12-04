from .ptt import parse_post_basic_info
from elasticsearch import Elasticsearch, AsyncElasticsearch
import boto3
import logging, os
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


host = os.getenv('ES_HOST')
region = os.getenv('ES_REGION')  # e.g. us-west-1
service = 'es'
auth = None

credentials = boto3.Session().get_credentials()
if credentials:
    logger.info('Connect Es by aws auth')
    from requests_aws4auth import AWS4Auth
    auth = AWS4Auth(credentials.access_key, credentials.secret_key,
                    region, service, session_token=credentials.token)
else:
    logger.info('Connect Es by basic auth')
    auth = (os.getenv('ES_USER'), os.getenv("ES_PASSWD"))

class Es:
    client = None
    filters = [

    ]
    patterns = [

    ]
    def __init__(self, hosts=None, port=443):
        self.client = AsyncElasticsearch(
            http_auth=auth,
            hosts=hosts or ['127.0.0.1'],
            use_ssl=True,
            verify_cert=False,
            ssl_show_warn=False,
            scheme='https',
            port=port,
        )

    async def find(self, keyword_id, keyword, last_time):
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
        result = await self.client.search(body=body)
        return parse_post_basic_info(keyword_id, keyword, result)
