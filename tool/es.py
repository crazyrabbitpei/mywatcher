from .ptt import parse_post_basic_info
from elasticsearch import Elasticsearch, AsyncElasticsearch

class Es:
    client = None
    filters = [

    ]
    patterns = [

    ]
    def __init__(self, http_auth, hosts=None, port=443):
        self.client = AsyncElasticsearch(
            http_auth=http_auth,
            hosts=hosts or ['127.0.0.1'],
            use_ssl=True,
            verify_cert=False,
            ssl_show_warn=False,
            scheme='https',
            port=port,
        )

    async def find(self, keyword_id, keyword, last_time='now-10m'):
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
                                    #'gte': '2020-12-03T22:22:17.548039+08:00',
                                }
                            }
                        }
                    ]
                }
            }
        }

        result = await self.client.search(body=body)
        return parse_post_basic_info(keyword_id, keyword, result)
