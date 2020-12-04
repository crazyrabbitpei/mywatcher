import boto3
import logging
import os
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
service = 'es'
region = os.getenv('ES_REGION')

def get():
    if os.environ.get('AUTH', 'basic') == 'aws':
        credentials = boto3.Session().get_credentials()
        logger.info('Get by aws auth')
        from requests_aws4auth import AWS4Auth
        auth = AWS4Auth(credentials.access_key, credentials.secret_key,
                        region, service, session_token=credentials.token)
    else:
        logger.info('Get by basic auth')
        auth = (os.getenv('ES_USER'), os.getenv("ES_PASSWD"))

    return auth
