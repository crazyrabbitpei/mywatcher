import os
import sys
import logging
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import (
    InvalidSignatureError, LineBotApiError
)
from linebot.models import (
    MessageEvent, FollowEvent, UnfollowEvent, TextSendMessage, TemplateSendMessage, ButtonsTemplate, ConfirmTemplate, CarouselTemplate, MessageAction
)

line_bot_api = LineBotApi(os.getenv('CHANNEL_ACCESS_TOKEN'))
handler = WebhookHandler(os.getenv('CHANNEL_SECRET'))

def push_message(*, user_id, message=None):
    '''
    return (ok_or_not:boolean, user_id, status_code, message)
    '''
    try:
        line_bot_api.push_message(user_id, TextSendMessage(text=f'{message}'))
    except LineBotApiError as e:
        return (False, user_id, e.status_code, e.message)

    return (True, user_id, 200, 'ok')
