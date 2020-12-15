from collections import defaultdict


def format_push_message(*, user_notice: dict, keywords: tuple, post_info: dict, is_test=False):
    result = {}
    for user_id, post_with_keywords in user_notice.items():
        items = post_with_keywords.items()
        if is_test:
            title = f'[測試]您有 {len(items)} 筆通知結果'
        else:
            title = f'您有 {len(items)} 筆通知結果'
        msg = ''
        user_keyword_count = defaultdict(int)
        for index, (post_id, keyword) in enumerate(items):
            keyword_msg = build_keywords(keywords, user_keyword_count)
            msg += '{index}) [{category}] {title}\n關鍵字: {keyword_msg}\n發文時間: {time}\n{url}\n'.format(index=index+1, keyword_msg=keyword_msg, **post_info[post_id])

        keyword_count_msg = ', '.join([f'{key}({count})' for key, count in user_keyword_count.items()])
        title += f', {keyword_count_msg}'

        result[user_id] = f'{title}\n{msg}--\n'

    return result


def build_keywords(keywords, user_keyword_count):
    result = []
    for keyword in keywords:
        user_keyword_count[keyword] += 1
        result.append(keyword)

    return ', '.join(result)
