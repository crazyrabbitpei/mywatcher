from collections import defaultdict
def format_push_message(*, user_notice: dict, keyword_info: tuple, post_info: dict):
    result = {}
    for user_id, post_with_keywords in user_notice.items():
        items = post_with_keywords.items()
        title = f'您有 {len(items)} 筆通知結果'
        msg = ''
        user_keyword_count = defaultdict(int)
        for index, (post_id, keyword_ids) in enumerate(items):
            keywords = build_keywords(keyword_ids, keyword_info, user_keyword_count)
            msg += '[{index}]{category} {title}\n關鍵字: {keywords}\n發文時間: {time}\n{url}\n'.format(index=index+1, keywords=keywords, **post_info[post_id])

        keyword_count_msg = ', '.join([f'{key}({count})' for key, count in user_keyword_count.items()])
        title += f', {keyword_count_msg}'

        result[user_id] = f'{title}\n{msg}--\n'

    return result


def build_keywords(keyword_ids, keyword_info, user_keyword_count):
    keywords = []
    for keyword_id in keyword_ids:
        user_keyword_count[keyword_info[1][keyword_id]] += 1
        keywords.append(keyword_info[1][keyword_id])

    return ', '.join(keywords)
