def parse_post_basic_info(data):
    for result, keyword in data:
        hits = result['hits']['hits']
        parsed_result = {}
        for hit in hits:
            post_id = hit['_source']['id']
            parsed_result[post_id] = {}
            parsed_result[post_id]['category'] = hit['_source'].get('category', '')
            parsed_result[post_id]['title'] = hit['_source'].get('title', '')
            parsed_result[post_id]['time'] = hit['_source'].get('time', '')
            parsed_result[post_id]['url'] = hit['_source']['url']
            parsed_result[post_id]['keyword'] = keyword

    return parsed_result
