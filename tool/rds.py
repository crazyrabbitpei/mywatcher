from psycopg2 import connect, extensions

class Rds:
    conn = None
    def __init__(self, *, dbname, user, password, host='127.0.0.1', port=5432):
        conn_string = f'host={host} port={port} dbname={dbname} user={user} password={password}'
        self.conn = connect(conn_string)

    def get_subcribed_keywords(self, tablename='line_keyword'):
        '''
        return [(keyword, count), (...)]
        '''
        keywords = None

        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute('SELECT k.keyword, count(k.keyword) FROM line_keyword_users AS ku INNER JOIN line_keyword AS k ON ku.keyword_id = k.id group BY k.keyword;')
                keywords = sorted([(r[0], r[1]) for r in curs.fetchall()], key=lambda tup: tup[1], reverse=True)

        return keywords

    def get_user_keyword_info_to_be_noticed(self, keywords: tuple):
        '''
        return [(user_id1, keyword_1), (user_id2, keyword_2)...]
        '''
        user_keyword_info = None
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute('SELECT user_id, keyword FROM line_keyword_users AS ku INNER JOIN line_keyword AS k ON ku.keyword_id = k.id WHERE keyword IN %s ;', (keywords, ))
                user_keyword_info = [(r[0], r[1]) for r in curs.fetchall()]

        return user_keyword_info

    def close(self):
        self.conn.commit()
        self.conn.close()
