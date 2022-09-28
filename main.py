import json_lines
import json
import psycopg
from datetime import datetime

START = datetime.now()
ROWCOUNT = 10000

def create_author():
    conn = psycopg.connect(dbname="postgres", user="postgres", password="asd123")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS public.authors (id bigint PRIMARY KEY, name character varying(255), username character varying(255), description text, follower_count integer, following_count integer, tweet_count integer, listed_count integer );")
    conn.commit()
    cursor.close()
    conn.close()

def load_author():
    conn = psycopg.connect(dbname="postgres", user="postgres", password="asd123")

    with json_lines.open('D:/authors.jsonl.gz') as f:
        count = 1
        while True:
            head = []
            for x in range(ROWCOUNT):
                item = next(f, None)
                if item is None:
                    break
                head.append((item['id'], item['name'].replace('\x00', ''), item['username'],
                             json.dumps(item['description'], ensure_ascii=False).encode('utf8').decode(), item['public_metrics']['followers_count'],
                             item['public_metrics']['following_count'], item['public_metrics']['tweet_count'],
                             item['public_metrics']['listed_count']))

            cursor = conn.cursor()
            try:
                with cursor.copy("COPY authors (id, name, username, description, follower_count, following_count, "
                                 + "tweet_count, listed_count) FROM STDIN") as copy:
                    for record in head:
                        copy.write_row(record)
            except psycopg.errors.UniqueViolation:
                conn.rollback()
                print("UNIQUE ERROR")
                for record in head:
                    cursor.execute("INSERT INTO authors (id, name, username, description, follower_count, following_count, tweet_count, listed_count) VALUES(%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", record)
            conn.commit()
            cursor.close()
            print("done", count, datetime.now()-START)
            count = count+1
            if len(head) != ROWCOUNT:
                break
            del head
    conn.close()

def create_convo():
    conn = psycopg.connect(dbname="postgres", user="postgres", password="asd123")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS public.conversations (id int8 PRIMARY KEY, author_id int8, content text, possibly_sensitive bool, language varchar(3), source text, retweet_count int4, reply_count int4, like_count int4, quote_count int4, created_at timestamp with time zone);")
    conn.commit()
    cursor.close()
    conn.close()


def insert_by_one(conn, cursor, head):
    for record in head:
        try:
            cursor.execute(
                "INSERT INTO conversations (id, author_id, content, possibly_sensitive, language, source, retweet_count, reply_count, like_count, quote_count, created_at) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                record)
        except psycopg.errors.ForeignKeyViolation:
            conn.rollback()


def divide_and_insert(conn, cursor, head):
    try:
        with cursor.copy("COPY conversations (id, author_id, content, possibly_sensitive, language, source, "
                         + "retweet_count, reply_count, like_count, quote_count, created_at) FROM STDIN") as copy:
            for record in head:
                copy.write_row(record)
    except (psycopg.errors.ForeignKeyViolation, psycopg.errors.UniqueViolation):
        conn.rollback()
        ln = len(head)//2
        if ln < 1000:
            insert_by_one(conn, cursor, head)
            return
        divide_and_insert(conn, cursor, head[:ln])
        divide_and_insert(conn, cursor, head[ln:])



def load_convo():
    conn = psycopg.connect(dbname="postgres", user="postgres", password="asd123")

    with json_lines.open('D:/conversations.jsonl.gz') as f:
        count = 1
        while True:
            head = []
            for x in range(ROWCOUNT):
                item = next(f, None)
                if item is None:
                    break
                head.append((item['id'], item['author_id'], item['text'], item['possibly_sensitive'], item['lang'],
                            item['source'], item['public_metrics']['retweet_count'],
                            item['public_metrics']['reply_count'], item['public_metrics']['like_count'],
                            item['public_metrics']['quote_count'], item['created_at']))

            cursor = conn.cursor()
            divide_and_insert(conn, cursor, head)
            '''try:
                with cursor.copy("COPY conversations (id, author_id, content, possibly_sensitive, language, source, "
                                 + "retweet_count, reply_count, like_count, quote_count, created_at) FROM STDIN") as copy:
                    for record in head:
                        copy.write_row(record)
            except (psycopg.errors.ForeignKeyViolation, psycopg.errors.UniqueViolation):
                conn.rollback()
                for record in head:
                    try:
                        cursor.execute(
                            "INSERT INTO conversations (id, author_id, content, possibly_sensitive, language, source, retweet_count, reply_count, like_count, quote_count, created_at) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            record)
                    except psycopg.errors.ForeignKeyViolation:
                        conn.rollback()'''

            conn.commit()
            cursor.close()
            print("done", count, datetime.now() - START)
            count = count + 1
            if len(head) != ROWCOUNT:
                break
            del head
    conn.close()


if __name__ == '__main__':
    load_convo()
