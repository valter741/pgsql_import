import json_lines
import json
import psycopg
from datetime import datetime

START = datetime.now()
ROWCOUNT = 10000


def create_tables():
    conn = psycopg.connect(dbname="postgres", user="postgres", password="asd123")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.authors (
            id bigint PRIMARY KEY NOT NULL,
            name character varying(255),
            username character varying(255),
            description text,
            follower_count integer,
            following_count integer,
            tweet_count integer,
            listed_count integer );""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.conversations (
            id bigint NOT NULL,
            author_id bigint NOT NULL,
            content text NOT NULL,
            possibly_sensitive bool NOT NULL,
            language varchar(3) NOT NULL,
            source text NOT NULL,
            retweet_count integer,
            reply_count integer,
            like_count integer,
            quote_count integer,
            created_at timestamp with time zone NOT NULL );""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.annotations (
            id BIGSERIAL PRIMARY KEY,
            conversation_id bigint NOT NULL,
            value text NOT NULL,
            type text NOT NULL,
            probability numeric(4, 3) NOT NULL );""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.links (
            id BIGSERIAL PRIMARY KEY,
            conversation_id bigint NOT NULL,
            url varchar(2048) NOT NULL,
            title text,
            description text );""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.conversation_references (
            id BIGSERIAL PRIMARY KEY,
            conversation_id bigint NOT NULL,
            parent_id bigint NOT NULL,
            type varchar(20)  NOT NULL );""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.context_annotations (
            id BIGSERIAL PRIMARY KEY,
            conversation_id bigint NOT NULL,
            context_domain_id bigint NOT NULL,
            context_entity_id bigint NOT NULL );""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.context_domains (
            id bigint,
            name varchar(255) NOT NULL,
            description text );""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.context_entities (
            id bigint,
            name varchar(255) NOT NULL,
            description text );""")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.conversation_hashtags (
            id BIGSERIAL PRIMARY KEY,
            conversation_id bigint NOT NULL,
            hashtag_id bigint,
            tag text );""")
    conn.commit()
    cursor.close()
    conn.close()


def fix_queries():
    conn = psycopg.connect(dbname="postgres", user="postgres", password="asd123")
    cursor = conn.cursor()
    blockstart = datetime.now()
    cursor.execute("""
        DELETE FROM conversations a USING (
            SELECT MIN(ctid) as ctid, id
                FROM conversations 
                GROUP BY id HAVING COUNT(*) > 1
            ) b
            WHERE a.id = b.id
            AND a.ctid <> b.ctid;
        
        ALTER TABLE conversations ADD CONSTRAINT pk_conv PRIMARY KEY ("id");
        
        INSERT INTO authors (id)
            SELECT DISTINCT author_id FROM conversations conv WHERE conv.author_id NOT IN (
                SELECT id FROM authors auth WHERE auth.id = conv.author_id);
        
        ALTER TABLE conversations
        ADD FOREIGN KEY (author_id) REFERENCES authors (id); """)
    conn.commit()
    timer(blockstart)
    blockstart = datetime.now()
    cursor.execute("""
        ALTER TABLE annotations
            ADD CONSTRAINT fk_conv FOREIGN KEY (conversation_id) REFERENCES conversations (id);
        ALTER TABLE links
            ADD CONSTRAINT fk_conv FOREIGN KEY (conversation_id) REFERENCES conversations (id); """)
    conn.commit()
    timer(blockstart)
    blockstart = datetime.now()
    cursor.execute("""
        DELETE FROM conversation_references an
        WHERE  NOT EXISTS (
           SELECT FROM conversations con
           WHERE  con.id = an.parent_id
           ); 
        
        ALTER TABLE conversation_references
            ADD CONSTRAINT fk_conv FOREIGN KEY (conversation_id) REFERENCES conversations (id);
            
        ALTER TABLE conversation_references
            ADD CONSTRAINT fk_conv2 FOREIGN KEY (parent_id) REFERENCES conversations (id); """)
    conn.commit()
    timer(blockstart)
    blockstart = datetime.now()
    cursor.execute("""
        DELETE FROM context_domains a USING (
            SELECT MIN(ctid) as ctid, id
                FROM context_domains 
                GROUP BY id HAVING COUNT(*) > 1
            ) b
            WHERE a.id = b.id
            AND a.ctid <> b.ctid; """)
    conn.commit()
    timer(blockstart)
    blockstart = datetime.now()
    cursor.execute("""
        DELETE FROM context_entities a USING (
            SELECT MIN(ctid) as ctid, id
                FROM context_entities
                GROUP BY id HAVING COUNT(*) > 1
            ) b
            WHERE a.id = b.id
            AND a.ctid <> b.ctid; """)
    conn.commit()
    timer(blockstart)
    blockstart = datetime.now()
    cursor.execute("""
        ALTER TABLE context_domains ADD CONSTRAINT pk_dom PRIMARY KEY ("id");
        
        ALTER TABLE context_entities ADD CONSTRAINT pk_ent PRIMARY KEY ("id");
        
        ALTER TABLE context_annotations
            ADD CONSTRAINT fk_conv FOREIGN KEY (conversation_id) REFERENCES conversations (id);
            
        ALTER TABLE context_annotations
            ADD CONSTRAINT fk_dom FOREIGN KEY (context_domain_id) REFERENCES context_domains (id);
            
        ALTER TABLE context_annotations
            ADD CONSTRAINT fk_entity FOREIGN KEY (context_entity_id) REFERENCES context_entities (id);""")
    conn.commit()
    timer(blockstart)
    blockstart = datetime.now()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hashtags AS 
            (SELECT id, tag FROM conversation_hashtags);
            
        ALTER TABLE hashtags
            ADD CONSTRAINT pk_tag PRIMARY KEY (id);""")
    conn.commit()
    timer(blockstart)
    blockstart = datetime.now()
    cursor.execute("""
        DELETE FROM hashtags a USING (
            SELECT MIN(ctid) as ctid, tag
                FROM hashtags 
                GROUP BY tag HAVING COUNT(*) > 1
            ) b
            WHERE a.tag = b.tag
            AND a.ctid <> b.ctid;
        
        ALTER TABLE hashtags
            ADD CONSTRAINT u_tag UNIQUE (tag);""")
    conn.commit()
    timer(blockstart)
    blockstart = datetime.now()
    cursor.execute("""
        UPDATE conversation_hashtags
            SET hashtag_id = hashtags.id
            FROM hashtags
            WHERE hashtags.tag = conversation_hashtags.tag;""")
    conn.commit()
    timer(blockstart)
    blockstart = datetime.now()
    cursor.execute("""
        ALTER TABLE conversation_hashtags
            DROP COLUMN tag;
          
        ALTER TABLE conversation_hashtags 
            ADD CONSTRAINT fk_conv FOREIGN KEY (conversation_id) REFERENCES conversations (id);
          
        ALTER TABLE conversation_hashtags 
            ADD CONSTRAINT fk_hash FOREIGN KEY (hashtag_id) REFERENCES hashtags (id);""")
    conn.commit()
    timer(blockstart)
    cursor.close()
    conn.close()


def load_author():
    conn = psycopg.connect(dbname="postgres", user="postgres", password="asd123")

    with json_lines.open('D:/authors.jsonl.gz') as f:
        count = 1
        while True:
            head = []
            blockstart = datetime.now()
            for x in range(ROWCOUNT):
                item = next(f, None)
                if item is None:
                    break
                head.append((item['id'], item['name'].replace('\x00', ''), item['username'],
                             json.dumps(item['description'], ensure_ascii=False).encode('utf8').decode(),
                             item['public_metrics']['followers_count'], item['public_metrics']['following_count'],
                             item['public_metrics']['tweet_count'], item['public_metrics']['listed_count']))

            cursor = conn.cursor()
            try:
                with cursor.copy("COPY authors (id, name, username, description, follower_count, following_count, "
                                 + "tweet_count, listed_count) FROM STDIN") as copy:
                    for record in head:
                        copy.write_row(record)
            except psycopg.errors.UniqueViolation:
                conn.rollback()
                for record in head:
                    cursor.execute(
                        "INSERT INTO authors (id, name, username, description, follower_count, following_count, tweet_count, listed_count) VALUES(%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        record)
            conn.commit()
            cursor.close()
            timer(blockstart)
            count = count + 1
            if len(head) != ROWCOUNT:
                break
            del head
    conn.close()


def load_convo():
    conn = psycopg.connect(dbname="postgres", user="postgres", password="asd123")

    with json_lines.open('D:/conversations.jsonl.gz') as f:
        count = 1
        while True:
            head = []
            annotations = []
            links = []
            refs = []
            domains = []
            entities = []
            context = []
            contags = []
            blockstart = datetime.now()
            for x in range(ROWCOUNT):
                item = next(f, None)
                '''if x < 50 and 'entities' in item and 'hashtags' in item['entities']:
                    print(item['entities']['hashtags'])'''

                if item is None:
                    break

                if 'context_annotations' in item:
                    for con in item['context_annotations']:
                        context.append((item['id'], con['domain']['id'], con['entity']['id']))
                        domdesc = None if 'description' not in con['domain'] else con['domain']['description']
                        domains.append((con['domain']['id'], con['domain']['name'], domdesc))
                        entdesc = None if 'description' not in con['entity'] else con['entity']['description']
                        entities.append((con['entity']['id'], con['entity']['name'], entdesc))

                if 'entities' in item:
                    if 'hashtags' in item['entities']:
                        for tag in item['entities']['hashtags']:
                            contags.append((item['id'], tag['tag']))
                    if 'annotations' in item['entities']:
                        for anno in item['entities']['annotations']:
                            annotations.append((item['id'], anno['normalized_text'], anno['type'], anno['probability']))
                    if 'urls' in item['entities']:
                        for url in item['entities']['urls']:
                            if len(url['expanded_url']) <= 2048:
                                title = None if 'title' not in url else url['title']
                                desc = None if 'description' not in url else url['description']
                                links.append((item['id'], url['expanded_url'], title, desc))

                if 'referenced_tweets' in item:
                    for ref in item['referenced_tweets']:
                        refs.append((item['id'], ref['id'], ref['type']))

                head.append((item['id'], item['author_id'], item['text'], item['possibly_sensitive'], item['lang'],
                             item['source'], item['public_metrics']['retweet_count'],
                             item['public_metrics']['reply_count'], item['public_metrics']['like_count'],
                             item['public_metrics']['quote_count'], item['created_at']))

            #break
            cursor = conn.cursor()
            with cursor.copy("COPY conversations (id, author_id, content, possibly_sensitive, language, source, "
                             + "retweet_count, reply_count, like_count, quote_count, created_at) FROM STDIN") as copy:
                for record in head:
                    copy.write_row(record)
            with cursor.copy("COPY annotations (conversation_id, value, type, probability) FROM STDIN") as copy:
                for record in annotations:
                    copy.write_row(record)
            with cursor.copy("COPY links (conversation_id, url, title, description) FROM STDIN") as copy:
                for record in links:
                    copy.write_row(record)
            with cursor.copy("COPY conversation_references (conversation_id, parent_id, type) FROM STDIN") as copy:
                for record in refs:
                    copy.write_row(record)
            with cursor.copy("COPY context_annotations (conversation_id, context_domain_id, context_entity_id) FROM STDIN") as copy:
                for record in context:
                    copy.write_row(record)
            with cursor.copy("COPY context_domains (id, name, description) FROM STDIN") as copy:
                for record in domains:
                    copy.write_row(record)
            with cursor.copy("COPY context_entities (id, name, description) FROM STDIN") as copy:
                for record in entities:
                    copy.write_row(record)
            with cursor.copy("COPY conversation_hashtags (conversation_id, tag) FROM STDIN") as copy:
                for record in contags:
                    copy.write_row(record)
            conn.commit()
            cursor.close()
            timer(blockstart)
            count = count + 1
            if len(head) != ROWCOUNT:
                break
            del head
            del annotations
            del links
            del refs
            del contags
            #break
    conn.close()


def timer(blockstart):
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    minutes, seconds = divmod(int((datetime.now()-START).total_seconds()), 60)
    minutes2, seconds2 = divmod(int((datetime.now()-blockstart).total_seconds()), 60)
    print(now + ';' + str(minutes) + ':' + str(seconds) + ';' + str(minutes2) + ':' + str(seconds2))


if __name__ == '__main__':
    create_tables()
    load_author()
    load_convo()
    fix_queries()
