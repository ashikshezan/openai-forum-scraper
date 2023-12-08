import dataclasses
import json
import logging

import psycopg2
import psycopg2.extras
from scrapy.exceptions import NotConfigured


class JsonPipeline:
    def open_spider(self, spider):
        self.file = open('output.json', 'w')
        self.file.write('[')  # Write opening bracket
        self.is_first_item = True

    def close_spider(self, spider):
        self.file.write(']')  # Write closing bracket
        self.file.close()

    def process_item(self, item, spider):
        # Convert dataclass instance to a dictionary
        item_dict = dataclasses.asdict(item)
        line = json.dumps(item_dict)

        # Handle comma placement
        if not self.is_first_item:
            self.file.write(',\n')
        else:
            self.is_first_item = False

        self.file.write(line)
        return item


class PostgresPipeline:
    def __init__(self, postgres_uri, postgres_user, postgres_pass, postgres_db, batch_size=100):
        self.postgres_uri = postgres_uri
        self.postgres_user = postgres_user
        self.postgres_pass = postgres_pass
        self.postgres_db = postgres_db
        self.batch_size = batch_size
        self.items_buffer = []

    @classmethod
    def from_crawler(cls, crawler):
        if not all(crawler.settings.get(key) for key in ['POSTGRES_URI', 'POSTGRES_USER', 'POSTGRES_PASS', 'POSTGRES_DB']):
            logging.error("Postgres is not configured.")
            raise NotConfigured
        return cls(
            postgres_uri=crawler.settings.get('POSTGRES_URI'),
            postgres_user=crawler.settings.get('POSTGRES_USER'),
            postgres_pass=crawler.settings.get('POSTGRES_PASS'),
            postgres_db=crawler.settings.get('POSTGRES_DB'),
            batch_size=crawler.settings.get('BATCH_SIZE', 100)
        )

    def open_spider(self, spider):
        self.connection = psycopg2.connect(
            host=self.postgres_uri,
            user=self.postgres_user,
            password=self.postgres_pass,
            dbname=self.postgres_db
        )
        self.cursor = self.connection.cursor()
        self._create_table()

    def close_spider(self, spider):
        if self.items_buffer:
            self._insert_items()
        self.connection.close()

    def process_item(self, item, spider):
        self.items_buffer.append(self._convert_item(item))
        if len(self.items_buffer) >= self.batch_size:
            self._insert_items()
        return item

    def _create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS topic_details (
            id SERIAL PRIMARY KEY,
            post_comments JSON,
            tags TEXT[],
            tags_descriptions JSON,
            title TEXT,
            posts_count INTEGER,
            created_at TIMESTAMP,
            views INTEGER,
            reply_count INTEGER,
            like_count INTEGER,
            last_posted_at TIMESTAMP,
            visible BOOLEAN,
            closed BOOLEAN,
            archived BOOLEAN,
            archetype TEXT,
            slug TEXT,
            word_count INTEGER,
            deleted_at TIMESTAMP,
            user_id INTEGER,
            featured_link TEXT,
            image_url TEXT,
            current_post_number INTEGER,
            highest_post_number INTEGER,
            participant_count INTEGER,
            thumbnails TEXT,
            vote_count INTEGER
        );
        """
        try:
            self.cursor.execute(create_table_query)
            self.connection.commit()
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            self.connection.rollback()

    def _convert_item(self, item):
        # Convert item to a dict if it's a dataclass
        item_dict = dict(item) if isinstance(item, dict) else item.__dict__
        converted_values = []
        columns = ['id', 'post_comments', 'tags', 'tags_descriptions', 'title', 'posts_count',
                   'created_at', 'views', 'reply_count', 'like_count', 'last_posted_at',
                   'visible', 'closed', 'archived', 'archetype', 'slug', 'word_count',
                   'deleted_at', 'user_id', 'featured_link', 'image_url', 'current_post_number',
                   'highest_post_number', 'participant_count', 'thumbnails', 'vote_count']
        for key in columns:
            value = item_dict.get(key)
            if key == 'tags' and isinstance(value, list):
                array_literal = "{" + ",".join(f'"{str(v)}"' for v in value) + "}"
                converted_values.append(array_literal)
            elif isinstance(value, (dict, list)):
                converted_values.append(json.dumps(value))
            elif value is None:
                converted_values.append(None)
            else:
                converted_values.append(value)
        return converted_values

    def _insert_items(self):
        if not self.items_buffer:
            return

        columns = ['id', 'post_comments', 'tags', 'tags_descriptions', 'title', 'posts_count',
                   'created_at', 'views', 'reply_count', 'like_count', 'last_posted_at',
                   'visible', 'closed', 'archived', 'archetype', 'slug', 'word_count',
                   'deleted_at', 'user_id', 'featured_link', 'image_url', 'current_post_number',
                   'highest_post_number', 'participant_count', 'thumbnails', 'vote_count']

        # The query should have a single placeholder for the entire row
        insert_query = f"INSERT INTO topic_details ({', '.join(columns)}) VALUES %s"

        # Adding ON CONFLICT clause for upsert
        on_conflict_query = f"ON CONFLICT (id) DO UPDATE SET " + \
                            ", ".join([f"{col}=EXCLUDED.{col}" for col in columns if col != 'id'])

        try:
            psycopg2.extras.execute_values(
                self.cursor, insert_query + " " + on_conflict_query,
                self.items_buffer, template=None, page_size=self.batch_size
            )
            self.connection.commit()
        except Exception as e:
            logging.error(f"Error inserting items: {e}")
            self.connection.rollback()

        self.items_buffer.clear()
