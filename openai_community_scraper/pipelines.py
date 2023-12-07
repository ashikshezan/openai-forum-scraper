# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import json
import logging

# useful for handling different item types with a single interface
import psycopg2
from itemadapter import ItemAdapter
from scrapy.exceptions import NotConfigured


class OpenaiCommunityScraperPipeline:
    def process_item(self, item, spider):
        return item


class PostgresPipeline:

    def __init__(self, postgres_uri, postgres_user, postgres_pass, postgres_db):
        self.postgres_uri = postgres_uri
        self.postgres_user = postgres_user
        self.postgres_pass = postgres_pass
        self.postgres_db = postgres_db

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.get('POSTGRES_URI') or not crawler.settings.get('POSTGRES_USER') or not crawler.settings.get('POSTGRES_PASS') or not crawler.settings.get('POSTGRES_DB'):
            logging.error("Postgres is not configured.")
            raise NotConfigured
        return cls(
            postgres_uri=crawler.settings.get('POSTGRES_URI'),
            postgres_user=crawler.settings.get('POSTGRES_USER'),
            postgres_pass=crawler.settings.get('POSTGRES_PASS'),
            postgres_db=crawler.settings.get('POSTGRES_DB')
        )

    def open_spider(self, spider):
        self.connection = psycopg2.connect(host=self.postgres_uri, user=self.postgres_user,
                                           password=self.postgres_pass, dbname=self.postgres_db)
        self.cursor = self.connection.cursor()

        # Create table with a complete schema
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
            -- Add any additional columns as needed
        );
        """
        try:
            self.cursor.execute(create_table_query)
            self.connection.commit()
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            self.connection.rollback()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        item_dict = dict(item) if isinstance(item, dict) else item.__dict__
        columns = ['id', 'post_comments', 'tags', 'tags_descriptions', 'title', 'posts_count',
                   'created_at', 'views', 'reply_count', 'like_count', 'last_posted_at',
                   'visible', 'closed', 'archived', 'archetype', 'slug', 'word_count',
                   'deleted_at', 'user_id', 'featured_link', 'image_url', 'current_post_number',
                   'highest_post_number', 'participant_count', 'thumbnails', 'vote_count']
        converted_values = []
        for key in columns:
            value = item_dict.get(key)

            if key == 'tags' and isinstance(value, list):
                # Convert list to PostgreSQL array format
                array_literal = "{" + ",".join(f'"{str(v)}"' for v in value) + "}"
                converted_values.append(array_literal)
            elif isinstance(value, (dict, list)):
                converted_values.append(json.dumps(value))
            elif value is None:
                converted_values.append(None)
            else:
                converted_values.append(value)

        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO topic_details ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            self.cursor.execute(insert_query, tuple(converted_values))
            self.connection.commit()
        except Exception as e:
            logging.error(f"Error inserting item: {e}")
            self.connection.rollback()

        return item
