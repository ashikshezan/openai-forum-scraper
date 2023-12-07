import logging
from datetime import datetime, timedelta

import scrapy

from openai_community_scraper.items import TopicDetail


class OpenAIForumSpider(scrapy.Spider):
    name = 'openai_forum'
    base_url = 'https://community.openai.com/latest.json'
    topic_listing_url_template = "https://community.openai.com/latest.json?no_definitions=false&page={}"
    topic_details_url_template = "https://community.openai.com/t/{}.json?track_visit=true&forceLoad=true"

    def __init__(self, days=7, *args, **kwargs):
        super(OpenAIForumSpider, self).__init__(*args, **kwargs)
        self.days = int(days)
        self.start_urls = [self.topic_listing_url_template.format(1)]
        self.number_of_days_ago = datetime.utcnow() - timedelta(days=self.days)
        logging.info(f"OpenAIForumSpider is initiated to scrap last {days} days of topics...")

    def parse(self, response):
        data = response.json()
        last_topic_date = None

        # Process the items here
        for item in data['topic_list']['topics']:
            created_at = datetime.fromisoformat(item['created_at'].rstrip('Z'))
            if created_at > self.number_of_days_ago:
                topic_id = item["id"]
                topic_detail_url = self.topic_details_url_template.format(topic_id)
                request = scrapy.Request(topic_detail_url, callback=self.parse_topic_detail)
                request.meta['topic_data'] = item  # Pass topic data to detail parser
                yield request
                last_topic_date = created_at

        # Handling pagination
        if last_topic_date and last_topic_date > self.number_of_days_ago:
            if page := self.get_next_page_number(data["topic_list"]["more_topics_url"]):
                yield scrapy.Request(self.topic_listing_url_template.format(page), callback=self.parse)

    def parse_topic_detail(self, response):
        # topic_data = response.meta['topic_data']
        topic_data = response.json()

        topic_detail_example = TopicDetail(
            id=topic_data["id"],
            post_comments=topic_data["post_stream"]["posts"],
            title=topic_data["title"],
            posts_count=topic_data["posts_count"],
            created_at=topic_data["created_at"],
            views=topic_data["views"],
            reply_count=topic_data["reply_count"],
            like_count=topic_data["like_count"],
            last_posted_at=topic_data["last_posted_at"],
            visible=topic_data["visible"],
            closed=topic_data["closed"],
            archived=topic_data["archived"],
            archetype=topic_data["archetype"],
            slug=topic_data["slug"],
            word_count=topic_data["word_count"],
            deleted_at=topic_data.get("deleted_at"),
            user_id=topic_data["user_id"],
            featured_link=topic_data.get("featured_link"),
            image_url=topic_data.get("image_url"),
            current_post_number=topic_data["current_post_number"],
            highest_post_number=topic_data["highest_post_number"],
            participant_count=topic_data["participant_count"],
            thumbnails=topic_data.get("thumbnails"),
            vote_count=topic_data["vote_count"]
        )
        yield topic_detail_example

    def get_next_page_number(self, more_topics_url: str) -> str | None:
        parts = more_topics_url.split("page=")
        if len(parts) > 1:
            return parts[1]

    def get_resolved_user_data(self, data: dict, posters: list) -> list[dict]:
        resolved_users = []
        for poster in posters:
            users = data["users"]
            for user in users:
                if user["id"] == poster["user_id"]:
                    resolved_users.append(user)
                    break

        return resolved_users
