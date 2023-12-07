import logging
from datetime import datetime, timedelta

import scrapy


class OpenAIForumSpider(scrapy.Spider):
    name = 'openai_forum'
    base_url = 'https://community.openai.com/latest.json'
    url_template = "https://community.openai.com/latest.json?no_definitions=false&page={}"

    def __init__(self, days=7, *args, **kwargs):
        super(OpenAIForumSpider, self).__init__(*args, **kwargs)
        self.days = int(days)
        self.start_urls = [self.url_template.format(1)]
        self.number_of_days_ago = datetime.utcnow() - timedelta(days=self.days)
        logging.debug("=================")
        logging.info(f"OpenAIForumSpider is initiated to scrap last {days} days of topics...")

    def parse(self, response):
        data = response.json()
        last_topic_date = None

        # Process the items here
        for item in data['topic_list']['topics']:
            created_at = datetime.fromisoformat(item['created_at'].rstrip('Z'))
            if created_at > self.number_of_days_ago:
                yield {
                    'title': item['title'],
                    'created_at': item['created_at'],
                    # Additional fields can be added as needed
                }
                last_topic_date = created_at

        # Handling pagination
        if last_topic_date and last_topic_date > self.number_of_days_ago:
            if page := self.get_next_page_number(data["topic_list"]["more_topics_url"]):
                yield scrapy.Request(self.url_template.format(page), callback=self.parse)

    def get_next_page_number(self, more_topics_url) -> str | None:
        parts = more_topics_url.split("page=")
        if len(parts) > 1:
            return parts[1]
