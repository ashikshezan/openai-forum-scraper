import logging
from datetime import datetime, timedelta

import scrapy

from openai_community_scraper.items import TopicDetail


class OpenAIForumSpider(scrapy.Spider):
    """
    A Scrapy Spider for scraping topics from the OpenAI Community Forum.

    Attributes:
        name (str): Name of the spider.
        base_url (str): Base URL for the OpenAI Community Forum.
        topic_listing_url_template (str): Template URL for topic listing pages.
        topic_details_url_template (str): Template URL for topic detail pages.
        days (int): Number of past days to scrape.
        number_of_days_ago (datetime): The datetime object representing the starting point for scraping.
    """

    name = 'openai_forum'
    base_url = 'https://community.openai.com/latest.json'
    topic_listing_url_template = "https://community.openai.com/latest.json?no_definitions=false&page={}"
    topic_details_url_template = "https://community.openai.com/t/{}.json?track_visit=true&forceLoad=true"

    # the modification of this classmethod enable setting the CLI command accordingly to the spider settings
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """
        Class method to create spider instance with command line arguments for output method and days.

        Args:
            crawler: The crawler instance.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            Instance of OpenAIForumSpider.
        """
        output_method = kwargs.pop('output_method', 'json')
        days = kwargs.pop('days', 7)

        spider = super(OpenAIForumSpider, cls).from_crawler(crawler, *args, **kwargs)
        spider.days = int(days)
        spider.start_urls = [spider.topic_listing_url_template.format(1)]
        spider.number_of_days_ago = datetime.utcnow() - timedelta(days=spider.days)
        logging.info(f"OpenAIForumSpider is initiated to scrap last {spider.days} days of topics...")

        # Setting pipeline based on output method
        if output_method == 'json':
            crawler.settings.setdict(
                {'ITEM_PIPELINES': {'openai_community_scraper.pipelines.JsonPipeline': 300}}, priority='cmdline')
        elif output_method == 'postgres':
            crawler.settings.setdict(
                {'ITEM_PIPELINES': {'openai_community_scraper.pipelines.PostgresPipeline': 300}}, priority='cmdline')

        return spider

    def parse(self, response):
        """
        Default callback used by Scrapy to process downloaded responses, when their requests don't specify a callback.

        Args:
            response: The response object to be processed.
        """
        data = response.json()
        last_topic_date = None

        # Process and yield requests for topic details
        for item in data['topic_list']['topics']:
            created_at = datetime.fromisoformat(item['created_at'].rstrip('Z'))
            if created_at > self.number_of_days_ago:
                topic_id = item["id"]
                topic_detail_url = self.topic_details_url_template.format(topic_id)
                request = scrapy.Request(topic_detail_url, callback=self.parse_topic_detail)
                request.meta['topic_data'] = item  # Pass topic data to detail parser
                yield request
                last_topic_date = created_at

        # Handling pagination if more topics are available within the date range
        if last_topic_date and last_topic_date > self.number_of_days_ago:
            if page := self.get_next_page_number(data["topic_list"]["more_topics_url"]):
                yield scrapy.Request(self.topic_listing_url_template.format(page), callback=self.parse)

    def parse_topic_detail(self, response):
        """
        Callback function to process the topic details page.

        Args:
            response: The response object with topic details.
        """
        # Extracting topic details and yielding TopicDetail items
        topic_data = response.json()
        topic_detail_example = TopicDetail(
            id=topic_data["id"],
            title=topic_data["title"],
            created_at=topic_data["created_at"],
            views=topic_data["views"],
            reply_count=topic_data["reply_count"],
            like_count=topic_data["like_count"],
            posts_count=topic_data["posts_count"],
            vote_count=topic_data["vote_count"],
            word_count=topic_data["word_count"],
            tags=topic_data["tags"],
            tags_descriptions=topic_data["tags_descriptions"],
            last_posted_at=topic_data["last_posted_at"],
            visible=topic_data["visible"],
            closed=topic_data["closed"],
            archived=topic_data["archived"],
            archetype=topic_data["archetype"],
            slug=topic_data["slug"],
            deleted_at=topic_data.get("deleted_at"),
            user_id=topic_data["user_id"],
            featured_link=topic_data.get("featured_link"),
            image_url=topic_data.get("image_url"),
            current_post_number=topic_data["current_post_number"],
            highest_post_number=topic_data["highest_post_number"],
            participant_count=topic_data["participant_count"],
            thumbnails=topic_data.get("thumbnails"),
            post_comments=topic_data["post_stream"]["posts"],
        )
        yield topic_detail_example

    def get_next_page_number(self, more_topics_url: str) -> str | None:
        """
        Extracts the next page number from the pagination URL.

        Args:
            more_topics_url (str): The URL containing the pagination information.

        Returns:
            The next page number as a string, or None if not found.
        """
        parts = more_topics_url.split("page=")
        if len(parts) > 1:
            return parts[1]
