from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TopicDetail:
    id: str
    title: Optional[str] = None
    created_at: Optional[str] = None
    views: Optional[int] = None
    reply_count: Optional[int] = None
    like_count: Optional[int] = None
    vote_count: Optional[int] = None
    participant_count: Optional[int] = None
    word_count: Optional[int] = None
    posts_count: Optional[int] = None
    tags: Optional[List[str]] = field(default_factory=list)
    tags_descriptions: Optional[Dict[str, str]] = field(default_factory=dict)
    last_posted_at: Optional[str] = None
    visible: Optional[bool] = None
    closed: Optional[bool] = None
    archived: Optional[bool] = None
    archetype: Optional[str] = None
    slug: Optional[str] = None
    deleted_at: Optional[str] = None
    user_id: Optional[int] = None
    featured_link: Optional[str] = None
    image_url: Optional[str] = None
    current_post_number: Optional[int] = None
    highest_post_number: Optional[int] = None
    thumbnails: Optional[str] = None
    post_comments: Optional[dict] = None
