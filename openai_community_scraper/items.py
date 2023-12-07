from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TopicDetail:
    id: int
    post_comments: Optional[dict] = None
    tags: Optional[List[str]] = field(default_factory=list)
    tags_descriptions: Optional[Dict[str, str]] = field(default_factory=dict)
    title: Optional[str] = None
    posts_count: Optional[int] = None
    created_at: Optional[str] = None
    views: Optional[int] = None
    reply_count: Optional[int] = None
    like_count: Optional[int] = None
    last_posted_at: Optional[str] = None
    visible: Optional[bool] = None
    closed: Optional[bool] = None
    archived: Optional[bool] = None
    archetype: Optional[str] = None
    slug: Optional[str] = None
    word_count: Optional[int] = None
    deleted_at: Optional[str] = None
    user_id: Optional[int] = None
    featured_link: Optional[str] = None
    image_url: Optional[str] = None
    current_post_number: Optional[int] = None
    highest_post_number: Optional[int] = None
    participant_count: Optional[int] = None
    thumbnails: Optional[str] = None
    vote_count: Optional[int] = None
