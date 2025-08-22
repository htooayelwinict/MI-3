#!/usr/bin/env python3
"""
MI-3 News Scraper - RSS/Atom Feed Worker
Async RSS feed polling with deduplication and normalization.
"""

import asyncio
import json
import hashlib
import logging
import random
import socket
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Set, Optional, Any
from pathlib import Path
from dataclasses import dataclass, asdict
import aiohttp
import feedparser
from dateutil import parser as date_parser
import yaml
from urllib.parse import urlparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class RawItem:
    """Normalized schema for news items from any source"""
    id: str  # Hash-based unique identifier
    title: str
    link: str
    published: str  # ISO format datetime string
    source: str  # Feed name/source
    publisher: str  # Publisher name
    summary: Optional[str] = None
    category: Optional[str] = None
    
    def __post_init__(self):
        """Generate unique ID after initialization"""
        if not self.id:
            self.id = self.generate_id()
    
    def generate_id(self) -> str:
        """Generate unique ID from title + link + published"""
        content = f"{self.title}{self.link}{self.published}"
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)

@dataclass
class FeedBackoff:
    """Per-host backoff tracking"""
    host: str
    next_attempt: datetime
    attempt_count: int = 0
    base_delay: float = 1.0
    max_delay: float = 300.0  # 5 minutes max
    
    def should_retry(self) -> bool:
        """Check if we should retry this host"""
        return datetime.now(timezone.utc) >= self.next_attempt
    
    def calculate_backoff(self, retry_after: Optional[int] = None) -> float:
        """Calculate next backoff delay with exponential backoff + jitter"""
        if retry_after:
            # Respect Retry-After header
            delay = retry_after
        else:
            # Exponential backoff with jitter
            delay = min(self.base_delay * (2 ** self.attempt_count), self.max_delay)
            # Add random jitter (±25%)
            jitter = delay * 0.25 * (random.random() * 2 - 1)
            delay += jitter
        
        self.attempt_count += 1
        self.next_attempt = datetime.now(timezone.utc) + timedelta(seconds=delay)
        logger.info(f"Host {self.host}: backing off for {delay:.1f}s (attempt #{self.attempt_count})")
        return delay
    
    def reset(self):
        """Reset backoff on successful request"""
        self.attempt_count = 0
        self.next_attempt = datetime.now(timezone.utc)

class FeedProcessor:
    """Processes RSS/Atom feeds with deduplication"""
    
    def __init__(self, sources_file: str, output_file: str, max_items: int = 1000):
        self.sources_file = Path(sources_file)
        self.output_file = Path(output_file)
        self.max_items = max_items
        self.seen_ids: Set[str] = set()
        self.items_cache: List[RawItem] = []
        
        # Per-host backoff tracking
        self.host_backoffs: Dict[str, FeedBackoff] = {}
        
        # ETag and Last-Modified support
        self.feed_metadata: Dict[str, Dict[str, str]] = {}
        
        # Ensure output directory exists
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing items on startup
        self._load_existing_items()
        self._load_feed_metadata()
    
    def _load_existing_items(self):
        """Load existing items from output file"""
        if self.output_file.exists():
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                items_data = data.get('items', [])
                self.items_cache = [
                    RawItem(**item) for item in items_data
                ]
                self.seen_ids = {item.id for item in self.items_cache}
                
                logger.info(f"Loaded {len(self.items_cache)} existing items")
                
            except Exception as e:
                logger.error(f"Error loading existing items: {e}")
                self.items_cache = []
                self.seen_ids = set()
    
    def _load_feed_metadata(self):
        """Load ETag and Last-Modified data for conditional requests"""
        metadata_file = self.output_file.with_suffix('.metadata.json')
        if metadata_file.exists():
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    self.feed_metadata = json.load(f)
                logger.info(f"Loaded feed metadata for {len(self.feed_metadata)} feeds")
            except Exception as e:
                logger.error(f"Error loading feed metadata: {e}")
                self.feed_metadata = {}
    
    def _save_feed_metadata(self):
        """Save ETag and Last-Modified data"""
        metadata_file = self.output_file.with_suffix('.metadata.json')
        try:
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.feed_metadata, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving feed metadata: {e}")
    
    def _get_host_backoff(self, url: str) -> FeedBackoff:
        """Get or create backoff tracker for host"""
        parsed = urlparse(url)
        host = parsed.netloc
        
        if host not in self.host_backoffs:
            self.host_backoffs[host] = FeedBackoff(
                host=host,
                next_attempt=datetime.now(timezone.utc)
            )
        
        return self.host_backoffs[host]
    
    def _create_session(self) -> aiohttp.ClientSession:
        """Create aiohttp session with proper configuration"""
        # Browser-like headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/rss+xml, application/rdf+xml, application/atom+xml, application/xml, text/xml, */*;q=0.1',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        # Force IPv4 connector with trust_env for better DNS resolution
        connector = aiohttp.TCPConnector(
            family=socket.AF_INET,  # Force IPv4 
            enable_cleanup_closed=True,
            limit=100,
            limit_per_host=10,
            ttl_dns_cache=300,
            use_dns_cache=True
        )
        
        # Create session with timeout and trust environment
        timeout = aiohttp.ClientTimeout(total=45, connect=15, sock_read=30)
        
        return aiohttp.ClientSession(
            headers=headers,
            connector=connector,
            timeout=timeout,
            trust_env=True  # Use environment proxy settings
        )
    
    def _load_feed_sources(self) -> List[Dict[str, Any]]:
        """Load RSS feed sources from YAML file"""
        if not self.sources_file.exists():
            logger.error(f"Sources file not found: {self.sources_file}")
            return []
        
        try:
            with open(self.sources_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                return data.get('feeds', [])
        except Exception as e:
            logger.error(f"Error loading feed sources: {e}")
            return []
    
    async def fetch_feed(self, session: aiohttp.ClientSession, feed_config: Dict[str, Any]) -> List[RawItem]:
        """Fetch and parse a single RSS/Atom feed with retry logic and backoff"""
        url = feed_config['url']
        name = feed_config['name']
        category = feed_config.get('category', 'news')
        
        # Check if host is in backoff
        backoff = self._get_host_backoff(url)
        if not backoff.should_retry():
            logger.info(f"Skipping {name} due to backoff until {backoff.next_attempt}")
            return []
        
        # Add small random initial delay to spread out requests
        initial_delay = random.uniform(0.5, 3.0)
        await asyncio.sleep(initial_delay)
        
        try:
            logger.info(f"Fetching feed: {name}")
            
            # Prepare conditional request headers
            request_headers = {}
            feed_meta = self.feed_metadata.get(url, {})
            
            if 'etag' in feed_meta:
                request_headers['If-None-Match'] = feed_meta['etag']
            if 'last_modified' in feed_meta:
                request_headers['If-Modified-Since'] = feed_meta['last_modified']
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    async with session.get(url, headers=request_headers) as response:
                        # Handle different HTTP status codes
                        if response.status == 304:
                            # Not modified, reset backoff and return empty
                            logger.info(f"Feed {name} not modified (304)")
                            backoff.reset()
                            return []
                        
                        elif response.status == 200:
                            # Success! Process the feed
                            content = await response.text()
                            
                            # Store ETag and Last-Modified for next request
                            self.feed_metadata[url] = {}
                            if 'ETag' in response.headers:
                                self.feed_metadata[url]['etag'] = response.headers['ETag']
                            if 'Last-Modified' in response.headers:
                                self.feed_metadata[url]['last_modified'] = response.headers['Last-Modified']
                            
                            # Reset backoff on success
                            backoff.reset()
                            
                            # Parse with feedparser
                            feed = feedparser.parse(content)
                            
                            if not feed.entries:
                                logger.warning(f"No entries found in feed: {name}")
                                return []
                            
                            items = []
                            publisher = feed.feed.get('title', name)
                            
                            for entry in feed.entries[:50]:  # Limit per feed
                                try:
                                    # Normalize published date
                                    published_dt = self._parse_published_date(entry)
                                    
                                    # Create RawItem
                                    item = RawItem(
                                        id="",  # Will be generated in __post_init__
                                        title=entry.get('title', 'No Title').strip(),
                                        link=entry.get('link', ''),
                                        published=published_dt.isoformat(),
                                        source=name,
                                        publisher=publisher,
                                        summary=entry.get('summary', ''),
                                        category=category
                                    )
                                    
                                    # Skip if we've seen this item before
                                    if item.id in self.seen_ids:
                                        continue
                                    
                                    items.append(item)
                                    self.seen_ids.add(item.id)
                                    
                                except Exception as e:
                                    logger.error(f"Error processing entry from {name}: {e}")
                                    continue
                            
                            logger.info(f"Processed {len(items)} new items from {name}")
                            return items
                        
                        elif response.status == 429:
                            # Rate limited - check for Retry-After header
                            retry_after = response.headers.get('Retry-After')
                            if retry_after and retry_after.isdigit():
                                backoff_delay = backoff.calculate_backoff(int(retry_after))
                            else:
                                backoff_delay = backoff.calculate_backoff()
                            
                            logger.warning(f"Rate limited (429) for {name}. Backing off for {backoff_delay:.1f}s")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(min(backoff_delay, 60))  # Cap retry delay to 1 minute
                                continue
                            else:
                                return []
                        
                        elif response.status == 403:
                            # Forbidden - implement backoff and retry
                            backoff_delay = backoff.calculate_backoff()
                            logger.warning(f"Forbidden (403) for {name}. Backing off for {backoff_delay:.1f}s")
                            return []
                        
                        elif response.status in [503, 502, 504]:
                            # Service unavailable - check for Retry-After
                            retry_after = response.headers.get('Retry-After')
                            if retry_after and retry_after.isdigit():
                                backoff_delay = backoff.calculate_backoff(int(retry_after))
                            else:
                                backoff_delay = backoff.calculate_backoff()
                            
                            logger.warning(f"Service unavailable ({response.status}) for {name}. Backing off for {backoff_delay:.1f}s")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(min(backoff_delay, 60))
                                continue
                            else:
                                return []
                        
                        else:
                            # Other HTTP errors
                            logger.warning(f"HTTP {response.status} for {name}: {url}")
                            if attempt < max_retries - 1 and response.status >= 500:
                                # Retry on server errors
                                backoff_delay = backoff.calculate_backoff()
                                await asyncio.sleep(min(backoff_delay, 30))
                                continue
                            else:
                                return []
                
                except aiohttp.ClientError as e:
                    # DNS/SSL/Connection errors
                    if "nodename nor servname provided" in str(e) or "SSL" in str(e):
                        logger.error(f"DNS/SSL error for {name}: {e}")
                        backoff_delay = backoff.calculate_backoff()
                        return []
                    
                    logger.warning(f"Client error for {name} (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        backoff_delay = backoff.calculate_backoff()
                        await asyncio.sleep(min(backoff_delay, 30))
                        continue
                    else:
                        backoff.calculate_backoff()
                        return []
                
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout for {name} (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(5 * (attempt + 1))  # Progressive delay
                        continue
                    else:
                        backoff.calculate_backoff()
                        return []
            
            return []
            
        except Exception as e:
            logger.error(f"Unexpected error fetching feed {name}: {e}")
            backoff.calculate_backoff()
            return []
    
    def _parse_published_date(self, entry) -> datetime:
        """Parse published date from feed entry"""
        # Try different date fields
        date_fields = ['published_parsed', 'updated_parsed', 'created_parsed']
        
        for field in date_fields:
            if hasattr(entry, field) and getattr(entry, field):
                time_struct = getattr(entry, field)
                try:
                    return datetime(*time_struct[:6], tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    continue
        
        # Try string date fields
        string_fields = ['published', 'updated', 'created']
        for field in string_fields:
            if hasattr(entry, field) and getattr(entry, field):
                try:
                    return date_parser.parse(getattr(entry, field))
                except Exception:
                    continue
        
        # Default to now if no date found
        return datetime.now(timezone.utc)
    
    async def poll_all_feeds(self) -> List[RawItem]:
        """Poll all configured feeds"""
        sources = self._load_feed_sources()
        if not sources:
            logger.warning("No feed sources configured")
            return []
        
        new_items = []
        
        # Use enhanced session with proper headers and IPv4 connector
        async with self._create_session() as session:
            tasks = [self.fetch_feed(session, feed_config) for feed_config in sources]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Feed fetch failed: {result}")
                else:
                    new_items.extend(result)
        
        # Add new items to cache
        self.items_cache.extend(new_items)
        
        # Sort by published date (most recent first) and limit
        self.items_cache.sort(key=lambda x: x.published, reverse=True)
        self.items_cache = self.items_cache[:self.max_items]
        
        # Update seen_ids to match current cache
        self.seen_ids = {item.id for item in self.items_cache}
        
        # Save to file and metadata
        self._save_items()
        self._save_feed_metadata()
        
        return new_items
    
    def _save_items(self):
        """Save current items to JSON file"""
        try:
            data = {
                'last_updated': datetime.now(timezone.utc).isoformat(),
                'total_items': len(self.items_cache),
                'items': [item.to_dict() for item in self.items_cache]
            }
            
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Saved {len(self.items_cache)} items to {self.output_file}")
            
        except Exception as e:
            logger.error(f"Error saving items: {e}")
    
    def get_latest_items(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get latest items as dictionaries"""
        return [item.to_dict() for item in self.items_cache[:limit]]

async def main():
    """Main polling loop"""
    from config.settings import settings
    
    if not settings.realtime.enabled:
        logger.info("Realtime feeds disabled in config")
        return
    
    processor = FeedProcessor(
        sources_file=settings.realtime.sources_file,
        output_file=settings.realtime.feed_data_file,
        max_items=settings.realtime.max_items_per_feed * 10  # Allow for multiple feeds
    )
    
    logger.info("Starting RSS feed polling worker")
    logger.info(f"Poll interval: {settings.realtime.poll_interval} seconds")
    
    while True:
        try:
            start_time = datetime.now()
            new_items = await processor.poll_all_feeds()
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Poll completed in {duration:.1f}s, {len(new_items)} new items")
            
            # Wait for next poll
            await asyncio.sleep(settings.realtime.poll_interval)
            
        except KeyboardInterrupt:
            logger.info("Shutting down feed worker")
            break
        except Exception as e:
            logger.error(f"Error in polling loop: {e}")
            await asyncio.sleep(60)  # Wait 1 minute on error

if __name__ == "__main__":
    asyncio.run(main())