#!/usr/bin/env python3
"""
MI-3 News Scraper - Adaptive RSS/Atom Feed Worker
Fast adaptive polling with per-host scheduling, conditional GET, and exponential backoff.
"""

import asyncio
import json
import hashlib
import logging
import random
import socket
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Set, Optional, Any
from pathlib import Path
from dataclasses import dataclass, asdict
from urllib.parse import urlparse
import aiohttp
import feedparser
from dateutil import parser as date_parser

from config.settings import settings

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

@dataclass
class HostState:
    """Per-host polling state for adaptive scheduling"""
    next_due: float  # epoch seconds when next poll is due
    interval: float  # current polling interval in seconds
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    consecutive_ok: int = 0
    consecutive_fail: int = 0
    last_status: Optional[int] = None
    backoff_logged: bool = False  # to suppress repeat backoff messages

class AdaptiveFeedWorker:
    """
    Adaptive RSS feed worker with per-host scheduling.
    
    Features:
    - Per-host adaptive intervals (60s baseline, 30s-900s range)
    - Conditional GET (ETag/Last-Modified support)
    - Exponential backoff on errors/429/403
    - Jitter and staggered startup
    - Respectful rate limiting with guardrails
    """
    
    def __init__(self):
        self.settings = settings.realtime
        self.running = False
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Per-host state tracking
        self.host_states: Dict[str, HostState] = {}
        
        # Feed sources grouped by host
        self.feeds_by_host: Dict[str, List[Dict[str, Any]]] = {}
        
        # Global deduplication
        self.seen_items: Set[str] = set()
        self.last_cleanup = time.time()
        
        # Statistics
        self.stats = {
            "total_fetches": 0,
            "total_new_items": 0,
            "total_errors": 0,
            "last_updated": None
        }
        
        logger.info(f"Adaptive feed worker initialized with {self.settings.poll_baseline_seconds}s baseline")

    async def initialize(self):
        """Initialize the worker"""
        self.running = True
        
        # Create aiohttp session with browser-like headers
        connector = aiohttp.TCPConnector(
            limit=20,  # Total connection limit
            limit_per_host=5,  # Per-host connection limit
            ttl_dns_cache=300,
            use_dns_cache=True
        )
        
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache'
            }
        )
        
        # Load feed sources and group by host
        await self._load_feeds()
        
        # Initialize host states with staggered startup
        await self._initialize_host_states()
        
        logger.info(f"Initialized adaptive worker with {len(self.host_states)} hosts")

    async def _load_feeds(self):
        """Load and group feeds by hostname"""
        from config.settings import load_feed_sources
        
        feeds = load_feed_sources(self.settings.sources_file)
        self.feeds_by_host.clear()
        
        for feed in feeds:
            try:
                parsed = urlparse(feed['url'])
                host = parsed.netloc.lower()
                
                if host not in self.feeds_by_host:
                    self.feeds_by_host[host] = []
                
                self.feeds_by_host[host].append(feed)
                
            except Exception as e:
                logger.warning(f"Invalid feed URL {feed.get('url', 'unknown')}: {e}")
        
        logger.info(f"Loaded {sum(len(feeds) for feeds in self.feeds_by_host.values())} feeds across {len(self.feeds_by_host)} hosts")

    async def _initialize_host_states(self):
        """Initialize per-host states with staggered startup"""
        now = time.time()
        
        for host in self.feeds_by_host:
            # Random startup delay to avoid thundering herd
            startup_delay = random.uniform(*self.settings.stagger_startup_seconds)
            
            self.host_states[host] = HostState(
                next_due=now + startup_delay,
                interval=float(self.settings.poll_baseline_seconds)
            )
            
            logger.info(f"Host {host}: initialized with {startup_delay:.1f}s startup delay")

    async def run(self):
        """Main adaptive polling loop"""
        await self.initialize()
        
        logger.info("Starting adaptive polling loop (1s tick interval)")
        
        try:
            while self.running:
                await self._tick()
                await asyncio.sleep(1)  # 1 second tick interval
                
        except Exception as e:
            logger.error(f"Fatal error in polling loop: {e}")
            raise
        finally:
            await self.cleanup()

    async def _tick(self):
        """Process one tick of the polling loop"""
        now = time.time()
        
        # Find hosts that are due for polling
        due_hosts = [
            host for host, state in self.host_states.items()
            if now >= state.next_due
        ]
        
        if due_hosts:
            # Process due hosts concurrently
            tasks = [self._poll_host(host, now) for host in due_hosts]
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Periodic cleanup
        if now - self.last_cleanup > 3600:  # Every hour
            await self._cleanup_seen_items()
            self.last_cleanup = now

    async def _poll_host(self, host: str, now: float):
        """Poll all feeds for a specific host"""
        state = self.host_states[host]
        feeds = self.feeds_by_host[host]
        
        try:
            # Fetch all feeds for this host concurrently
            tasks = [self._fetch_feed(feed, state) for feed in feeds]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            total_new_items = 0
            status_codes = []
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.warning(f"Feed {feeds[i]['name']}: fetch error: {result}")
                    status_codes.append(None)
                else:
                    status, new_items = result
                    status_codes.append(status)
                    total_new_items += new_items
            
            # Update host state based on results
            await self._update_host_state(host, status_codes, total_new_items, now)
            
        except Exception as e:
            logger.error(f"Host {host}: polling error: {e}")
            await self._handle_host_error(host, now)

    async def _fetch_feed(self, feed: Dict[str, Any], state: HostState) -> tuple[Optional[int], int]:
        """Fetch a single RSS feed with conditional GET support"""
        url = feed['url']
        
        headers = {}
        
        # Add conditional GET headers
        if state.etag:
            headers['If-None-Match'] = state.etag
        if state.last_modified:
            headers['If-Modified-Since'] = state.last_modified
        
        # Add referer for politeness
        try:
            parsed = urlparse(url)
            headers['Referer'] = f"{parsed.scheme}://{parsed.netloc}/"
        except:
            pass
        
        try:
            async with self.session.get(url, headers=headers) as response:
                self.stats["total_fetches"] += 1
                
                # Handle status codes
                if response.status == 304:
                    # Not modified
                    return 304, 0
                
                elif response.status == 200:
                    # Success - parse content
                    content = await response.text()
                    
                    # Update caching headers
                    state.etag = response.headers.get('ETag')
                    state.last_modified = response.headers.get('Last-Modified')
                    
                    # Parse feed
                    new_items = await self._parse_feed_content(feed, content)
                    return 200, new_items
                
                elif response.status == 429:
                    # Rate limited
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        try:
                            # Override interval with Retry-After value
                            retry_seconds = int(retry_after)
                            state.interval = max(retry_seconds, state.interval)
                            logger.info(f"Host {urlparse(url).netloc}: Retry-After {retry_seconds}s")
                        except ValueError:
                            pass
                    
                    return 429, 0
                
                elif response.status in [403, 404]:
                    # Forbidden or not found
                    return response.status, 0
                
                elif response.status >= 500:
                    # Server error
                    return response.status, 0
                
                else:
                    # Other status
                    return response.status, 0
                    
        except asyncio.TimeoutError:
            logger.warning(f"Feed {feed['name']}: timeout")
            return None, 0
        except aiohttp.ClientError as e:
            logger.warning(f"Feed {feed['name']}: client error: {e}")
            return None, 0
        except Exception as e:
            logger.warning(f"Feed {feed['name']}: unexpected error: {e}")
            return None, 0

    async def _parse_feed_content(self, feed: Dict[str, Any], content: str) -> int:
        """Parse RSS/Atom content and extract new items"""
        try:
            parsed = feedparser.parse(content)
            
            if parsed.bozo and parsed.bozo_exception:
                logger.warning(f"Feed {feed['name']}: parsing warning: {parsed.bozo_exception}")
            
            new_items = []
            
            for entry in parsed.entries[:self.settings.max_items_per_feed]:
                try:
                    # Extract item data
                    item = await self._extract_item_data(feed, entry, parsed.feed)
                    
                    # Check for duplicates
                    if item.id not in self.seen_items:
                        new_items.append(item)
                        self.seen_items.add(item.id)
                        
                except Exception as e:
                    logger.warning(f"Feed {feed['name']}: error parsing entry: {e}")
                    continue
            
            if new_items:
                await self._save_items(new_items)
                self.stats["total_new_items"] += len(new_items)
                
                logger.debug(f"Feed {feed['name']}: parsed {len(new_items)} new items")
            
            return len(new_items)
            
        except Exception as e:
            logger.error(f"Feed {feed['name']}: parsing error: {e}")
            return 0

    async def _extract_item_data(self, feed: Dict[str, Any], entry, feed_info) -> RawItem:
        """Extract normalized item data from feed entry"""
        # Extract basic fields
        title = entry.get('title', 'No title').strip()
        link = entry.get('link', '').strip()
        
        # Parse publication date
        published_str = entry.get('published', entry.get('updated', ''))
        if published_str:
            try:
                published_dt = date_parser.parse(published_str)
                published = published_dt.isoformat()
            except:
                published = datetime.now(timezone.utc).isoformat()
        else:
            published = datetime.now(timezone.utc).isoformat()
        
        # Extract summary
        summary = entry.get('summary', entry.get('description', ''))
        if summary:
            summary = summary.strip()[:500]  # Limit summary length
        
        # Extract category
        category = feed.get('category', 'general')
        if hasattr(entry, 'tags') and entry.tags:
            category = entry.tags[0].get('term', category)
        
        # Publisher name
        publisher = feed_info.get('title', feed.get('name', urlparse(feed['url']).netloc))
        
        return RawItem(
            id='',  # Will be generated in __post_init__
            title=title,
            link=link,
            published=published,
            source=feed['name'],
            publisher=publisher,
            summary=summary,
            category=category
        )

    async def _update_host_state(self, host: str, status_codes: List[Optional[int]], new_items: int, now: float):
        """Update host state based on polling results"""
        state = self.host_states[host]
        
        # Determine overall result for this host
        has_success = any(code == 200 for code in status_codes if code is not None)
        has_not_modified = any(code == 304 for code in status_codes if code is not None)
        has_rate_limit = any(code == 429 for code in status_codes if code is not None)
        has_forbidden = any(code == 403 for code in status_codes if code is not None)
        has_server_error = any(code and code >= 500 for code in status_codes if code is not None)
        has_client_error = any(code is None for code in status_codes)
        
        # Update consecutive counters
        if has_success or has_not_modified:
            state.consecutive_ok += 1
            state.consecutive_fail = 0
            state.backoff_logged = False
        else:
            state.consecutive_fail += 1
            if state.consecutive_fail == 1:  # Reset consecutive_ok on first failure
                state.consecutive_ok = 0
        
        old_interval = state.interval
        
        # Adjust interval based on results
        if has_success and new_items > 0:
            # Got new items - speed up slightly
            state.interval = max(
                self.settings.poll_min_seconds,
                state.interval * 0.9
            )
            
        elif has_not_modified or (has_success and new_items == 0):
            # No new content - slow down slightly
            state.interval = min(
                state.interval * 1.1,
                2 * self.settings.poll_baseline_seconds
            )
            
        elif has_rate_limit or has_forbidden or has_server_error or has_client_error:
            # Error condition - exponential backoff
            backoff_interval = max(state.interval, self.settings.backoff_base) * self.settings.backoff_factor
            state.interval = min(backoff_interval, self.settings.poll_max_seconds)
            
            # Log backoff (once per backoff cycle)
            if not state.backoff_logged:
                error_type = "rate_limit" if has_rate_limit else "forbidden" if has_forbidden else "server_error" if has_server_error else "client_error"
                logger.warning(f"Host {host}: backoff escalate to {state.interval:.1f}s (reason={error_type})")
                state.backoff_logged = True
        
        # Add jitter and schedule next poll
        jitter = random.uniform(*self.settings.jitter_range_seconds)
        state.next_due = now + state.interval + jitter
        
        # Determine status for logging
        status_str = "304" if has_not_modified else str(max((c for c in status_codes if c), default="error"))
        
        # Log result
        next_time = datetime.fromtimestamp(state.next_due, tz=timezone.utc).isoformat()
        logger.info(f"Host {host}: status={status_str}, new={new_items}, interval={state.interval:.1f}s, next={next_time}")
        
        # Log recovery if interval decreased after backoff
        if old_interval > self.settings.poll_baseline_seconds * 2 and state.interval < old_interval:
            logger.info(f"Host {host}: recovered, interval now {state.interval:.1f}s")

    async def _handle_host_error(self, host: str, now: float):
        """Handle host-level errors"""
        state = self.host_states[host]
        state.consecutive_fail += 1
        state.consecutive_ok = 0
        
        # Apply backoff
        state.interval = min(
            max(state.interval, self.settings.backoff_base) * self.settings.backoff_factor,
            self.settings.poll_max_seconds
        )
        
        jitter = random.uniform(*self.settings.jitter_range_seconds)
        state.next_due = now + state.interval + jitter
        
        logger.error(f"Host {host}: error backoff to {state.interval:.1f}s")
        self.stats["total_errors"] += 1

    async def _save_items(self, items: List[RawItem]):
        """Save items to storage"""
        try:
            # Load existing data
            data_file = Path(self.settings.feed_data_file)
            data_file.parent.mkdir(parents=True, exist_ok=True)
            
            if data_file.exists():
                with open(data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                data = {"items": [], "last_updated": None, "total_items": 0}
            
            # Add new items to the beginning
            new_data = [asdict(item) for item in items]
            data["items"] = new_data + data["items"]
            
            # Keep only recent items (last 1000)
            data["items"] = data["items"][:1000]
            
            # Update metadata
            data["last_updated"] = datetime.now(timezone.utc).isoformat()
            data["total_items"] = len(data["items"])
            
            # Write to file atomically
            temp_file = data_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            temp_file.replace(data_file)
            
            # Update stats
            self.stats["last_updated"] = data["last_updated"]
            
        except Exception as e:
            logger.error(f"Error saving items: {e}")

    async def _cleanup_seen_items(self):
        """Cleanup old seen items to prevent memory growth"""
        # Keep only recent 10000 items
        if len(self.seen_items) > 10000:
            # Convert to list, sort, and keep recent items
            # This is a simple cleanup - in production you might want a time-based approach
            item_list = list(self.seen_items)
            self.seen_items = set(item_list[-5000:])  # Keep most recent half
            logger.info(f"Cleaned up seen items cache: {len(item_list)} -> {len(self.seen_items)}")

    async def stop(self):
        """Stop the worker gracefully"""
        logger.info("Stopping adaptive feed worker...")
        self.running = False

    async def cleanup(self):
        """Cleanup resources"""
        if self.session:
            await self.session.close()
            self.session = None
        
        logger.info("Adaptive feed worker cleanup complete")

async def main():
    """Main entry point"""
    worker = AdaptiveFeedWorker()
    
    try:
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await worker.stop()

if __name__ == "__main__":
    asyncio.run(main())
