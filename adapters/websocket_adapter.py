#!/usr/bin/env python3
"""
MI-3 News Scraper - WebSocket Adapter
Generic WebSocket client with auto-reconnect, backpressure, and vendor payload mapping.
"""

import asyncio
import json
import logging
import random
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Callable
import signal

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from bus.stream import stream
from adapters.mappers import map_ws_payload_to_raw
from storage.schemas import RawItem
from config.settings import settings

logger = logging.getLogger(__name__)

class WebSocketAdapter:
    """
    Generic WebSocket client for real-time news feeds.
    
    Features:
    - Auto-reconnect with exponential backoff + jitter
    - Optional ping/pong keepalive
    - Backpressure handling with queue size limits
    - Pluggable payload mapping per vendor
    - Comprehensive logging and error recovery
    """
    
    def __init__(self, source_config: Dict[str, Any]):
        self.config = source_config
        self.name = source_config['name']
        self.url = source_config['url']
        self.topic = source_config.get('topic', 'news')
        
        # Connection settings
        self.headers = source_config.get('headers', {})
        self.ping_interval = source_config.get('ping_interval', 30)  # seconds
        self.reconnect_backoff = source_config.get('reconnect_backoff', [1, 2, 4, 8, 16, 32])
        
        # Backpressure settings
        self.max_queue_size = source_config.get('max_queue_size', settings.event_driven.max_queue_size)
        self.message_queue = deque(maxlen=self.max_queue_size)
        
        # State tracking
        self.websocket = None
        self.running = False
        self.reconnect_attempts = 0
        self.last_message_time = time.time()
        self.message_count = 0
        self.error_count = 0
        
        # Custom mapper function (can be overridden)
        self.mapper_func: Callable[[Dict[str, Any], Dict[str, Any]], Optional[RawItem]] = map_ws_payload_to_raw
        
        logger.info(f"WebSocket adapter initialized for {self.name}")
    
    async def connect_and_run(self):
        """Main connection and message processing loop"""
        self.running = True
        
        while self.running:
            try:
                await self._connect_with_backoff()
                if self.websocket:
                    await self._message_loop()
                    
            except Exception as e:
                logger.error(f"{self.name}: Unexpected error in main loop: {e}")
                self.error_count += 1
                
            if self.running:
                await self._backoff_delay()
    
    async def _connect_with_backoff(self):
        """Connect with exponential backoff"""
        if not self.running:
            return
            
        try:
            # Calculate backoff delay
            if self.reconnect_attempts > 0:
                max_backoff_idx = min(self.reconnect_attempts - 1, len(self.reconnect_backoff) - 1)
                base_delay = self.reconnect_backoff[max_backoff_idx]
                
                # Add jitter (±25%)
                jitter = base_delay * 0.25 * (random.random() * 2 - 1)
                delay = max(0.1, base_delay + jitter)
                
                logger.info(f"{self.name}: Reconnecting in {delay:.1f}s (attempt #{self.reconnect_attempts})")
                await asyncio.sleep(delay)
            
            if not self.running:
                return
                
            # Attempt connection
            logger.info(f"{self.name}: Connecting to {self.url}")
            
            # Use aiohttp for WebSocket with custom headers
            session = aiohttp.ClientSession()
            
            try:
                self.websocket = await session.ws_connect(
                    self.url,
                    headers=self.headers,
                    heartbeat=self.ping_interval,
                    timeout=aiohttp.ClientTimeout(total=30),
                    ssl=True  # Verify SSL certificates
                )
                
                logger.info(f"{self.name}: WebSocket connected successfully")
                self.reconnect_attempts = 0  # Reset on successful connection
                self.last_message_time = time.time()
                
                return  # Connection successful
                
            except Exception as e:
                await session.close()
                raise e
                
        except Exception as e:
            logger.warning(f"{self.name}: Connection failed: {e}")
            self.reconnect_attempts += 1
            self.websocket = None
            self.error_count += 1
    
    async def _message_loop(self):
        """Main message processing loop"""
        try:
            async for msg in self.websocket:
                if not self.running:
                    break
                    
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_text_message(msg.data)
                    
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    logger.warning(f"{self.name}: Received binary message (ignoring)")
                    
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"{self.name}: WebSocket error: {self.websocket.exception()}")
                    break
                    
                elif msg.type in [aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED]:
                    logger.info(f"{self.name}: WebSocket connection closed")
                    break
                    
        except ConnectionClosed as e:
            logger.info(f"{self.name}: Connection closed: {e}")
        except WebSocketException as e:
            logger.warning(f"{self.name}: WebSocket exception: {e}")
        except Exception as e:
            logger.error(f"{self.name}: Message loop error: {e}")
            self.error_count += 1
        finally:
            if self.websocket and not self.websocket.closed:
                await self.websocket.close()
            self.websocket = None
    
    async def _handle_text_message(self, data: str):
        """Process incoming text message"""
        try:
            self.last_message_time = time.time()
            self.message_count += 1
            
            # Parse JSON
            try:
                payload = json.loads(data)
            except json.JSONDecodeError as e:
                logger.warning(f"{self.name}: Invalid JSON message: {e}")
                return
            
            # Check for heartbeat/ping messages (vendor-specific)
            if self._is_heartbeat_message(payload):
                logger.debug(f"{self.name}: Received heartbeat")
                return
            
            # Map payload to RawItem
            try:
                raw_item = self.mapper_func(payload, self.config)
                if not raw_item:
                    logger.debug(f"{self.name}: Mapper returned None for payload")
                    return
                    
            except Exception as e:
                logger.error(f"{self.name}: Mapping error: {e}")
                return
            
            # Check backpressure
            if len(self.message_queue) >= self.max_queue_size:
                # Drop oldest message
                dropped = self.message_queue.popleft()
                logger.warning(f"{self.name}: Queue full, dropped message {dropped.get('id', 'unknown')}")
            
            # Add to local queue
            item_dict = raw_item.to_dict()
            self.message_queue.append(item_dict)
            
            # Publish to event bus
            success = stream.xadd_json(
                "news.raw", 
                item_dict, 
                source=f"websocket:{self.name}"
            )
            
            if success:
                logger.debug(f"{self.name}: Published item {raw_item.id} to news.raw")
            else:
                logger.debug(f"{self.name}: Item {raw_item.id} filtered (duplicate or rate limit)")
                
        except Exception as e:
            logger.error(f"{self.name}: Error handling message: {e}")
            logger.debug(f"{self.name}: Failed message data: {data[:200]}...")
    
    def _is_heartbeat_message(self, payload: Dict[str, Any]) -> bool:
        """Check if message is a heartbeat/ping (vendor-specific)"""
        # Common heartbeat patterns
        heartbeat_indicators = [
            payload.get('type') == 'heartbeat',
            payload.get('message_type') == 'ping',
            payload.get('event') == 'ping',
            'heartbeat' in str(payload).lower(),
            'ping' in str(payload).lower()
        ]
        
        return any(heartbeat_indicators)
    
    async def _backoff_delay(self):
        """Wait before reconnection attempt"""
        if self.reconnect_attempts == 0:
            return  # No delay needed
            
        max_idx = min(self.reconnect_attempts - 1, len(self.reconnect_backoff) - 1)
        delay = self.reconnect_backoff[max_idx]
        
        logger.info(f"{self.name}: Waiting {delay}s before reconnect...")
        await asyncio.sleep(delay)
    
    async def stop(self):
        """Gracefully stop the adapter"""
        logger.info(f"{self.name}: Stopping WebSocket adapter")
        self.running = False
        
        if self.websocket and not self.websocket.closed:
            await self.websocket.close()
        
        # Log final statistics
        uptime = time.time() - self.last_message_time if self.message_count > 0 else 0
        logger.info(f"{self.name}: Final stats - Messages: {self.message_count}, "
                   f"Errors: {self.error_count}, Uptime: {uptime:.1f}s")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get adapter statistics"""
        uptime = time.time() - self.last_message_time if self.message_count > 0 else 0
        
        return {
            'name': self.name,
            'url': self.url,
            'connected': self.websocket is not None and not self.websocket.closed,
            'running': self.running,
            'message_count': self.message_count,
            'error_count': self.error_count,
            'reconnect_attempts': self.reconnect_attempts,
            'queue_size': len(self.message_queue),
            'max_queue_size': self.max_queue_size,
            'uptime_seconds': uptime,
            'last_message_time': datetime.fromtimestamp(self.last_message_time).isoformat()
        }

class WebSocketManager:
    """Manages multiple WebSocket adapters"""
    
    def __init__(self):
        self.adapters: List[WebSocketAdapter] = []
        self.running = False
        
    def add_sources(self, sources: List[Dict[str, Any]]):
        """Add WebSocket sources from configuration"""
        for source_config in sources:
            adapter = WebSocketAdapter(source_config)
            self.adapters.append(adapter)
            logger.info(f"Added WebSocket source: {source_config['name']}")
    
    async def start_all(self):
        """Start all WebSocket adapters concurrently"""
        if not self.adapters:
            logger.warning("No WebSocket sources configured")
            return
        
        logger.info(f"Starting {len(self.adapters)} WebSocket adapters")
        self.running = True
        
        # Start all adapters concurrently
        tasks = [adapter.connect_and_run() for adapter in self.adapters]
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"WebSocket manager error: {e}")
        finally:
            self.running = False
    
    async def stop_all(self):
        """Stop all adapters gracefully"""
        logger.info("Stopping all WebSocket adapters")
        self.running = False
        
        # Stop all adapters concurrently
        tasks = [adapter.stop() for adapter in self.adapters]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    def get_stats(self) -> List[Dict[str, Any]]:
        """Get statistics for all adapters"""
        return [adapter.get_stats() for adapter in self.adapters]

async def main():
    """Main entry point for WebSocket adapter CLI"""
    if not settings.event_driven.enabled:
        logger.error("EVENT_DRIVEN_ENABLED=false - WebSocket adapter disabled")
        sys.exit(1)
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="MI-3 WebSocket News Adapter")
    parser.add_argument('--config-index', type=int, help="Run single source by index")
    parser.add_argument('--list-sources', action='store_true', help="List configured sources")
    parser.add_argument('--stats', action='store_true', help="Show runtime statistics")
    args = parser.parse_args()
    
    # Load WebSocket sources
    ws_sources = settings.event_driven.ws_sources
    if not ws_sources:
        logger.error("No WebSocket sources configured in WS_SOURCES")
        sys.exit(1)
    
    if args.list_sources:
        print("Configured WebSocket sources:")
        for i, source in enumerate(ws_sources):
            print(f"  [{i}] {source['name']} - {source['url']}")
        sys.exit(0)
    
    # Create manager and add sources
    manager = WebSocketManager()
    
    if args.config_index is not None:
        # Run single source
        if 0 <= args.config_index < len(ws_sources):
            manager.add_sources([ws_sources[args.config_index]])
            logger.info(f"Running single WebSocket source: {ws_sources[args.config_index]['name']}")
        else:
            logger.error(f"Invalid config index {args.config_index}, max: {len(ws_sources)-1}")
            sys.exit(1)
    else:
        # Run all sources
        manager.add_sources(ws_sources)
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        asyncio.create_task(manager.stop_all())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start all adapters
        await manager.start_all()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        await manager.stop_all()
        logger.info("WebSocket adapter shutdown complete")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("WebSocket adapter terminated")
    except Exception as e:
        logger.error(f"WebSocket adapter fatal error: {e}")
        sys.exit(1)