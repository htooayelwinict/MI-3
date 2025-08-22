#!/usr/bin/env python3
"""
MI-3 News Scraper - SSE Test Client
Test client to demonstrate Server-Sent Events streaming functionality.
"""

import asyncio
import json
import aiohttp
import signal
import sys
from datetime import datetime

class SSETestClient:
    """Test client for Server-Sent Events streaming"""
    
    def __init__(self, base_url: str = "http://127.0.0.1:8000"):
        self.base_url = base_url
        self.running = False
        
    async def test_sse_stream(self, source_filter: str = None):
        """Test the /stream endpoint with Server-Sent Events"""
        
        url = f"{self.base_url}/stream"
        if source_filter:
            url += f"?source={source_filter}"
        
        print(f"🔌 Connecting to SSE stream: {url}")
        print("=" * 60)
        
        try:
            timeout = aiohttp.ClientTimeout(total=None)  # No timeout for streaming
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"❌ Failed to connect: HTTP {response.status}")
                        return
                    
                    print(f"✅ Connected! Status: {response.status}")
                    print(f"📡 Content-Type: {response.headers.get('Content-Type')}")
                    print(f"🔄 Listening for events...\n")
                    
                    event_count = 0
                    self.running = True
                    
                    async for line in response.content:
                        if not self.running:
                            break
                            
                        try:
                            line = line.decode('utf-8').strip()
                            
                            if line.startswith('data: '):
                                data_str = line[6:]  # Remove 'data: ' prefix
                                
                                try:
                                    event_data = json.loads(data_str)
                                    event_count += 1
                                    
                                    await self.handle_sse_event(event_data, event_count)
                                    
                                except json.JSONDecodeError as e:
                                    print(f"⚠️  Invalid JSON in event: {data_str[:100]}...")
                                    
                        except UnicodeDecodeError:
                            continue  # Skip invalid UTF-8
                            
        except asyncio.CancelledError:
            print("\n🛑 Stream cancelled by user")
        except Exception as e:
            print(f"\n❌ Stream error: {e}")
        finally:
            self.running = False
            print(f"\n📊 Total events received: {event_count}")
    
    async def handle_sse_event(self, event_data: dict, event_count: int):
        """Handle different types of SSE events"""
        event_type = event_data.get('type', 'unknown')
        timestamp = event_data.get('timestamp', 'N/A')
        
        # Format timestamp for display
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            time_str = dt.strftime('%H:%M:%S')
        except:
            time_str = timestamp[:8] if len(timestamp) > 8 else timestamp
        
        if event_type == 'connected':
            client_id = event_data.get('client_id', 'unknown')
            print(f"🟢 [{time_str}] Connected - Client ID: {client_id}")
            
        elif event_type == 'initial_data':
            count = event_data.get('count', 0)
            total = event_data.get('total_available', 0)
            source_filter = event_data.get('source_filter')
            print(f"📦 [{time_str}] Initial data: {count} items (total: {total})")
            if source_filter:
                print(f"    🔍 Filtered by source: {source_filter}")
            
            # Show first few items
            items = event_data.get('items', [])[:3]
            for i, item in enumerate(items, 1):
                title = item.get('title', 'No title')[:50]
                source = item.get('source', 'Unknown')
                print(f"    {i}. {title}... [{source}]")
            
        elif event_type == 'new_item':
            item = event_data.get('item', {})
            title = item.get('title', 'No title')
            source = item.get('source', 'Unknown')
            published = item.get('published', 'N/A')
            
            print(f"🆕 [{time_str}] NEW: {title[:60]}")
            print(f"    📰 Source: {source}")
            print(f"    📅 Published: {published}")
            print(f"    🔗 Link: {item.get('link', 'N/A')}")
            print()
            
        elif event_type == 'batch_complete':
            new_count = event_data.get('new_items_count', 0)
            total_tracking = event_data.get('total_items_tracking', 0)
            if new_count > 1:
                print(f"✅ [{time_str}] Batch complete: {new_count} new items (tracking {total_tracking})\n")
            
        elif event_type == 'heartbeat':
            items_tracking = event_data.get('items_tracking', 0)
            uptime_checks = event_data.get('uptime_checks', 0)
            last_update = event_data.get('last_data_update', 'N/A')
            
            print(f"💓 [{time_str}] Heartbeat #{uptime_checks} - Tracking {items_tracking} items")
            if last_update != 'N/A':
                try:
                    update_dt = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                    update_str = update_dt.strftime('%H:%M:%S')
                    print(f"    📊 Last data update: {update_str}")
                except:
                    print(f"    📊 Last data update: {last_update}")
            
        elif event_type == 'error':
            message = event_data.get('message', 'Unknown error')
            recoverable = event_data.get('recoverable', False)
            print(f"❌ [{time_str}] Error: {message}")
            if recoverable:
                print("    🔄 Attempting to recover...")
            
        elif event_type == 'disconnected':
            client_id = event_data.get('client_id', 'unknown')
            print(f"🔴 [{time_str}] Disconnected - Client ID: {client_id}")
            
        else:
            print(f"❓ [{time_str}] Unknown event type '{event_type}': {event_data}")
    
    def stop(self):
        """Stop the SSE client"""
        self.running = False

async def main():
    """Main test function"""
    client = SSETestClient()
    
    # Set up signal handler for graceful shutdown
    def signal_handler(sig, frame):
        print(f"\n🛑 Received signal {sig}, shutting down...")
        client.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse command line arguments
    source_filter = None
    if len(sys.argv) > 1:
        source_filter = sys.argv[1]
        print(f"🔍 Filtering by source: {source_filter}")
    
    try:
        # Test the SSE stream
        await client.test_sse_stream(source_filter)
        
    except KeyboardInterrupt:
        print("\n👋 Test client stopped")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print("""
MI-3 SSE Test Client

Usage:
  python examples/sse_test_client.py                    # Stream all sources
  python examples/sse_test_client.py "Yahoo Finance"   # Filter by source
  python examples/sse_test_client.py --help            # Show this help

This client connects to the /stream endpoint and displays real-time events.

Make sure the API server is running:
  uvicorn realtime.hub:app --host 127.0.0.1 --port 8000

Press Ctrl+C to stop the client.
""")
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")