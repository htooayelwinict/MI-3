#!/usr/bin/env python3
"""
MI-3 News Scraper - Real-time Demo
Example script showing how to use the real-time feed features.
"""

import asyncio
import aiohttp
import json
from datetime import datetime

async def demo_api_client():
    """Demo client for the real-time API"""
    base_url = "http://127.0.0.1:8000"
    
    async with aiohttp.ClientSession() as session:
        print("🚀 MI-3 Real-time API Demo")
        print("=" * 40)
        
        # Test root endpoint
        print("\n1. Testing API root...")
        async with session.get(f"{base_url}/") as resp:
            if resp.status == 200:
                data = await resp.json()
                print(f"✅ API Status: {data.get('status')}")
                print(f"📅 Timestamp: {data.get('timestamp')}")
            else:
                print(f"❌ API not responding (status: {resp.status})")
                return
        
        # Test stats
        print("\n2. Getting statistics...")
        async with session.get(f"{base_url}/stats") as resp:
            if resp.status == 200:
                data = await resp.json()
                stats = data.get('stats', {})
                print(f"📊 Total items: {stats.get('total_items', 0)}")
                print(f"📰 Sources: {stats.get('sources_count', 0)}")
                print(f"🕒 Last updated: {stats.get('last_updated', 'Never')}")
            else:
                print(f"❌ Stats unavailable")
        
        # Test latest items
        print("\n3. Getting latest items...")
        async with session.get(f"{base_url}/latest?limit=5") as resp:
            if resp.status == 200:
                data = await resp.json()
                items = data.get('items', [])
                print(f"📄 Found {len(items)} latest items:")
                
                for i, item in enumerate(items[:3], 1):
                    print(f"   {i}. {item.get('title', 'No title')[:60]}...")
                    print(f"      Source: {item.get('source', 'Unknown')}")
                    print(f"      Published: {item.get('published', 'Unknown')}")
                    print()
            else:
                print(f"❌ Latest items unavailable")
        
        print("✅ Demo completed!")
        print("\nTo see streaming updates, open:")
        print(f"   {base_url}/stream")
        print("\nOr in a browser:")
        print(f"   {base_url}/latest")

def print_usage():
    """Print usage instructions"""
    print("""
🔥 MI-3 Real-time News Scraper Setup Guide

1. Install dependencies:
   pip install -r requirements-realtime.txt

2. Enable real-time features:
   export REALTIME_ENABLED=true

3. Start the feed worker (in one terminal):
   python -m ingest.feeds_worker

4. Start the API server (in another terminal):
   uvicorn realtime.hub:app --host 127.0.0.1 --port 8000

5. Test the API:
   python examples/realtime_demo.py

Available endpoints:
- GET /latest?limit=50&source=Yahoo - Get latest items
- GET /stream - Server-sent events stream
- GET /stats - Get statistics
- GET /sources - Get configured sources
- GET /health - Health check

Environment Variables:
- REALTIME_ENABLED=true/false
- FEED_POLL_INTERVAL=300 (seconds)
- REALTIME_API_HOST=127.0.0.1
- REALTIME_API_PORT=8000
""")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print_usage()
    else:
        try:
            asyncio.run(demo_api_client())
        except KeyboardInterrupt:
            print("\n👋 Demo interrupted")
        except Exception as e:
            print(f"❌ Demo failed: {e}")
            print("\nMake sure the API server is running:")
            print("uvicorn realtime.hub:app --host 127.0.0.1 --port 8000")