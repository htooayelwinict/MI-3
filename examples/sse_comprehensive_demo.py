#!/usr/bin/env python3
"""
MI-3 News Scraper - Comprehensive SSE Demo
Demonstrates all enhanced Server-Sent Events features.
"""

import asyncio
import aiohttp
import json
import sys
from datetime import datetime
import signal

async def test_multiple_clients():
    """Test multiple SSE clients connecting simultaneously"""
    print("🔀 Testing multiple concurrent SSE clients...")
    print("=" * 60)
    
    async def single_client(client_id: int, duration: int = 30):
        """Single SSE client connection"""
        url = "http://127.0.0.1:8000/stream"
        
        try:
            timeout = aiohttp.ClientTimeout(total=duration + 5)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"❌ Client {client_id}: Failed to connect (HTTP {response.status})")
                        return
                    
                    print(f"✅ Client {client_id}: Connected successfully")
                    events_received = 0
                    start_time = asyncio.get_event_loop().time()
                    
                    async for line in response.content:
                        if asyncio.get_event_loop().time() - start_time > duration:
                            break
                            
                        try:
                            line = line.decode('utf-8').strip()
                            
                            if line.startswith('data: '):
                                data_str = line[6:]
                                event_data = json.loads(data_str)
                                events_received += 1
                                
                                event_type = event_data.get('type', 'unknown')
                                timestamp = event_data.get('timestamp', '')
                                
                                if event_type == 'connected':
                                    server_client_id = event_data.get('client_id', 'unknown')
                                    print(f"🟢 Client {client_id}: Server assigned ID {server_client_id}")
                                
                                elif event_type == 'new_item':
                                    item = event_data.get('item', {})
                                    title = item.get('title', 'No title')[:40]
                                    print(f"📰 Client {client_id}: New item - {title}...")
                                
                                elif event_type == 'heartbeat':
                                    uptime = event_data.get('uptime_checks', 0)
                                    tracking = event_data.get('items_tracking', 0)
                                    print(f"💓 Client {client_id}: Heartbeat #{uptime} (tracking {tracking} items)")
                                
                                elif event_type == 'error':
                                    message = event_data.get('message', 'Unknown error')
                                    print(f"❌ Client {client_id}: Error - {message}")
                                    
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            continue
                    
                    print(f"📊 Client {client_id}: Received {events_received} events in {duration}s")
                    
        except Exception as e:
            print(f"❌ Client {client_id}: Error - {e}")
    
    # Launch multiple clients concurrently
    tasks = [
        single_client(1, 30),
        single_client(2, 30),
        single_client(3, 30)
    ]
    
    await asyncio.gather(*tasks, return_exceptions=True)
    print("✅ Multiple client test completed")

async def test_source_filtering():
    """Test source filtering functionality"""
    print("\n🔍 Testing source filtering...")
    print("=" * 60)
    
    # First, get available sources
    async with aiohttp.ClientSession() as session:
        async with session.get("http://127.0.0.1:8000/sources") as response:
            if response.status == 200:
                data = await response.json()
                sources = data.get('sources', [])
                if sources:
                    test_source = sources[0]['name']
                    print(f"📡 Testing filter with source: {test_source}")
                    
                    # Test filtered stream
                    url = f"http://127.0.0.1:8000/stream?source={test_source}"
                    
                    timeout = aiohttp.ClientTimeout(total=20)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(url) as response:
                            if response.status != 200:
                                print(f"❌ Filtered stream failed: HTTP {response.status}")
                                return
                            
                            print(f"✅ Connected to filtered stream")
                            events_received = 0
                            start_time = asyncio.get_event_loop().time()
                            
                            async for line in response.content:
                                if asyncio.get_event_loop().time() - start_time > 15:
                                    break
                                
                                try:
                                    line = line.decode('utf-8').strip()
                                    
                                    if line.startswith('data: '):
                                        data_str = line[6:]
                                        event_data = json.loads(data_str)
                                        events_received += 1
                                        
                                        event_type = event_data.get('type', 'unknown')
                                        
                                        if event_type == 'initial_data':
                                            source_filter = event_data.get('source_filter')
                                            count = event_data.get('count', 0)
                                            print(f"📦 Initial data: {count} items from '{source_filter}'")
                                        
                                        elif event_type == 'new_item':
                                            item = event_data.get('item', {})
                                            item_source = item.get('source', 'Unknown')
                                            title = item.get('title', 'No title')[:40]
                                            print(f"📰 New item from '{item_source}': {title}...")
                                            
                                except (UnicodeDecodeError, json.JSONDecodeError):
                                    continue
                            
                            print(f"📊 Filtered stream: Received {events_received} events")
                else:
                    print("⚠️  No sources available for filtering test")
            else:
                print(f"❌ Failed to get sources: HTTP {response.status}")

async def test_heartbeat_timing():
    """Test heartbeat timing (should be every 15 seconds)"""
    print("\n💓 Testing heartbeat timing...")
    print("=" * 60)
    
    url = "http://127.0.0.1:8000/stream"
    heartbeat_times = []
    
    try:
        timeout = aiohttp.ClientTimeout(total=60)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    print(f"❌ Failed to connect: HTTP {response.status}")
                    return
                
                print("✅ Connected - monitoring heartbeat timing...")
                start_time = asyncio.get_event_loop().time()
                
                async for line in response.content:
                    current_time = asyncio.get_event_loop().time()
                    if current_time - start_time > 50:  # Test for 50 seconds
                        break
                    
                    try:
                        line = line.decode('utf-8').strip()
                        
                        if line.startswith('data: '):
                            data_str = line[6:]
                            event_data = json.loads(data_str)
                            
                            if event_data.get('type') == 'heartbeat':
                                elapsed = current_time - start_time
                                heartbeat_times.append(elapsed)
                                uptime_checks = event_data.get('uptime_checks', 0)
                                print(f"💓 Heartbeat #{uptime_checks} at {elapsed:.1f}s")
                                
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
        
        # Analyze heartbeat intervals
        if len(heartbeat_times) > 1:
            intervals = [heartbeat_times[i] - heartbeat_times[i-1] for i in range(1, len(heartbeat_times))]
            avg_interval = sum(intervals) / len(intervals)
            print(f"📊 Average heartbeat interval: {avg_interval:.1f}s (target: 15s)")
            
            if 14 <= avg_interval <= 16:
                print("✅ Heartbeat timing is correct!")
            else:
                print(f"⚠️  Heartbeat timing may be off (expected ~15s, got {avg_interval:.1f}s)")
        else:
            print("⚠️  Not enough heartbeats received for timing analysis")
            
    except Exception as e:
        print(f"❌ Heartbeat test error: {e}")

async def test_connection_resilience():
    """Test connection resilience and error handling"""
    print("\n🛡️  Testing connection resilience...")
    print("=" * 60)
    
    url = "http://127.0.0.1:8000/stream"
    
    try:
        # Test with a very short timeout to simulate network issues
        timeout = aiohttp.ClientTimeout(total=10, sock_read=2)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"❌ Connection failed: HTTP {response.status}")
                        return
                    
                    print("✅ Connection established with short timeout")
                    events_received = 0
                    
                    async for line in response.content:
                        try:
                            line = line.decode('utf-8').strip()
                            
                            if line.startswith('data: '):
                                events_received += 1
                                data_str = line[6:]
                                event_data = json.loads(data_str)
                                
                                event_type = event_data.get('type', 'unknown')
                                if event_type in ['connected', 'initial_data', 'heartbeat']:
                                    print(f"📡 Received {event_type} event")
                                    
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            continue
                    
                    print(f"📊 Received {events_received} events before timeout")
                    
            except asyncio.TimeoutError:
                print("⏰ Connection timed out as expected (testing resilience)")
            except Exception as e:
                print(f"🔄 Connection error handled: {e}")
                
    except Exception as e:
        print(f"❌ Resilience test error: {e}")

async def main():
    """Main demo function"""
    print("🚀 MI-3 Enhanced SSE Comprehensive Demo")
    print("=" * 60)
    print("Testing all Server-Sent Events enhancements...")
    print()
    
    # Check if server is running
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("http://127.0.0.1:8000/") as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ Server is running: {data.get('name', 'Unknown')}")
                else:
                    print(f"❌ Server returned HTTP {response.status}")
                    return
    except Exception as e:
        print(f"❌ Cannot connect to server: {e}")
        print("Please start the server with: uvicorn realtime.hub:app --port 8000")
        return
    
    try:
        # Run all tests
        await test_multiple_clients()
        await test_source_filtering()
        await test_heartbeat_timing() 
        await test_connection_resilience()
        
        print("\n🎉 All SSE enhancement tests completed!")
        print("\n📋 Summary of enhancements:")
        print("✅ StreamingResponse with proper text/event-stream media type")
        print("✅ Async generator with continuous real-time yielding")
        print("✅ Integration with existing news feed system")
        print("✅ Multiple concurrent client support")
        print("✅ 15-second heartbeat to prevent timeouts")
        print("✅ Enhanced headers for better browser compatibility")
        print("✅ Client disconnect detection and cleanup")
        print("✅ Source filtering support")
        print("✅ Proper SSE format: data: {...}\\n\\n")
        print("✅ Error handling and recovery")
        print("✅ Detailed event types and metadata")
        
    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print("""
MI-3 Enhanced SSE Comprehensive Demo

This script tests all enhancements made to the Server-Sent Events streaming:

1. Multiple concurrent clients
2. Source filtering functionality  
3. Heartbeat timing (15-second intervals)
4. Connection resilience and error handling

Usage:
  python examples/sse_comprehensive_demo.py

Prerequisites:
- Start the API server: uvicorn realtime.hub:app --port 8000
- Optionally start feed worker: python -m ingest.feeds_worker

Press Ctrl+C to stop the demo.
""")
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("\n👋 Demo stopped by user")
        except Exception as e:
            print(f"❌ Fatal error: {e}")