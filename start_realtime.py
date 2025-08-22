#!/usr/bin/env python3
"""
MI-3 News Scraper - Real-time Quick Start
Script to quickly start both the feed worker and API server.
"""

import os
import sys
import subprocess
import time
import signal
from pathlib import Path

def check_dependencies():
    """Check if required dependencies are installed"""
    required_modules = ['aiohttp', 'feedparser', 'fastapi', 'uvicorn', 'yaml']
    missing = []
    
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing.append(module)
    
    if missing:
        print(f"❌ Missing dependencies: {', '.join(missing)}")
        print("Install with: pip install -r requirements-realtime.txt")
        return False
    
    return True

def start_processes():
    """Start both feed worker and API server"""
    if not check_dependencies():
        return
    
    # Set environment variable
    os.environ['REALTIME_ENABLED'] = 'true'
    
    print("🚀 Starting MI-3 Real-time News Scraper")
    print("=" * 50)
    
    processes = []
    
    try:
        # Start feed worker
        print("📡 Starting feed worker...")
        feed_worker = subprocess.Popen([
            sys.executable, "-m", "ingest.feeds_worker"
        ])
        processes.append(feed_worker)
        
        # Wait a moment
        time.sleep(2)
        
        # Start API server
        print("🌐 Starting API server...")
        api_server = subprocess.Popen([
            "uvicorn", "realtime.hub:app", 
            "--host", "127.0.0.1", 
            "--port", "8000",
            "--log-level", "info"
        ])
        processes.append(api_server)
        
        print("\n✅ Both services started!")
        print("📊 API available at: http://127.0.0.1:8000")
        print("📄 Latest items: http://127.0.0.1:8000/latest")
        print("📈 Real-time stream: http://127.0.0.1:8000/stream")
        print("\n⏹️  Press Ctrl+C to stop both services")
        
        # Wait for processes
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Shutting down services...")
        
        for process in processes:
            if process.poll() is None:
                process.terminate()
        
        # Give processes time to shutdown gracefully
        time.sleep(2)
        
        for process in processes:
            if process.poll() is None:
                process.kill()
        
        print("✅ All services stopped")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        
        # Cleanup
        for process in processes:
            if process.poll() is None:
                process.terminate()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--help":
        print("""
MI-3 Real-time News Scraper Quick Start

Usage:
  python start_realtime.py     # Start both services
  python start_realtime.py --help   # Show this help

This script will:
1. Check dependencies
2. Start the RSS feed worker
3. Start the FastAPI server
4. Display available endpoints

Press Ctrl+C to stop both services.
""")
    else:
        start_processes()