#!/usr/bin/env python3
"""
Quick test script for MI-3 Event-Driven News System
"""

import subprocess
import time
import requests
import json
import sys
import signal

def start_server():
    """Start the realtime stack"""
    print("🚀 Starting MI-3 realtime stack...")
    
    proc = subprocess.Popen(
        ['./scripts/run_realtime_stack.sh'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    print("⏳ Waiting for services to start...")
    time.sleep(8)
    
    # Test if API is ready
    api_ready = False
    for attempt in range(10):
        try:
            response = requests.get('http://127.0.0.1:8000/', timeout=2)
            if response.status_code == 200:
                api_ready = True
                break
        except:
            pass
        time.sleep(1)
    
    if api_ready:
        print("✅ API server is ready!")
        return proc
    else:
        print("❌ API server failed to start")
        proc.terminate()
        return None

def test_system():
    """Test the system functionality"""
    print("\n🧪 Testing system...")
    
    try:
        # Test root endpoint
        response = requests.get('http://127.0.0.1:8000/', timeout=5)
        if response.status_code == 200:
            data = response.json()
            features = data.get('features', {})
            print("🎛️  Features:")
            for feature, enabled in features.items():
                status = '✅' if enabled else '❌'
                print(f"   {status} {feature}")
        
        # Test webhook health
        response = requests.get('http://127.0.0.1:8000/push/health', timeout=5)
        if response.status_code == 200:
            health = response.json()
            print(f"\n📡 Webhook Status: {health.get('status', 'unknown')}")
            print(f"🔐 HMAC Required: {health.get('signature_required', False)}")
        
        # Test latest endpoint
        response = requests.get('http://127.0.0.1:8000/latest?limit=5', timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"\n📰 Latest Items: {data.get('count', 0)} items available")
            
        print("\n✅ System tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ System test failed: {e}")
        return False

def main():
    """Main function"""
    print("MI-3 Event-Driven News System - Quick Test")
    print("=" * 50)
    
    # Start server
    proc = start_server()
    if not proc:
        sys.exit(1)
    
    # Test system
    test_success = test_system()
    
    if test_success:
        print("\n🎯 System is ready! Available endpoints:")
        print("   📡 Root:           http://127.0.0.1:8000/")
        print("   📰 Latest Items:   http://127.0.0.1:8000/latest")
        print("   📡 SSE Stream:     http://127.0.0.1:8000/stream")
        print("   🔗 Webhook:        http://127.0.0.1:8000/push/inbound")
        print("   ❤️  Health:        http://127.0.0.1:8000/push/health")
        
        print("\n💡 Next steps:")
        print("   • Test webhook: python test_webhook.py")
        print("   • View in browser: http://127.0.0.1:8000/test-page")
        print("   • Send webhooks to: http://127.0.0.1:8000/push/inbound")
        
        print("\n⏸️  Press Ctrl+C to stop the server...")
        
        # Keep running until Ctrl+C
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    
    # Cleanup
    print("\n🛑 Stopping services...")
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except:
        proc.kill()
    print("✅ Services stopped")

if __name__ == "__main__":
    main()