#!/usr/bin/env python3
"""
Test webhook functionality with HMAC signature validation.
"""

import json
import hmac
import hashlib
import requests
import sys
from datetime import datetime

def generate_signature(payload_bytes: bytes, secret: str) -> str:
    """Generate HMAC-SHA256 signature for webhook payload"""
    signature = hmac.new(
        secret.encode('utf-8'),
        payload_bytes,
        hashlib.sha256
    ).hexdigest()
    return f'sha256={signature}'

def test_webhook():
    """Test webhook with HMAC signature"""
    
    # Configuration
    webhook_url = 'http://127.0.0.1:8000/push/inbound'
    webhook_secret = 'mi3_webhook_secret_key_2024'  # From .env file
    
    # Test payload
    payload = {
        'title': 'Test News from Webhook Script',
        'url': 'https://example.com/webhook-script-test',
        'published': datetime.now().isoformat() + 'Z',
        'category': 'test',
        'description': 'Testing webhook integration with Python script',
        'publisher': 'Test Publisher'
    }
    
    # Serialize payload
    payload_json = json.dumps(payload, separators=(',', ':'))
    payload_bytes = payload_json.encode('utf-8')
    
    # Generate signature
    signature = generate_signature(payload_bytes, webhook_secret)
    
    # Headers
    headers = {
        'Content-Type': 'application/json',
        'X-Signature': signature,
        'X-Vendor': 'test_script'
    }
    
    print(f"Testing webhook: {webhook_url}")
    print(f"Payload: {payload_json}")
    print(f"Signature: {signature}")
    print("-" * 50)
    
    try:
        # Send webhook
        response = requests.post(webhook_url, data=payload_json, headers=headers, timeout=10)
        
        print(f"Response Status: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        print(f"Response Body: {response.text}")
        
        if response.status_code == 200:
            print("✅ Webhook test successful!")
            
            # Test SSE stream to see if item appears
            print("\nTesting SSE stream to verify item propagation...")
            stream_response = requests.get('http://127.0.0.1:8000/stream', 
                                         headers={'Accept': 'text/event-stream'}, 
                                         stream=True, timeout=5)
            
            if stream_response.status_code == 200:
                print("✅ SSE stream accessible")
                # Read a few lines to see if our item appears
                for i, line in enumerate(stream_response.iter_lines(decode_unicode=True)):
                    if i > 10:  # Read first few lines
                        break
                    if line:
                        print(f"SSE: {line}")
            else:
                print(f"⚠️  SSE stream returned {stream_response.status_code}")
                
        else:
            print("❌ Webhook test failed!")
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection failed! Make sure the server is running:")
        print("   ./scripts/run_realtime_stack.sh")
    except requests.exceptions.Timeout:
        print("❌ Request timed out")
    except Exception as e:
        print(f"❌ Error: {e}")

def test_webhook_without_signature():
    """Test webhook health endpoint (no signature required)"""
    
    health_url = 'http://127.0.0.1:8000/push/health'
    
    try:
        response = requests.get(health_url, timeout=5)
        print(f"\nWebhook Health Check: {response.status_code}")
        if response.status_code == 200:
            health_data = response.json()
            print(f"Status: {health_data.get('status', 'unknown')}")
            print(f"Signature Required: {health_data.get('signature_required', False)}")
            print(f"Webhook Path: {health_data.get('webhook_path', 'unknown')}")
        else:
            print(f"Health check failed: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Health check failed - server not running")
    except Exception as e:
        print(f"❌ Health check error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--health':
        test_webhook_without_signature()
    else:
        print("MI-3 Webhook Test Script")
        print("=" * 30)
        print("Make sure the server is running: ./scripts/run_realtime_stack.sh")
        print()
        
        test_webhook_without_signature()
        test_webhook()
        
        print("\n" + "=" * 50)
        print("Test completed! Check the server logs and /latest endpoint:")
        print("  curl http://127.0.0.1:8000/latest")