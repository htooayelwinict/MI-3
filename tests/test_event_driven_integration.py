#!/usr/bin/env python3
"""
Integration tests for event-driven news ingestion system.
"""

import asyncio
import json
import pytest
from unittest.mock import patch, Mock
from fastapi.testclient import TestClient

from bus.stream import EventBus, get_event_bus
from storage.schemas import RawItem
from adapters.webhook_adapter import webhook_router
from adapters.mappers import safe_map_payload


class TestEventDrivenIntegration:
    """Integration tests for the complete event-driven system"""
    
    def setup_method(self):
        """Set up test environment"""
        # Create fresh event bus for each test
        self.event_bus = EventBus()
        
        # Mock the global event bus
        self.event_bus_patcher = patch('bus.stream._event_bus', self.event_bus)
        self.event_bus_patcher.start()
        
        # Setup webhook test client
        from fastapi import FastAPI
        self.app = FastAPI()
        self.app.include_router(webhook_router)
        self.webhook_client = TestClient(self.app)
    
    def teardown_method(self):
        """Clean up after tests"""
        self.event_bus_patcher.stop()
    
    def test_webhook_to_sse_pipeline(self):
        """Test complete pipeline: webhook → event bus → SSE stream"""
        
        # Step 1: Send webhook
        payload = {
            "title": "Integration Test News",
            "url": "https://example.com/integration-test",
            "published": "2024-01-01T12:00:00Z",
            "category": "integration",
            "description": "End-to-end integration test"
        }
        
        with patch('adapters.webhook_adapter.settings') as mock_settings:
            mock_settings.event_driven.webhook_secret = ""  # No signature required
            
            # Send webhook
            response = self.webhook_client.post(
                "/push/inbound",
                json=payload,
                headers={"X-Vendor": "integration_test"}
            )
            
            assert response.status_code == 200
        
        # Step 2: Verify item in event bus
        # Allow some time for background processing
        import time
        time.sleep(0.1)
        
        recent_items = self.event_bus.get_recent_messages("news.raw", 10)
        
        # Should have at least one item
        assert len(recent_items) >= 1
        
        # Find our test item
        test_item = None
        for item in recent_items:
            if item.get('title') == 'Integration Test News':
                test_item = item
                break
        
        assert test_item is not None
        assert test_item['link'] == 'https://example.com/integration-test'
        assert test_item['source'] == 'webhook:integration_test'
        assert test_item['topic'] == 'integration'
    
    def test_multiple_sources_deduplication(self):
        """Test deduplication across multiple event sources"""
        
        # Create same news item from different sources
        base_item = {
            "title": "Duplicate Test News",
            "link": "https://example.com/duplicate-test",
            "published": "2024-01-01T15:00:00Z",
            "topic": "test",
            "publisher": "Test Publisher"
        }
        
        # Source 1: Direct event bus publication
        item1 = RawItem(
            id="",
            source="websocket:source1",
            **base_item
        )
        
        success1 = self.event_bus.xadd_json("news.raw", item1.to_dict(), "websocket:source1")
        assert success1 == True
        
        # Source 2: Same item from webhook (should be deduplicated)
        webhook_payload = {
            "title": "Duplicate Test News",
            "url": "https://example.com/duplicate-test",
            "published": "2024-01-01T15:00:00Z",
            "category": "test",
            "publisher": "Test Publisher"
        }
        
        with patch('adapters.webhook_adapter.settings') as mock_settings:
            mock_settings.event_driven.webhook_secret = ""
            
            response = self.webhook_client.post(
                "/push/inbound",
                json=webhook_payload,
                headers={"X-Vendor": "source2"}
            )
            
            assert response.status_code == 200
        
        # Give time for processing
        import time
        time.sleep(0.1)
        
        # Should only have one unique item
        recent_items = self.event_bus.get_recent_messages("news.raw", 10)
        duplicate_items = [
            item for item in recent_items 
            if item.get('title') == 'Duplicate Test News'
        ]
        
        # Should be deduplicated (only one item despite two sources)
        assert len(duplicate_items) == 1
    
    def test_rate_limiting_functionality(self):
        """Test rate limiting prevents spam"""
        
        # Configure low rate limit for testing
        self.event_bus.default_rate_limit = 2.0  # 2 messages per second
        
        # Send multiple identical requests rapidly
        payload = {
            "title": "Rate Limit Test",
            "url": "https://example.com/rate-limit-test",
            "published": "2024-01-01T16:00:00Z"
        }
        
        successful_requests = 0
        rate_limited_requests = 0
        
        with patch('adapters.webhook_adapter.settings') as mock_settings:
            mock_settings.event_driven.webhook_secret = ""
            
            # Send 10 requests rapidly
            for i in range(10):
                response = self.webhook_client.post(
                    "/push/inbound",
                    json={**payload, "title": f"Rate Limit Test {i}"},  # Unique titles
                    headers={"X-Vendor": "rate_test"}
                )
                
                if response.status_code == 200:
                    successful_requests += 1
                else:
                    rate_limited_requests += 1
        
        # Should have accepted some but rate limited others
        assert successful_requests > 0
        # Rate limiting happens at event bus level, webhook always returns 200
        # but items get filtered by rate limiter
        
        # Check event bus for actual published items
        import time
        time.sleep(0.1)
        
        recent_items = self.event_bus.get_recent_messages("news.raw", 20)
        rate_test_items = [
            item for item in recent_items 
            if 'Rate Limit Test' in item.get('title', '')
        ]
        
        # Should be fewer than 10 due to rate limiting
        assert len(rate_test_items) < 10
        assert len(rate_test_items) > 0
    
    @pytest.mark.asyncio
    async def test_websocket_adapter_to_event_bus(self):
        """Test WebSocket adapter publishing to event bus"""
        from adapters.websocket_adapter import WebSocketAdapter
        
        config = {
            'name': 'test_websocket',
            'url': 'wss://test.example.com',
            'topic': 'test',
            'max_queue_size': 10
        }
        
        adapter = WebSocketAdapter(config)
        
        # Simulate WebSocket message
        ws_payload = {
            'title': 'WebSocket Integration Test',
            'url': 'https://example.com/websocket-test',
            'timestamp': '2024-01-01T17:00:00Z',
            'summary': 'WebSocket integration test message'
        }
        
        # Process the message (this should publish to event bus)
        await adapter._handle_text_message(json.dumps(ws_payload))
        
        # Verify in event bus
        recent_items = self.event_bus.get_recent_messages("news.raw", 10)
        
        ws_item = None
        for item in recent_items:
            if item.get('title') == 'WebSocket Integration Test':
                ws_item = item
                break
        
        assert ws_item is not None
        assert ws_item['source'] == 'websocket:test_websocket'
        assert ws_item['link'] == 'https://example.com/websocket-test'
        assert ws_item['summary'] == 'WebSocket integration test message'
    
    def test_unified_data_manager_integration(self):
        """Test UnifiedDataManager combining RSS and event bus data"""
        from realtime.hub import UnifiedDataManager
        from pathlib import Path
        import tempfile
        import json
        
        # Create temporary RSS file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            rss_data = {
                "items": [
                    {
                        "id": "rss_item_1",
                        "title": "RSS News Item",
                        "link": "https://example.com/rss-item",
                        "published": "2024-01-01T10:00:00Z",
                        "source": "rss:yahoo_finance",
                        "publisher": "Yahoo Finance",
                        "category": "finance"
                    }
                ],
                "total_items": 1,
                "last_updated": "2024-01-01T10:00:00Z"
            }
            json.dump(rss_data, f)
            rss_file_path = f.name
        
        # Add item to event bus
        event_item = RawItem(
            id="event_item_1",
            title="Event Bus News Item",
            link="https://example.com/event-item",
            published="2024-01-01T11:00:00Z",
            source="webhook:test_source",
            publisher="Test Publisher",
            topic="business"
        )
        
        self.event_bus.xadd_json("news.raw", event_item.to_dict(), "webhook:test")
        
        # Test UnifiedDataManager
        manager = UnifiedDataManager(rss_data_file=Path(rss_file_path))
        
        # Get latest items (should combine RSS + event bus)
        latest_items = manager.get_latest_items(10)
        
        # Should have items from both sources
        assert len(latest_items) >= 2
        
        # Find items from each source
        rss_items = [item for item in latest_items if 'rss:' in item.get('source', '')]
        event_items = [item for item in latest_items if 'webhook:' in item.get('source', '')]
        
        assert len(rss_items) >= 1
        assert len(event_items) >= 1
        
        # Verify RSS item conversion (category -> topic)
        rss_item = next((item for item in rss_items if item['id'] == 'rss_item_1'), None)
        assert rss_item is not None
        assert rss_item.get('topic') == 'finance'  # Should convert from category
        
        # Verify event bus item
        event_item_found = next((item for item in event_items if item['id'] == 'event_item_1'), None)
        assert event_item_found is not None
        assert event_item_found['topic'] == 'business'
        
        # Test statistics
        stats = manager.get_stats()
        assert stats['rss_items'] == 1
        assert stats['event_bus_items'] >= 1
        assert stats['total_sources'] >= 2
        
        # Clean up
        import os
        os.unlink(rss_file_path)
    
    def test_payload_normalization_consistency(self):
        """Test that different sources produce consistent RawItem format"""
        
        # Test data representing same news from different sources
        base_news = {
            "title": "Consistency Test News",
            "url": "https://example.com/consistency-test",
            "published_time": "2024-01-01T12:00:00Z",
            "description": "Testing payload consistency",
            "category": "test"
        }
        
        # WebSocket format
        ws_payload = {
            "headline": base_news["title"],
            "link": base_news["url"],
            "timestamp": base_news["published_time"],
            "summary": base_news["description"],
            "category": base_news["category"]
        }
        
        ws_config = {"name": "test_ws", "topic": "test"}
        ws_result = safe_map_payload(ws_payload, "websocket", config=ws_config)
        
        # Webhook format
        webhook_payload = {
            "title": base_news["title"],
            "url": base_news["url"],
            "published": base_news["published_time"],
            "description": base_news["description"],
            "category": base_news["category"]
        }
        
        webhook_headers = {"X-Vendor": "test_webhook"}
        webhook_result = safe_map_payload(webhook_payload, "webhook", headers=webhook_headers)
        
        # Both should produce valid RawItems
        assert ws_result is not None
        assert webhook_result is not None
        
        # Key fields should be consistent
        assert ws_result.title == webhook_result.title
        assert ws_result.link == webhook_result.link
        assert ws_result.topic == webhook_result.topic
        
        # Sources should be different but publishers identifiable
        assert ws_result.source == "websocket:test_ws"
        assert webhook_result.source == "webhook:test_webhook"
        
        # Both should have valid IDs
        assert ws_result.id
        assert webhook_result.id
        
        # IDs might be different due to source differences in generation
        # but both should be valid SHA256 prefixes
        assert len(ws_result.id) == 16
        assert len(webhook_result.id) == 16


class TestEventBusResilience:
    """Test event bus resilience and error handling"""
    
    def setup_method(self):
        """Set up test environment"""
        self.event_bus = EventBus()
    
    def test_malformed_item_rejection(self):
        """Test that malformed items are rejected"""
        
        # Missing required fields
        invalid_items = [
            {},  # Empty
            {"title": "No Link"},  # Missing link
            {"link": "https://example.com"},  # Missing title
            {"title": "Test", "link": "https://example.com"},  # Missing published
        ]
        
        for invalid_item in invalid_items:
            result = self.event_bus.xadd_json("news.raw", invalid_item, "test")
            assert result == False  # Should reject invalid items
        
        # Valid item should succeed
        valid_item = {
            "id": "valid_test",
            "title": "Valid Item",
            "link": "https://example.com/valid",
            "published": "2024-01-01T12:00:00Z",
            "source": "test:valid",
            "publisher": "Test Publisher",
            "topic": "test"
        }
        
        result = self.event_bus.xadd_json("news.raw", valid_item, "test")
        assert result == True
    
    def test_high_volume_processing(self):
        """Test event bus under high message volume"""
        
        # Send many messages rapidly
        num_messages = 100
        successful_publishes = 0
        
        for i in range(num_messages):
            item = {
                "id": f"high_vol_{i}",
                "title": f"High Volume Test {i}",
                "link": f"https://example.com/high-vol-{i}",
                "published": "2024-01-01T12:00:00Z",
                "source": "test:high_volume",
                "publisher": "Test Publisher",
                "topic": "test"
            }
            
            result = self.event_bus.xadd_json("news.raw", item, "high_volume_test")
            if result:
                successful_publishes += 1
        
        # Should handle most messages (some might be rate limited)
        assert successful_publishes > 0
        
        # Check that messages are in the bus
        recent_items = self.event_bus.get_recent_messages("news.raw", num_messages)
        high_vol_items = [
            item for item in recent_items 
            if "High Volume Test" in item.get("title", "")
        ]
        
        # Should have published items (exact count depends on rate limiting)
        assert len(high_vol_items) > 0
        assert len(high_vol_items) <= successful_publishes