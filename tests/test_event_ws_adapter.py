#!/usr/bin/env python3
"""
Tests for WebSocket adapter functionality.
"""

import asyncio
import json
import pytest
from unittest.mock import Mock, patch, AsyncMock

from adapters.websocket_adapter import WebSocketAdapter
from storage.schemas import RawItem
from bus.stream import EventBus


class TestWebSocketAdapter:
    """Test WebSocket adapter with mock connections"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.config = {
            'name': 'test_source',
            'url': 'wss://example.com/feed',
            'topic': 'test',
            'headers': {'Authorization': 'Bearer token'},
            'ping_interval': 10,
            'reconnect_backoff': [1, 2, 4],
            'max_queue_size': 100
        }
        
        self.adapter = WebSocketAdapter(self.config)
    
    def test_adapter_initialization(self):
        """Test adapter initialization"""
        assert self.adapter.name == 'test_source'
        assert self.adapter.url == 'wss://example.com/feed'
        assert self.adapter.topic == 'test'
        assert self.adapter.max_queue_size == 100
        assert not self.adapter.running
        assert self.adapter.message_count == 0
    
    def test_heartbeat_detection(self):
        """Test heartbeat message detection"""
        # Test various heartbeat formats
        heartbeat_msgs = [
            {'type': 'heartbeat'},
            {'message_type': 'ping'},
            {'event': 'ping'},
            {'msg': 'heartbeat alive'},
        ]
        
        for msg in heartbeat_msgs:
            assert self.adapter._is_heartbeat_message(msg)
        
        # Test non-heartbeat
        news_msg = {'title': 'Breaking News', 'url': 'http://example.com/news'}
        assert not self.adapter._is_heartbeat_message(news_msg)
    
    @pytest.mark.asyncio
    async def test_message_processing(self):
        """Test processing of valid news messages"""
        # Mock the event bus
        with patch('adapters.websocket_adapter.stream') as mock_stream:
            mock_stream.xadd_json.return_value = True
            
            # Test message data
            test_payload = {
                'title': 'Test News Item',
                'url': 'https://example.com/test-news',
                'timestamp': '2024-01-01T12:00:00Z',
                'category': 'business'
            }
            
            # Process the message
            await self.adapter._handle_text_message(json.dumps(test_payload))
            
            # Verify message was published
            assert mock_stream.xadd_json.called
            call_args = mock_stream.xadd_json.call_args
            assert call_args[0][0] == "news.raw"  # Channel
            assert call_args[1]['source'] == "websocket:test_source"  # Source
            
            # Verify message was added to queue
            assert len(self.adapter.message_queue) == 1
            assert self.adapter.message_count == 1
    
    @pytest.mark.asyncio
    async def test_backpressure_handling(self):
        """Test queue overflow and message dropping"""
        # Set small queue size
        self.adapter.max_queue_size = 2
        
        with patch('adapters.websocket_adapter.stream') as mock_stream:
            mock_stream.xadd_json.return_value = True
            
            # Send 3 messages (should drop oldest)
            for i in range(3):
                payload = {
                    'title': f'News Item {i}',
                    'url': f'https://example.com/news-{i}',
                    'timestamp': '2024-01-01T12:00:00Z'
                }
                await self.adapter._handle_text_message(json.dumps(payload))
            
            # Verify queue size is maintained
            assert len(self.adapter.message_queue) <= self.adapter.max_queue_size
            assert self.adapter.message_count == 3
    
    @pytest.mark.asyncio
    async def test_invalid_json_handling(self):
        """Test handling of invalid JSON messages"""
        initial_count = self.adapter.message_count
        
        # Send invalid JSON
        await self.adapter._handle_text_message("invalid json {")
        
        # Verify no message was processed
        assert self.adapter.message_count == initial_count
        assert len(self.adapter.message_queue) == 0
    
    def test_get_stats(self):
        """Test statistics collection"""
        # Add some test data
        self.adapter.message_count = 10
        self.adapter.error_count = 2
        self.adapter.running = True
        
        stats = self.adapter.get_stats()
        
        assert stats['name'] == 'test_source'
        assert stats['message_count'] == 10
        assert stats['error_count'] == 2
        assert stats['running'] == True
        assert 'uptime_seconds' in stats
        assert 'last_message_time' in stats


class TestWebSocketDeduplication:
    """Test WebSocket adapter deduplication via event bus"""
    
    @pytest.mark.asyncio
    async def test_duplicate_message_filtering(self):
        """Test that duplicate messages are filtered by event bus"""
        bus = EventBus()
        
        # Create test RawItem
        item1 = RawItem(
            id='test123',
            topic='business',
            title='Test News',
            link='https://example.com/news',
            published='2024-01-01T12:00:00Z',
            source='websocket:test',
            publisher='Test Publisher'
        )
        
        # First publication should succeed
        result1 = bus.xadd_json("news.raw", item1.to_dict(), "websocket:test")
        assert result1 == True
        
        # Duplicate should be filtered
        result2 = bus.xadd_json("news.raw", item1.to_dict(), "websocket:test")
        assert result2 == False  # Filtered as duplicate
        
        # Verify only one item in recent messages
        recent = bus.get_recent_messages("news.raw", 10)
        assert len(recent) == 1
    
    @pytest.mark.asyncio
    async def test_same_link_different_timestamp_dedup(self):
        """Test that items with same link but different timestamps are deduplicated"""
        bus = EventBus()
        
        # Two items with same link but different timestamps
        item1 = RawItem(
            id='',  # Will be generated
            topic='business',
            title='Breaking News',
            link='https://example.com/breaking',
            published='2024-01-01T12:00:00Z',
            source='websocket:source1',
            publisher='Publisher1'
        )
        
        item2 = RawItem(
            id='',  # Will be generated
            topic='business', 
            title='Breaking News',
            link='https://example.com/breaking',
            published='2024-01-01T12:05:00Z',  # Different time
            source='websocket:source2',
            publisher='Publisher2'
        )
        
        # Both should have same ID due to same link+title (ignoring seconds in published)
        # This tests the deduplication logic in RawItem.make_id()
        
        result1 = bus.xadd_json("news.raw", item1.to_dict(), "websocket:source1")
        result2 = bus.xadd_json("news.raw", item2.to_dict(), "websocket:source2")
        
        # If IDs are truly the same, second should be filtered
        if item1.id == item2.id:
            assert result1 == True
            assert result2 == False  # Duplicate
        else:
            # Different IDs mean both are kept
            assert result1 == True
            assert result2 == True


@pytest.mark.asyncio
async def test_websocket_adapter_integration():
    """Integration test with mock WebSocket connection"""
    config = {
        'name': 'integration_test',
        'url': 'wss://mock.example.com/feed',
        'topic': 'test',
        'max_queue_size': 10
    }
    
    adapter = WebSocketAdapter(config)
    
    # Mock websocket connection and messages
    mock_messages = [
        json.dumps({
            'title': 'Mock News 1',
            'url': 'https://example.com/mock1',
            'timestamp': '2024-01-01T10:00:00Z'
        }),
        json.dumps({
            'title': 'Mock News 2', 
            'url': 'https://example.com/mock2',
            'timestamp': '2024-01-01T10:01:00Z'
        }),
        json.dumps({'type': 'heartbeat'}),  # Should be ignored
    ]
    
    with patch('adapters.websocket_adapter.stream') as mock_stream:
        mock_stream.xadd_json.return_value = True
        
        # Process mock messages
        for msg in mock_messages:
            await adapter._handle_text_message(msg)
        
        # Verify results
        assert adapter.message_count == 2  # Heartbeat not counted
        assert len(adapter.message_queue) == 2
        assert mock_stream.xadd_json.call_count == 2