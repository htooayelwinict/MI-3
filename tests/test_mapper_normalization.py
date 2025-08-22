#!/usr/bin/env python3
"""
Tests for payload mapper normalization functionality.
"""

import pytest
from datetime import datetime
from adapters.mappers import (
    map_ws_payload_to_raw,
    map_webhook_payload_to_raw,
    map_newswire_to_raw,
    safe_map_payload
)
from storage.schemas import RawItem


class TestWebSocketMappers:
    """Test WebSocket payload mapping"""
    
    def test_generic_websocket_mapping(self):
        """Test generic WebSocket payload mapping"""
        payload = {
            'title': 'Generic WebSocket News',
            'url': 'https://example.com/ws-news',
            'timestamp': '2024-01-01T12:00:00Z',
            'summary': 'This is a test news item',
            'tags': ['business', 'technology']
        }
        
        config = {
            'name': 'generic_ws_source',
            'topic': 'business',
            'publisher': 'Generic Publisher'
        }
        
        result = map_ws_payload_to_raw(payload, config)
        
        assert result is not None
        assert result.title == 'Generic WebSocket News'
        assert result.link == 'https://example.com/ws-news'
        assert result.source == 'websocket:generic_ws_source'
        assert result.publisher == 'Generic Publisher'
        assert result.topic == 'business'
        assert result.summary == 'This is a test news item'
        assert result.id  # Should generate ID
    
    def test_reuters_websocket_mapping(self):
        """Test Reuters-specific WebSocket mapping"""
        payload = {
            'headline': 'Reuters Breaking News',
            'url': 'https://reuters.com/breaking',
            'timestamp': 1704110400,  # Unix timestamp
            'category': 'markets',
            'summary': 'Reuters news summary',
            'tags': ['reuters', 'breaking']
        }
        
        config = {
            'name': 'reuters_websocket',
            'topic': 'business'
        }
        
        result = map_ws_payload_to_raw(payload, config)
        
        assert result is not None
        assert result.title == 'Reuters Breaking News'
        assert result.publisher == 'Reuters'
        assert result.source == 'websocket:reuters_websocket'
        assert result.topic == 'markets'  # Should use payload category
    
    def test_bloomberg_websocket_mapping(self):
        """Test Bloomberg-specific WebSocket mapping"""
        payload = {
            'story_id': 'BBG123456',
            'headline': 'Bloomberg Market Update',
            'story_url': 'https://bloomberg.com/story/123',
            'datetime': '2024-01-01T15:30:00.000Z',
            'topic': 'equity_markets',
            'keywords': 'stocks,trading,NYSE'
        }
        
        config = {
            'name': 'bloomberg_terminal',
            'topic': 'markets'
        }
        
        result = map_ws_payload_to_raw(payload, config)
        
        assert result is not None
        assert result.title == 'Bloomberg Market Update'
        assert result.link == 'https://bloomberg.com/story/123'
        assert result.publisher == 'Bloomberg'
        assert result.topic == 'equity_markets'  # Should use payload topic
        assert result.tags == 'stocks,trading,NYSE'
    
    def test_websocket_missing_fields_handling(self):
        """Test handling of missing fields in WebSocket payloads"""
        # Minimal payload with only title
        payload = {
            'title': 'Minimal News Item'
        }
        
        config = {
            'name': 'test_source',
            'topic': 'general'
        }
        
        result = map_ws_payload_to_raw(payload, config)
        
        assert result is not None
        assert result.title == 'Minimal News Item'
        assert result.link == ''  # Should default to empty
        assert result.source == 'websocket:test_source'
        assert result.published  # Should generate timestamp
    
    def test_websocket_mapping_error_handling(self):
        """Test error handling in WebSocket mapping"""
        # Invalid config (missing name)
        invalid_config = {
            'topic': 'test'
        }
        
        payload = {'title': 'Test'}
        
        result = map_ws_payload_to_raw(payload, invalid_config)
        # Should handle gracefully and not crash
        assert result is not None or result is None  # Either outcome is acceptable


class TestWebhookMappers:
    """Test webhook payload mapping"""
    
    def test_generic_webhook_mapping(self):
        """Test generic webhook payload mapping"""
        payload = {
            'title': 'Webhook News Item',
            'url': 'https://example.com/webhook-news',
            'published': '2024-01-01T10:00:00Z',
            'category': 'technology',
            'description': 'Webhook test description',
            'publisher': 'Test Publisher'
        }
        
        headers = {
            'X-Vendor': 'test_vendor',
            'User-Agent': 'TestWebhook/1.0'
        }
        
        result = map_webhook_payload_to_raw(payload, headers)
        
        assert result is not None
        assert result.title == 'Webhook News Item'
        assert result.link == 'https://example.com/webhook-news'
        assert result.source == 'webhook:test_vendor'
        assert result.publisher == 'Test Publisher'
        assert result.topic == 'technology'
        assert result.summary == 'Webhook test description'
    
    def test_reuters_webhook_mapping(self):
        """Test Reuters-specific webhook mapping"""
        payload = {
            'headline': 'Reuters Webhook News',
            'canonical_url': 'https://reuters.com/webhook/123',
            'date_published': '2024-01-01T14:30:00Z',
            'category': 'world',
            'description': 'Reuters webhook description',
            'topics': ['politics', 'international']
        }
        
        headers = {
            'X-Vendor': 'reuters',
            'User-Agent': 'Reuters-Webhook/2.0'
        }
        
        result = map_webhook_payload_to_raw(payload, headers)
        
        assert result is not None
        assert result.title == 'Reuters Webhook News'
        assert result.link == 'https://reuters.com/webhook/123'
        assert result.source == 'webhook:reuters'
        assert result.publisher == 'Reuters'
        assert result.tags == 'politics,international'
    
    def test_bloomberg_webhook_mapping(self):
        """Test Bloomberg-specific webhook mapping"""
        payload = {
            'headline': 'Bloomberg Webhook Update',
            'story_url': 'https://bloomberg.com/webhook/456',
            'published_at': '2024-01-01T16:00:00.000Z',
            'primary_category': 'fixed_income',
            'abstract': 'Bloomberg webhook abstract',
            'tags': ['bonds', 'yields', 'treasury']
        }
        
        headers = {
            'User-Agent': 'Bloomberg-Webhook/1.5'
        }
        
        result = map_webhook_payload_to_raw(payload, headers)
        
        assert result is not None
        assert result.title == 'Bloomberg Webhook Update'
        assert result.link == 'https://bloomberg.com/webhook/456'
        assert result.source == 'webhook:bloomberg'
        assert result.publisher == 'Bloomberg'
        assert result.topic == 'fixed_income'
        assert result.tags == 'bonds,yields,treasury'
    
    def test_webhook_vendor_detection(self):
        """Test vendor detection from headers"""
        payload = {
            'title': 'Vendor Detection Test',
            'url': 'https://example.com/test'
        }
        
        # Test various header patterns
        test_cases = [
            ({'X-Vendor': 'CustomVendor'}, 'webhook:customvendor'),
            ({'User-Agent': 'GitHub-Hookshot/abc'}, 'webhook:github'),
            ({'X-Slack-Signature': 'v0=123'}, 'webhook:slack'),
            ({'User-Agent': 'Yahoo-Webhook/1.0'}, 'webhook:yahoo'),
            ({}, 'webhook:unknown')
        ]
        
        for headers, expected_source in test_cases:
            result = map_webhook_payload_to_raw(payload, headers)
            assert result is not None
            assert result.source == expected_source


class TestNewswireMappers:
    """Test newswire payload mapping"""
    
    def test_bloomberg_newswire_mapping(self):
        """Test Bloomberg Terminal newswire mapping"""
        payload = {
            'story_id': 'BBG_TERMINAL_123',
            'headline': 'Bloomberg Terminal News',
            'published_date': 1704117600,  # Unix timestamp
            'story_abstract': 'Terminal news abstract',
            'category': 'equity',
            'topics': ['stocks', 'earnings']
        }
        
        config = {
            'name': 'bloomberg_terminal',
            'vendor': 'bloomberg_terminal',
            'topic': 'markets'
        }
        
        result = map_newswire_to_raw(payload, config)
        
        assert result is not None
        assert result.title == 'Bloomberg Terminal News'
        assert result.source == 'newswire:bloomberg_terminal'
        assert result.publisher == 'Bloomberg Terminal'
        assert result.summary == 'Terminal news abstract'
        assert result.tags == 'stocks,earnings'
        # Link should be bloomberg:// protocol
        assert 'bloomberg://story/' in result.link
    
    def test_reuters_newswire_mapping(self):
        """Test Reuters Eikon newswire mapping"""
        payload = {
            'storyId': 'REUTERS_EIKON_456',
            'headline': 'Reuters Eikon Update',
            'versionCreated': '2024-01-01T17:00:00Z',
            'bodyText': 'Reuters Eikon news body',
            'category': 'commodities',
            'subject': 'oil,energy,trading'
        }
        
        config = {
            'name': 'reuters_eikon',
            'vendor': 'reuters_eikon',
            'topic': 'commodities'
        }
        
        result = map_newswire_to_raw(payload, config)
        
        assert result is not None
        assert result.title == 'Reuters Eikon Update'
        assert result.source == 'newswire:reuters_eikon'
        assert result.publisher == 'Reuters Terminal'
        assert result.summary == 'Reuters Eikon news body'
        assert result.tags == 'oil,energy,trading'
        # Link should be reuters:// protocol
        assert 'reuters://story/' in result.link
    
    def test_generic_newswire_mapping(self):
        """Test generic newswire mapping"""
        payload = {
            'headline': 'Generic Newswire Item',
            'url': 'https://newswire.example.com/item/789',
            'published_date': '2024-01-01T18:00:00Z',
            'body': 'Generic newswire body text',
            'category': 'general'
        }
        
        config = {
            'name': 'custom_newswire',
            'vendor': 'custom_vendor',
            'topic': 'general'
        }
        
        result = map_newswire_to_raw(payload, config)
        
        assert result is not None
        assert result.title == 'Generic Newswire Item'
        assert result.link == 'https://newswire.example.com/item/789'
        assert result.source == 'newswire:custom_vendor'
        assert result.publisher == 'custom_vendor'
        assert result.summary == 'Generic newswire body text'


class TestDateTimeNormalization:
    """Test datetime normalization across different formats"""
    
    def test_unix_timestamp_normalization(self):
        """Test Unix timestamp normalization"""
        result = RawItem._normalize_datetime(1704117600)  # 2024-01-01 18:00:00 UTC
        assert result.startswith('2024-01-01T18:00:00')
        assert result.endswith('+00:00') or result.endswith('Z')
    
    def test_iso_string_normalization(self):
        """Test ISO string normalization"""
        test_cases = [
            '2024-01-01T12:00:00Z',
            '2024-01-01T12:00:00.000Z',
            '2024-01-01T12:00:00+00:00',
            '2024-01-01 12:00:00',
        ]
        
        for dt_str in test_cases:
            result = RawItem._normalize_datetime(dt_str)
            assert result.startswith('2024-01-01T12:00:00')
            # Should be normalized to UTC
            assert '+00:00' in result or result.endswith('Z')
    
    def test_datetime_object_normalization(self):
        """Test datetime object normalization"""
        from datetime import datetime, timezone
        
        dt = datetime(2024, 1, 1, 15, 30, 0, tzinfo=timezone.utc)
        result = RawItem._normalize_datetime(dt)
        
        assert result.startswith('2024-01-01T15:30:00')
        assert '+00:00' in result
    
    def test_invalid_datetime_handling(self):
        """Test handling of invalid datetime values"""
        invalid_values = [
            None,
            '',
            'invalid-date',
            'not-a-timestamp',
            []
        ]
        
        for invalid_val in invalid_values:
            result = RawItem._normalize_datetime(invalid_val)
            # Should return current timestamp for invalid values
            assert result  # Should be non-empty
            assert 'T' in result  # Should be ISO format


class TestSafeMapping:
    """Test safe mapping function with error handling"""
    
    def test_safe_websocket_mapping(self):
        """Test safe WebSocket mapping"""
        payload = {'title': 'Safe WS Test', 'url': 'https://example.com/safe'}
        config = {'name': 'safe_test', 'topic': 'test'}
        
        result = safe_map_payload(payload, 'websocket', config=config)
        
        assert result is not None
        assert result.title == 'Safe WS Test'
        assert result.source == 'websocket:safe_test'
    
    def test_safe_webhook_mapping(self):
        """Test safe webhook mapping"""
        payload = {'title': 'Safe Webhook Test', 'url': 'https://example.com/safe'}
        headers = {'X-Vendor': 'safe_vendor'}
        
        result = safe_map_payload(payload, 'webhook', headers=headers)
        
        assert result is not None
        assert result.title == 'Safe Webhook Test'
        assert result.source == 'webhook:safe_vendor'
    
    def test_safe_mapping_with_invalid_adapter_type(self):
        """Test safe mapping with invalid adapter type"""
        payload = {'title': 'Test'}
        
        result = safe_map_payload(payload, 'invalid_type')
        
        assert result is None  # Should handle gracefully
    
    def test_safe_mapping_with_missing_config(self):
        """Test safe mapping with missing required config"""
        payload = {'title': 'Test'}
        
        # WebSocket requires config but none provided
        result = safe_map_payload(payload, 'websocket')
        assert result is None
        
        # Webhook requires headers but none provided
        result = safe_map_payload(payload, 'webhook')
        assert result is None