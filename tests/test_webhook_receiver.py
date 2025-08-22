#!/usr/bin/env python3
"""
Tests for webhook receiver functionality.
"""

import hmac
import hashlib
import json
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, Mock

from adapters.webhook_adapter import validate_signature, webhook_router
from storage.schemas import RawItem


class TestSignatureValidation:
    """Test HMAC signature validation"""
    
    def test_valid_github_signature(self):
        """Test GitHub-style signature validation"""
        secret = "my_webhook_secret"
        payload = b'{"test": "data"}'
        
        # Generate valid signature
        signature = hmac.new(
            secret.encode(), payload, hashlib.sha256
        ).hexdigest()
        github_sig = f"sha256={signature}"
        
        assert validate_signature(payload, github_sig, secret) == True
    
    def test_valid_plain_signature(self):
        """Test plain hex signature validation"""
        secret = "my_webhook_secret"
        payload = b'{"test": "data"}'
        
        signature = hmac.new(
            secret.encode(), payload, hashlib.sha256
        ).hexdigest()
        
        assert validate_signature(payload, signature, secret) == True
    
    def test_invalid_signature(self):
        """Test invalid signature rejection"""
        secret = "my_webhook_secret"
        payload = b'{"test": "data"}'
        invalid_sig = "invalid_signature_hash"
        
        assert validate_signature(payload, invalid_sig, secret) == False
    
    def test_empty_secret_or_signature(self):
        """Test handling of empty secret or signature"""
        payload = b'{"test": "data"}'
        
        assert validate_signature(payload, "", "secret") == False
        assert validate_signature(payload, "sig", "") == False
        assert validate_signature(payload, "", "") == False


class TestWebhookReceiver:
    """Test webhook receiver endpoints"""
    
    def setup_method(self):
        """Set up test client"""
        from fastapi import FastAPI
        
        self.app = FastAPI()
        self.app.include_router(webhook_router)
        self.client = TestClient(self.app)
        
        # Mock settings
        self.mock_settings = Mock()
        self.mock_settings.event_driven.webhook_secret = "test_secret"
        self.mock_settings.event_driven.webhook_path = "/push/inbound"
        self.mock_settings.event_driven.default_rate_limit = 10.0
    
    def _create_signed_payload(self, payload_dict: dict, secret: str) -> tuple:
        """Helper to create signed payload"""
        payload_json = json.dumps(payload_dict)
        payload_bytes = payload_json.encode('utf-8')
        
        signature = hmac.new(
            secret.encode(), payload_bytes, hashlib.sha256
        ).hexdigest()
        
        return payload_json, f"sha256={signature}"
    
    @patch('adapters.webhook_adapter.settings')
    @patch('adapters.webhook_adapter.process_webhook_payload')
    def test_valid_webhook_request(self, mock_process, mock_settings):
        """Test valid webhook request with correct signature"""
        mock_settings.event_driven.webhook_secret = "test_secret"
        mock_process.return_value = None  # Async function mock
        
        payload = {
            "title": "Test News Item",
            "url": "https://example.com/test",
            "published": "2024-01-01T12:00:00Z",
            "vendor": "test_vendor"
        }
        
        payload_json, signature = self._create_signed_payload(payload, "test_secret")
        
        response = self.client.post(
            "/push/inbound",
            data=payload_json,
            headers={
                "Content-Type": "application/json",
                "X-Signature": signature,
                "X-Vendor": "test_vendor"
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "accepted"
        assert "processing" in data["message"]
    
    @patch('adapters.webhook_adapter.settings')
    def test_invalid_signature_rejection(self, mock_settings):
        """Test webhook rejection with invalid signature"""
        mock_settings.event_driven.webhook_secret = "test_secret"
        
        payload = {"test": "data"}
        
        response = self.client.post(
            "/push/inbound",
            json=payload,
            headers={"X-Signature": "invalid_signature"}
        )
        
        assert response.status_code == 401
        assert "Invalid signature" in response.json()["detail"]
    
    @patch('adapters.webhook_adapter.settings')
    def test_missing_signature_rejection(self, mock_settings):
        """Test webhook rejection when signature is required but missing"""
        mock_settings.event_driven.webhook_secret = "test_secret"
        
        payload = {"test": "data"}
        
        response = self.client.post("/push/inbound", json=payload)
        
        assert response.status_code == 401
        assert "Missing signature" in response.json()["detail"]
    
    @patch('adapters.webhook_adapter.settings')
    @patch('adapters.webhook_adapter.process_webhook_payload')
    def test_no_signature_when_secret_empty(self, mock_process, mock_settings):
        """Test webhook acceptance when no secret is configured"""
        mock_settings.event_driven.webhook_secret = ""
        mock_process.return_value = None
        
        payload = {"title": "Test News", "url": "https://example.com/test"}
        
        response = self.client.post("/push/inbound", json=payload)
        
        assert response.status_code == 200
    
    def test_invalid_json_rejection(self):
        """Test rejection of invalid JSON payload"""
        response = self.client.post(
            "/push/inbound",
            data="invalid json {",
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 400
        assert "Invalid JSON" in response.json()["detail"]
    
    def test_health_endpoint(self):
        """Test webhook health endpoint"""
        with patch('adapters.webhook_adapter.webhook_stats') as mock_stats:
            mock_stats.get_stats.return_value = {
                'total_requests': 100,
                'valid_requests': 95,
                'success_rate': 0.95
            }
            
            with patch('adapters.webhook_adapter.settings') as mock_settings:
                mock_settings.event_driven.webhook_path = "/push/inbound"
                mock_settings.event_driven.webhook_secret = "secret"
                
                response = self.client.get("/push/health")
                assert response.status_code == 200
                
                data = response.json()
                assert data["webhook_path"] == "/push/inbound"
                assert data["signature_required"] == True
                assert data["status"] in ["ready", "healthy", "degraded", "unhealthy"]
    
    @patch('adapters.webhook_adapter.settings')
    def test_test_endpoint_when_secret_configured(self, mock_settings):
        """Test that test endpoint is disabled when secret is configured"""
        mock_settings.event_driven.webhook_secret = "test_secret"
        
        payload = {"test": "data"}
        response = self.client.post("/push/test", json=payload)
        
        assert response.status_code == 403
        assert "disabled" in response.json()["detail"]
    
    @patch('adapters.webhook_adapter.settings')
    @patch('adapters.webhook_adapter.process_webhook_payload')
    def test_test_endpoint_when_no_secret(self, mock_process, mock_settings):
        """Test that test endpoint works when no secret is configured"""
        mock_settings.event_driven.webhook_secret = ""
        
        # Mock successful processing
        mock_item = RawItem(
            id="test123",
            topic="test",
            title="Test Item",
            link="https://example.com/test",
            published="2024-01-01T12:00:00Z",
            source="webhook:test",
            publisher="Test"
        )
        mock_process.return_value = mock_item
        
        payload = {"title": "Test News", "url": "https://example.com/test"}
        response = self.client.post("/push/test", json=payload)
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "success"
        assert data["item_id"] == "test123"


class TestWebhookPayloadProcessing:
    """Test webhook payload processing and mapping"""
    
    @pytest.mark.asyncio
    async def test_webhook_payload_processing(self):
        """Test processing of webhook payload to RawItem"""
        from adapters.webhook_adapter import process_webhook_payload
        
        payload = {
            "title": "Breaking: Market Update",
            "url": "https://example.com/market-update",
            "published": "2024-01-01T15:30:00Z",
            "description": "Market sees significant changes",
            "category": "finance"
        }
        
        headers = {
            "X-Vendor": "test_vendor",
            "User-Agent": "TestVendor/1.0"
        }
        
        with patch('adapters.webhook_adapter.stream') as mock_stream:
            mock_stream.xadd_json.return_value = True
            
            result = await process_webhook_payload(payload, headers, "test_vendor")
            
            assert result is not None
            assert result.title == "Breaking: Market Update"
            assert result.link == "https://example.com/market-update"
            assert result.source == "webhook:test_vendor"
            assert result.topic == "finance"
            
            # Verify published to event bus
            mock_stream.xadd_json.assert_called_once()
            call_args = mock_stream.xadd_json.call_args
            assert call_args[0][0] == "news.raw"
            assert call_args[1]['source'] == "webhook:test_vendor"
    
    @pytest.mark.asyncio
    async def test_webhook_duplicate_filtering(self):
        """Test that webhook duplicates are filtered by event bus"""
        from adapters.webhook_adapter import process_webhook_payload
        
        payload = {
            "title": "Duplicate News",
            "url": "https://example.com/duplicate",
            "published": "2024-01-01T12:00:00Z"
        }
        
        headers = {"X-Vendor": "test"}
        
        with patch('adapters.webhook_adapter.stream') as mock_stream:
            # First call succeeds
            mock_stream.xadd_json.return_value = True
            result1 = await process_webhook_payload(payload, headers, "test")
            assert result1 is not None
            
            # Second call filtered (duplicate)
            mock_stream.xadd_json.return_value = False
            result2 = await process_webhook_payload(payload, headers, "test")
            assert result2 is not None  # Item created but filtered by bus
    
    @pytest.mark.asyncio 
    async def test_webhook_invalid_payload_handling(self):
        """Test handling of invalid webhook payloads"""
        from adapters.webhook_adapter import process_webhook_payload
        
        # Missing required fields
        invalid_payload = {
            "description": "Missing title and url"
        }
        
        headers = {"X-Vendor": "test"}
        
        with patch('adapters.webhook_adapter.stream'):
            result = await process_webhook_payload(invalid_payload, headers, "test")
            # Should return None for invalid payload
            assert result is None