#!/usr/bin/env python3
"""
MI-3 News Scraper - Webhook Adapter
FastAPI webhook receiver with HMAC validation and vendor payload mapping.
"""

import hashlib
import hmac
import json
import logging
import time
from typing import Dict, Any, Optional, List

from fastapi import APIRouter, Request, HTTPException, Header, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from bus.stream import stream
from adapters.mappers import map_webhook_payload_to_raw
from storage.schemas import RawItem, validate_raw_item
from config.settings import settings

logger = logging.getLogger(__name__)

class WebhookStats:
    """Track webhook statistics"""
    
    def __init__(self):
        self.total_requests = 0
        self.valid_requests = 0
        self.invalid_signatures = 0
        self.mapping_errors = 0
        self.published_items = 0
        self.duplicate_items = 0
        self.rate_limited = 0
        self.start_time = time.time()
        self.vendor_stats = {}
    
    def record_request(self, vendor: str = "unknown"):
        """Record incoming request"""
        self.total_requests += 1
        if vendor not in self.vendor_stats:
            self.vendor_stats[vendor] = {
                'requests': 0,
                'valid': 0,
                'errors': 0
            }
        self.vendor_stats[vendor]['requests'] += 1
    
    def record_valid(self, vendor: str = "unknown"):
        """Record valid request"""
        self.valid_requests += 1
        self.vendor_stats[vendor]['valid'] += 1
    
    def record_error(self, error_type: str, vendor: str = "unknown"):
        """Record error by type"""
        if error_type == 'invalid_signature':
            self.invalid_signatures += 1
        elif error_type == 'mapping_error':
            self.mapping_errors += 1
        elif error_type == 'rate_limited':
            self.rate_limited += 1
        
        if vendor in self.vendor_stats:
            self.vendor_stats[vendor]['errors'] += 1
    
    def record_published(self):
        """Record successful publication"""
        self.published_items += 1
    
    def record_duplicate(self):
        """Record duplicate item"""
        self.duplicate_items += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics summary"""
        uptime = time.time() - self.start_time
        
        return {
            'uptime_seconds': uptime,
            'total_requests': self.total_requests,
            'valid_requests': self.valid_requests,
            'success_rate': self.valid_requests / max(1, self.total_requests),
            'invalid_signatures': self.invalid_signatures,
            'mapping_errors': self.mapping_errors,
            'published_items': self.published_items,
            'duplicate_items': self.duplicate_items,
            'rate_limited': self.rate_limited,
            'requests_per_minute': self.total_requests / max(1, uptime / 60),
            'vendor_stats': self.vendor_stats
        }

# Global stats instance
webhook_stats = WebhookStats()

class WebhookResponse(BaseModel):
    """Standard webhook response"""
    status: str
    message: str
    item_id: Optional[str] = None

def validate_signature(payload_body: bytes, signature: str, secret: str) -> bool:
    """
    Validate HMAC-SHA256 webhook signature.
    
    Supports multiple signature formats:
    - GitHub: sha256=<hex>
    - Generic: <hex>
    - Slack: v0=<hex>
    
    Args:
        payload_body: Raw request body bytes
        signature: Signature header value
        secret: Webhook secret key
        
    Returns:
        True if signature is valid
    """
    if not secret or not signature:
        return False
    
    try:
        # Extract hex hash from signature (remove prefix if present)
        if signature.startswith('sha256='):
            provided_hash = signature[7:]
        elif signature.startswith('v0='):
            provided_hash = signature[3:]
        else:
            provided_hash = signature
        
        # Calculate expected hash
        expected_hash = hmac.new(
            secret.encode('utf-8'),
            payload_body,
            hashlib.sha256
        ).hexdigest()
        
        # Constant-time comparison
        return hmac.compare_digest(expected_hash, provided_hash)
        
    except Exception as e:
        logger.error(f"Signature validation error: {e}")
        return False

def extract_vendor_from_headers(headers: Dict[str, str]) -> str:
    """Extract vendor name from headers"""
    # Try various header patterns
    vendor_headers = [
        'X-Vendor',
        'X-Source', 
        'User-Agent',
        'X-GitHub-Event',  # GitHub specific
        'X-Slack-Signature'  # Slack specific
    ]
    
    for header in vendor_headers:
        value = headers.get(header, '').lower()
        if value:
            # Extract meaningful vendor name
            if 'github' in value:
                return 'github'
            elif 'slack' in value:
                return 'slack'
            elif 'reuters' in value:
                return 'reuters'
            elif 'bloomberg' in value:
                return 'bloomberg'
            elif 'cnbc' in value or 'nbc' in value:
                return 'cnbc'
            elif 'yahoo' in value:
                return 'yahoo'
            else:
                return value.split('/')[0]  # Take first part of User-Agent
    
    return 'unknown'

async def process_webhook_payload(
    payload: Dict[str, Any], 
    headers: Dict[str, str],
    vendor: str
) -> Optional[RawItem]:
    """
    Process webhook payload in background.
    
    Args:
        payload: JSON payload from webhook
        headers: HTTP headers
        vendor: Detected vendor name
        
    Returns:
        RawItem if successful, None if failed
    """
    try:
        # Map payload to RawItem
        raw_item = map_webhook_payload_to_raw(payload, headers)
        if not raw_item:
            logger.warning(f"Webhook mapping returned None for vendor {vendor}")
            webhook_stats.record_error('mapping_error', vendor)
            return None
        
        # Validate RawItem
        item_dict = raw_item.to_dict()
        if not validate_raw_item(item_dict):
            logger.error(f"Invalid RawItem from webhook {vendor}: {item_dict}")
            webhook_stats.record_error('mapping_error', vendor)
            return None
        
        # Publish to event bus
        success = stream.xadd_json(
            "news.raw",
            item_dict,
            source=f"webhook:{vendor}"
        )
        
        if success:
            webhook_stats.record_published()
            logger.info(f"Published webhook item {raw_item.id} from {vendor}")
        else:
            webhook_stats.record_duplicate()
            logger.debug(f"Webhook item {raw_item.id} filtered (duplicate/rate limit)")
        
        return raw_item
        
    except Exception as e:
        logger.error(f"Error processing webhook from {vendor}: {e}")
        webhook_stats.record_error('mapping_error', vendor)
        return None

# Create FastAPI router for webhook endpoints
webhook_router = APIRouter(prefix="/push", tags=["webhooks"])

@webhook_router.post("/inbound")
async def receive_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_signature: Optional[str] = Header(None, alias="X-Signature"),
    x_hub_signature_256: Optional[str] = Header(None, alias="X-Hub-Signature-256"), # GitHub
    x_slack_signature: Optional[str] = Header(None, alias="X-Slack-Signature"),     # Slack
    x_vendor: Optional[str] = Header(None, alias="X-Vendor")
):
    """
    Main webhook receiver endpoint.
    
    Supports multiple signature header formats and vendor identification.
    Processes payloads asynchronously to return 200 quickly.
    """
    
    # Get raw request body for signature validation
    body = await request.body()
    headers = dict(request.headers)
    
    # Detect vendor from headers
    vendor = x_vendor or extract_vendor_from_headers(headers)
    webhook_stats.record_request(vendor)
    
    # Get signature from various header formats
    signature = (
        x_signature or 
        x_hub_signature_256 or 
        x_slack_signature or
        headers.get('authorization', '')
    )
    
    # Validate signature if secret is configured
    webhook_secret = settings.event_driven.webhook_secret
    if webhook_secret:
        if not signature:
            logger.warning(f"Missing signature for webhook from {vendor}")
            webhook_stats.record_error('invalid_signature', vendor)
            raise HTTPException(status_code=401, detail="Missing signature")
        
        if not validate_signature(body, signature, webhook_secret):
            logger.warning(f"Invalid signature for webhook from {vendor}")
            webhook_stats.record_error('invalid_signature', vendor)
            raise HTTPException(status_code=401, detail="Invalid signature")
    
    # Parse JSON payload
    try:
        payload = await request.json()
    except Exception as e:
        logger.warning(f"Invalid JSON from webhook {vendor}: {e}")
        webhook_stats.record_error('mapping_error', vendor)
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    
    # Record valid request
    webhook_stats.record_valid(vendor)
    
    # Process payload in background (non-blocking)
    background_tasks.add_task(
        process_webhook_payload,
        payload,
        headers,
        vendor
    )
    
    # Return 200 immediately
    return WebhookResponse(
        status="accepted",
        message="Webhook received and processing"
    )

@webhook_router.get("/health")
async def webhook_health():
    """Health check endpoint for webhook receiver"""
    stats = webhook_stats.get_stats()
    
    # Determine health status
    if stats['total_requests'] == 0:
        status = "ready"
    elif stats['success_rate'] > 0.8:
        status = "healthy"
    elif stats['success_rate'] > 0.5:
        status = "degraded"
    else:
        status = "unhealthy"
    
    return {
        "status": status,
        "webhook_path": settings.event_driven.webhook_path,
        "signature_required": bool(settings.event_driven.webhook_secret),
        "stats": stats
    }

@webhook_router.get("/stats")
async def webhook_statistics():
    """Detailed webhook statistics"""
    return {
        "webhook_stats": webhook_stats.get_stats(),
        "event_bus_stats": stream.get_stats(),
        "configuration": {
            "webhook_path": settings.event_driven.webhook_path,
            "signature_required": bool(settings.event_driven.webhook_secret),
            "rate_limit": settings.event_driven.default_rate_limit
        }
    }

@webhook_router.post("/test")
async def test_webhook(
    payload: Dict[str, Any],
    x_vendor: Optional[str] = Header("test")
):
    """
    Test endpoint for webhook development (no signature validation).
    Only available when webhook_secret is not configured.
    """
    
    if settings.event_driven.webhook_secret:
        raise HTTPException(
            status_code=403, 
            detail="Test endpoint disabled when webhook secret is configured"
        )
    
    vendor = x_vendor or "test"
    headers = {"X-Vendor": vendor}
    
    # Process immediately (not in background for testing)
    raw_item = await process_webhook_payload(payload, headers, vendor)
    
    if raw_item:
        return WebhookResponse(
            status="success",
            message="Test webhook processed successfully",
            item_id=raw_item.id
        )
    else:
        return WebhookResponse(
            status="error",
            message="Failed to process test webhook"
        )

def get_webhook_router() -> APIRouter:
    """Get configured webhook router for inclusion in main FastAPI app"""
    return webhook_router

def setup_webhook_routes(app):
    """Set up webhook routes in FastAPI app"""
    if not settings.event_driven.enabled:
        logger.info("Event-driven webhooks disabled")
        return
    
    # Mount webhook router
    app.include_router(webhook_router)
    logger.info(f"Webhook receiver mounted at {settings.event_driven.webhook_path}")
    
    # Log configuration
    logger.info(f"Webhook secret configured: {bool(settings.event_driven.webhook_secret)}")
    logger.info(f"Webhook rate limit: {settings.event_driven.default_rate_limit} req/sec")

if __name__ == "__main__":
    # Standalone webhook server for testing
    import uvicorn
    from fastapi import FastAPI
    
    app = FastAPI(title="MI-3 Webhook Receiver", version="1.0.0")
    app.include_router(webhook_router)
    
    print("Starting standalone webhook server...")
    print(f"Webhook endpoint: http://127.0.0.1:8001{settings.event_driven.webhook_path}")
    print(f"Health check: http://127.0.0.1:8001/push/health")
    
    uvicorn.run(app, host="127.0.0.1", port=8001, log_level="info")