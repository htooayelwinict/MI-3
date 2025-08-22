#!/usr/bin/env python3
"""
MI-3 News Scraper - Real-time API Hub
FastAPI server providing real-time access to news data from RSS, WebSocket, webhook, and newswire sources.
"""

import json
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="MI-3 News Scraper API",
    description="Real-time news feed API",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure as needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@dataclass
class UnifiedDataManager:
    """
    Manages access to unified feed data from multiple sources.
    
    Combines data from:
    - RSS feeds (legacy file-based)
    - Event bus (WebSocket, webhook, newswire via news.raw channel)
    """
    rss_data_file: Path
    _last_modified: Optional[float] = None
    _cached_rss_data: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        self.rss_data_file = Path(self.rss_data_file)
    
    def _load_rss_data(self) -> Dict[str, Any]:
        """Load RSS data from JSON file with caching"""
        if not self.rss_data_file.exists():
            return {"items": [], "last_updated": None, "total_items": 0}
        
        try:
            current_mtime = self.rss_data_file.stat().st_mtime
            
            # Check if we need to reload
            if (self._cached_rss_data is None or 
                self._last_modified is None or 
                current_mtime > self._last_modified):
                
                with open(self.rss_data_file, 'r', encoding='utf-8') as f:
                    self._cached_rss_data = json.load(f)
                self._last_modified = current_mtime
                logger.debug("Reloaded RSS data from file")
            
            return self._cached_rss_data
            
        except Exception as e:
            logger.error(f"Error loading RSS data: {e}")
            return {"items": [], "last_updated": None, "total_items": 0}
    
    def _get_event_bus_items(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent items from event bus"""
        try:
            from bus.stream import get_recent_raw_news
            return get_recent_raw_news(limit)
        except Exception as e:
            logger.error(f"Error getting event bus items: {e}")
            return []
    
    def get_latest_items(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get latest items from all sources (RSS + event bus)"""
        # Get RSS items
        rss_data = self._load_rss_data()
        rss_items = rss_data.get("items", [])
        
        # Get event bus items  
        event_items = self._get_event_bus_items(limit * 2)
        
        # Combine and deduplicate by ID
        all_items = []
        seen_ids = set()
        
        # Add event bus items first (more real-time)
        for item in event_items:
            item_id = item.get('id')
            if item_id and item_id not in seen_ids:
                all_items.append(item)
                seen_ids.add(item_id)
        
        # Add RSS items (fallback to legacy)
        for item in rss_items:
            item_id = item.get('id')
            if item_id and item_id not in seen_ids:
                # Convert legacy format if needed
                if 'category' in item and 'topic' not in item:
                    item['topic'] = item['category']
                all_items.append(item)
                seen_ids.add(item_id)
        
        # Sort by published date (most recent first)
        try:
            all_items.sort(key=lambda x: x.get('published', ''), reverse=True)
        except Exception as e:
            logger.warning(f"Error sorting items: {e}")
        
        return all_items[:limit]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics from all sources"""
        rss_data = self._load_rss_data()
        event_items = self._get_event_bus_items(1000)  # Sample for stats
        
        # Combine source counts
        rss_sources = set(item.get("source", "") for item in rss_data.get("items", []))
        event_sources = set(item.get("source", "") for item in event_items)
        all_sources = rss_sources | event_sources
        
        # Get event bus stats
        try:
            from bus.stream import stream
            bus_stats = stream.get_stats()
        except Exception:
            bus_stats = {}
        
        return {
            "rss_items": rss_data.get("total_items", 0),
            "event_bus_items": len(event_items),
            "total_sources": len(all_sources),
            "rss_last_updated": rss_data.get("last_updated"),
            "event_bus_stats": bus_stats,
            "unified_sources": list(all_sources)
        }

# Global data manager instance
data_manager = None

def get_data_manager() -> UnifiedDataManager:
    """Get unified data manager instance"""
    global data_manager
    if data_manager is None:
        from config.settings import settings
        data_manager = UnifiedDataManager(rss_data_file=settings.realtime.feed_data_file)
    return data_manager

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    from config.settings import settings
    
    # Initialize event bus
    try:
        from bus.stream import stream
        logger.info("Event bus initialized")
    except Exception as e:
        logger.error(f"Failed to initialize event bus: {e}")
    
    # Setup webhook routes if event-driven is enabled
    if settings.event_driven.enabled:
        try:
            from adapters.webhook_adapter import setup_webhook_routes
            setup_webhook_routes(app)
        except Exception as e:
            logger.error(f"Failed to setup webhook routes: {e}")
    
    # Log startup status
    if not settings.realtime.enabled:
        logger.warning("Realtime RSS polling disabled (REALTIME_ENABLED=false)")
    
    if settings.event_driven.enabled:
        logger.info("Event-driven adapters enabled (EVENT_DRIVEN_ENABLED=true)")
    else:
        logger.warning("Event-driven adapters disabled (EVENT_DRIVEN_ENABLED=false)")
    
    logger.info("MI-3 Real-time API Hub started")

# Mount static files for examples
try:
    app.mount("/examples", StaticFiles(directory="examples"), name="examples")
except Exception:
    logger.warning("Could not mount /examples directory - test pages not available")

@app.get("/")
async def root():
    """API root endpoint"""
    from config.settings import settings
    
    endpoints = ["/latest", "/stream", "/stats", "/test-page"]
    
    # Add event-driven endpoints if enabled
    if settings.event_driven.enabled:
        endpoints.extend([
            settings.event_driven.webhook_path,
            "/push/health",
            "/push/stats"
        ])
    
    return {
        "name": "MI-3 News Scraper API", 
        "version": "1.0.0",
        "status": "active",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "endpoints": endpoints,
        "features": {
            "rss_polling": settings.realtime.enabled,
            "event_driven": settings.event_driven.enabled,
            "websockets": settings.event_driven.enabled and bool(settings.event_driven.ws_sources),
            "webhooks": settings.event_driven.enabled and bool(settings.event_driven.webhook_secret),
            "newswire": settings.event_driven.enabled and bool(settings.event_driven.newswire_sources)
        },
        "sse_info": {
            "stream_endpoint": "/stream",
            "heartbeat_interval": "15 seconds",
            "supports_source_filter": True,
            "test_page": "/test-page",
            "unified_sources": "RSS + WebSocket + webhook + newswire"
        }
    }

@app.get("/test-page")
async def serve_test_page():
    """Serve the SSE test page"""
    try:
        return FileResponse("examples/sse_test_page.html", media_type="text/html")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Test page not found")

@app.get("/latest")
async def get_latest_items(limit: int = 50, source: Optional[str] = None):
    """
    Get latest news items from feeds
    
    Args:
        limit: Maximum number of items to return (default: 50, max: 500)
        source: Filter by source name (optional)
    """
    # Validate limit
    limit = max(1, min(limit, 500))
    
    try:
        dm = get_data_manager()
        items = dm.get_latest_items(limit * 2)  # Get extra for filtering
        
        # Filter by source if specified
        if source:
            items = [item for item in items if item.get("source", "").lower() == source.lower()]
        
        # Apply final limit
        items = items[:limit]
        
        return {
            "items": items,
            "count": len(items),
            "limit": limit,
            "source_filter": source,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in /latest endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/stats")
async def get_stats():
    """Get statistics about the feed data"""
    try:
        dm = get_data_manager()
        stats = dm.get_stats()
        
        return {
            "stats": stats,
            "api_status": "active",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in /stats endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/stream")
async def stream_items(request: Request, source: Optional[str] = None):
    """
    Server-Sent Events stream of new items
    
    Streams new items as they become available in real-time.
    Supports multiple concurrent clients with heartbeat to prevent timeouts.
    
    Args:
        source: Optional filter by source name
    """
    
    async def event_stream():
        """Async generator for SSE stream"""
        client_id = id(request)  # Unique client identifier
        logger.info(f"New SSE client connected: {client_id}")
        
        dm = get_data_manager()
        last_seen_items = set()
        heartbeat_counter = 0
        
        try:
            # Send connection established event
            connection_data = {
                "type": "connected",
                "client_id": str(client_id),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message": "SSE stream established"
            }
            yield f"data: {json.dumps(connection_data)}\n\n"
            
            # Initialize with current items
            current_items = dm.get_latest_items(200)
            if source:
                current_items = [item for item in current_items 
                               if item.get("source", "").lower() == source.lower()]
            
            last_seen_items = {item.get("id") for item in current_items if item.get("id")}
            
            # Send initial batch of recent items
            init_data = {
                "type": "initial_data",
                "count": len(current_items),
                "items": current_items[:10],  # Send first 10 items
                "total_available": len(current_items),
                "source_filter": source,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            yield f"data: {json.dumps(init_data, ensure_ascii=False)}\n\n"
            
            logger.info(f"Client {client_id}: Initialized with {len(current_items)} items")
            
        except Exception as e:
            logger.error(f"Client {client_id}: Initialization error: {e}")
            error_data = {
                "type": "error",
                "message": "Stream initialization failed",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            yield f"data: {json.dumps(error_data)}\n\n"
            return
        
        # Main streaming loop
        while True:
            try:
                # Check if client disconnected
                if await request.is_disconnected():
                    logger.info(f"Client {client_id}: Disconnected")
                    break
                
                # Get current items and check for new ones
                current_items = dm.get_latest_items(200)
                if source:
                    current_items = [item for item in current_items 
                                   if item.get("source", "").lower() == source.lower()]
                
                current_ids = {item.get("id") for item in current_items if item.get("id")}
                
                # Find new items since last check
                new_ids = current_ids - last_seen_items
                if new_ids:
                    new_items = [item for item in current_items if item.get("id") in new_ids]
                    
                    # Sort new items by published date (most recent first)
                    new_items.sort(key=lambda x: x.get("published", ""), reverse=True)
                    
                    # Send each new item as separate event
                    for item in new_items:
                        event_data = {
                            "type": "new_item",
                            "item": item,
                            "client_id": str(client_id),
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }
                        yield f"data: {json.dumps(event_data, ensure_ascii=False)}\n\n"
                    
                    # Send summary event for batch
                    batch_data = {
                        "type": "batch_complete",
                        "new_items_count": len(new_items),
                        "total_items_tracking": len(current_ids),
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                    yield f"data: {json.dumps(batch_data)}\n\n"
                    
                    # Update tracking
                    last_seen_items = current_ids
                    logger.info(f"Client {client_id}: Streamed {len(new_items)} new items")
                
                # Heartbeat every 15 seconds to prevent connection timeout
                heartbeat_counter += 1
                if heartbeat_counter % 3 == 0:  # Every 15s (3 * 5s)
                    stats = dm.get_stats()
                    heartbeat_data = {
                        "type": "heartbeat",
                        "client_id": str(client_id),
                        "items_tracking": len(last_seen_items),
                        "total_items_available": stats.get("total_items", 0),
                        "last_data_update": stats.get("last_updated"),
                        "uptime_checks": heartbeat_counter,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                    yield f"data: {json.dumps(heartbeat_data)}\n\n"
                
                # Wait 5 seconds before next check (faster polling for real-time feel)
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Client {client_id}: Stream error: {e}")
                error_data = {
                    "type": "error",
                    "client_id": str(client_id),
                    "message": str(e),
                    "recoverable": True,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                yield f"data: {json.dumps(error_data)}\n\n"
                
                # Wait before retrying
                await asyncio.sleep(10)
        
        # Send disconnection event (if we reach here cleanly)
        try:
            disconnect_data = {
                "type": "disconnected",
                "client_id": str(client_id),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message": "Stream ended"
            }
            yield f"data: {json.dumps(disconnect_data)}\n\n"
        except:
            pass  # Client may already be disconnected
    
    # Return StreamingResponse with proper SSE headers
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
            "Content-Type": "text/event-stream; charset=utf-8"
        }
    )

@app.get("/sources")
async def get_sources():
    """Get list of configured feed sources"""
    try:
        from config.settings import load_feed_sources, settings
        sources = load_feed_sources(settings.realtime.sources_file)
        
        return {
            "sources": sources,
            "count": len(sources),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in /sources endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        dm = get_data_manager()
        stats = dm.get_stats()
        
        return {
            "status": "healthy",
            "api_version": "1.0.0",
            "data_available": stats.get("total_items", 0) > 0,
            "last_data_update": stats.get("last_updated"),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )

def create_app() -> FastAPI:
    """Factory function to create FastAPI app"""
    return app

if __name__ == "__main__":
    from config.settings import settings
    
    # Check if realtime is enabled
    if not settings.realtime.enabled:
        logger.warning("REALTIME_ENABLED=false, but starting API server anyway")
        logger.warning("Set REALTIME_ENABLED=true to enable full functionality")
    
    # Start the server
    uvicorn.run(
        "realtime.hub:app",
        host=settings.realtime.api_host,
        port=settings.realtime.api_port,
        reload=False,
        log_level="info"
    )