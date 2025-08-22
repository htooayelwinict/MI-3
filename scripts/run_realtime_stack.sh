#!/bin/bash

# MI-3 News Scraper - Real-time Stack Runner
# Starts RSS polling, event-driven adapters, and FastAPI hub

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"

# Process tracking
declare -a PIDS
MAIN_PID=$$

# Create logs directory
mkdir -p "$LOG_DIR"

# Function to print colored output
print_status() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

print_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

print_info() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] INFO: $1${NC}"
}

# Cleanup function
cleanup() {
    print_warning "Shutting down all services..."
    
    # Kill all child processes
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            print_info "Stopping process $pid"
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    
    # Wait a moment for graceful shutdown
    sleep 2
    
    # Force kill if still running
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            print_warning "Force killing process $pid"
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done
    
    print_status "All services stopped"
    exit 0
}

# Set trap for cleanup
trap cleanup SIGINT SIGTERM EXIT

# Check Python environment
check_python() {
    print_info "Checking Python environment..."
    
    # Activate virtual environment if it exists
    if [ -f "$PROJECT_DIR/.venv/bin/activate" ]; then
        source "$PROJECT_DIR/.venv/bin/activate"
        print_info "Activated virtual environment"
    fi
    
    if ! command -v python &> /dev/null; then
        print_error "Python not found. Please install Python 3.8+"
        exit 1
    fi
    
    # Check if we're in the right directory
    if [ ! -f "$PROJECT_DIR/main.py" ]; then
        print_error "main.py not found. Please run from MI-3 project root"
        exit 1
    fi
    
    print_status "Python environment OK"
}

# Start RSS feed worker
start_rss_worker() {
    if [ "${REALTIME_ENABLED:-false}" == "true" ]; then
        print_status "Starting RSS feed worker..."
        
        cd "$PROJECT_DIR"
        python -m ingest.feeds_worker > "$LOG_DIR/feeds_worker.log" 2>&1 &
        local pid=$!
        PIDS+=($pid)
        
        print_status "RSS feed worker started (PID: $pid)"
        sleep 1
        
        # Check if it's still running
        if ! kill -0 "$pid" 2>/dev/null; then
            print_error "RSS feed worker failed to start. Check $LOG_DIR/feeds_worker.log"
            return 1
        fi
    else
        print_info "RSS polling disabled (REALTIME_ENABLED=false)"
    fi
}

# Start WebSocket adapters
start_websocket_adapters() {
    if [ "${EVENT_DRIVEN_ENABLED:-false}" == "true" ]; then
        # Check if WebSocket sources are configured
        local ws_sources="${WS_SOURCES:-[]}"
        if [ "$ws_sources" != "[]" ] && [ "$ws_sources" != "" ]; then
            print_status "Starting WebSocket adapters..."
            
            cd "$PROJECT_DIR"
            python -m adapters.websocket_adapter > "$LOG_DIR/websocket_adapter.log" 2>&1 &
            local pid=$!
            PIDS+=($pid)
            
            print_status "WebSocket adapters started (PID: $pid)"
            sleep 1
            
            if ! kill -0 "$pid" 2>/dev/null; then
                print_error "WebSocket adapters failed to start. Check $LOG_DIR/websocket_adapter.log"
                return 1
            fi
        else
            print_info "No WebSocket sources configured"
        fi
    else
        print_info "Event-driven adapters disabled (EVENT_DRIVEN_ENABLED=false)"
    fi
}

# Start newswire adapters
start_newswire_adapters() {
    if [ "${EVENT_DRIVEN_ENABLED:-false}" == "true" ]; then
        # Check if newswire sources are configured
        local newswire_sources="${NEWSWIRE_SOURCES:-[]}"
        if [ "$newswire_sources" != "[]" ] && [ "$newswire_sources" != "" ]; then
            print_status "Starting newswire adapters..."
            
            cd "$PROJECT_DIR"
            python -m adapters.newswire_adapter > "$LOG_DIR/newswire_adapter.log" 2>&1 &
            local pid=$!
            PIDS+=($pid)
            
            print_status "Newswire adapters started (PID: $pid)"
            sleep 1
            
            if ! kill -0 "$pid" 2>/dev/null; then
                print_warning "Newswire adapters failed to start. Check $LOG_DIR/newswire_adapter.log"
                print_info "Continuing without newswire adapters..."
                # Remove failed PID from tracking
                PIDS=("${PIDS[@]/$pid}")
            fi
        else
            print_info "No newswire sources configured"
        fi
    fi
}

# Start FastAPI hub
start_api_hub() {
    print_status "Starting FastAPI hub..."
    
    local host="${REALTIME_API_HOST:-127.0.0.1}"
    local port="${REALTIME_API_PORT:-8000}"
    
    cd "$PROJECT_DIR"
    python -m uvicorn realtime.hub:app --host "$host" --port "$port" --log-level info > "$LOG_DIR/api_hub.log" 2>&1 &
    local pid=$!
    PIDS+=($pid)
    
    print_status "FastAPI hub started (PID: $pid) at http://$host:$port"
    sleep 2
    
    # Check if it's still running
    if ! kill -0 "$pid" 2>/dev/null; then
        print_error "FastAPI hub failed to start. Check $LOG_DIR/api_hub.log"
        return 1
    fi
    
    # Test API endpoint
    if command -v curl &> /dev/null; then
        if curl -s "http://$host:$port/" > /dev/null 2>&1; then
            print_status "API hub health check passed"
        else
            print_warning "API hub health check failed (may still be starting)"
        fi
    fi
}

# Print service status
print_service_status() {
    print_info "Service Status:"
    echo "=================="
    
    for i in "${!PIDS[@]}"; do
        local pid="${PIDS[$i]}"
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "  ${GREEN}✓${NC} Process $pid (running)"
        else
            echo -e "  ${RED}✗${NC} Process $pid (stopped)"
        fi
    done
    
    echo ""
    echo "Log files:"
    echo "  RSS Worker:      $LOG_DIR/feeds_worker.log"
    echo "  WebSocket:       $LOG_DIR/websocket_adapter.log"
    echo "  Newswire:        $LOG_DIR/newswire_adapter.log"
    echo "  API Hub:         $LOG_DIR/api_hub.log"
    echo ""
    
    local host="${REALTIME_API_HOST:-127.0.0.1}"
    local port="${REALTIME_API_PORT:-8000}"
    echo "API Endpoints:"
    echo "  Root:            http://$host:$port/"
    echo "  Latest Items:    http://$host:$port/latest"
    echo "  SSE Stream:      http://$host:$port/stream"
    echo "  Stats:           http://$host:$port/stats"
    
    if [ "${EVENT_DRIVEN_ENABLED:-false}" == "true" ]; then
        local webhook_path="${WEBHOOK_PATH:-/push/inbound}"
        echo "  Webhook:         http://$host:$port$webhook_path"
        echo "  Webhook Health:  http://$host:$port/push/health"
    fi
}

# Monitor services
monitor_services() {
    print_status "Monitoring services... (Press Ctrl+C to stop)"
    
    while true; do
        sleep 10
        
        # Check if any service has died
        local any_died=false
        for pid in "${PIDS[@]}"; do
            if ! kill -0 "$pid" 2>/dev/null; then
                print_error "Process $pid has died!"
                any_died=true
            fi
        done
        
        if $any_died; then
            print_error "Some services have failed. Check log files."
            break
        fi
    done
}

# Main execution
main() {
    print_status "Starting MI-3 Real-time News Stack"
    print_info "Project directory: $PROJECT_DIR"
    print_info "Log directory: $LOG_DIR"
    
    # Load environment variables if .env exists
    if [ -f "$PROJECT_DIR/.env" ]; then
        print_info "Loading environment from .env file"
        # Load .env file line by line, properly handling JSON values
        while IFS='=' read -r key value; do
            # Skip comments and empty lines
            [[ "$key" =~ ^#.*$ ]] && continue
            [[ -z "$key" ]] && continue
            
            # Remove any trailing comments
            value=$(echo "$value" | sed 's/#.*$//')
            
            # Export the variable
            export "$key"="$value"
        done < "$PROJECT_DIR/.env"
    fi
    
    # Print configuration
    echo ""
    print_info "Configuration:"
    echo "  REALTIME_ENABLED:     ${REALTIME_ENABLED:-false}"
    echo "  EVENT_DRIVEN_ENABLED: ${EVENT_DRIVEN_ENABLED:-false}"
    echo "  API Host:Port:        ${REALTIME_API_HOST:-127.0.0.1}:${REALTIME_API_PORT:-8000}"
    echo "  WebSocket Sources:    $(echo "${WS_SOURCES:-[]}" | jq -r 'length' 2>/dev/null || echo "0")"
    echo "  Newswire Sources:     $(echo "${NEWSWIRE_SOURCES:-[]}" | jq -r 'length' 2>/dev/null || echo "0")"
    echo ""
    
    # Pre-flight checks
    check_python
    
    # Start services
    start_rss_worker
    start_websocket_adapters  
    start_newswire_adapters
    start_api_hub
    
    echo ""
    print_service_status
    echo ""
    
    # Monitor services
    monitor_services
}

# Help function
show_help() {
    echo "MI-3 Real-time Stack Runner"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo "  --status       Show service status and exit"
    echo ""
    echo "Environment Variables:"
    echo "  REALTIME_ENABLED=true/false          Enable RSS polling"
    echo "  EVENT_DRIVEN_ENABLED=true/false      Enable event-driven adapters"
    echo "  REALTIME_API_HOST=127.0.0.1          API server host"
    echo "  REALTIME_API_PORT=8000                API server port"
    echo "  WS_SOURCES='[{...}]'                  WebSocket sources JSON"
    echo "  NEWSWIRE_SOURCES='[{...}]'            Newswire sources JSON"
    echo "  WEBHOOK_SECRET=secret                 Webhook validation secret"
    echo ""
    echo "Examples:"
    echo "  # Start with RSS polling only"
    echo "  REALTIME_ENABLED=true $0"
    echo ""
    echo "  # Start with event-driven only"
    echo "  EVENT_DRIVEN_ENABLED=true WEBHOOK_SECRET=mysecret $0"
    echo ""
    echo "  # Start all services"
    echo "  REALTIME_ENABLED=true EVENT_DRIVEN_ENABLED=true $0"
}

# Parse command line arguments
case "${1:-}" in
    -h|--help)
        show_help
        exit 0
        ;;
    --status)
        print_service_status
        exit 0
        ;;
    "")
        main
        ;;
    *)
        print_error "Unknown option: $1"
        show_help
        exit 1
        ;;
esac