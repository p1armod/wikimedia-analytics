#!/bin/bash
# ============================================================
# Wikimedia Analytics — Service Management Script
# Place this in: ~/wikimedia-analytics/services.sh
# Usage: ./services.sh start | stop | restart | status | logs
# ============================================================

set -e

# ---- Configuration ----
# Auto-detect project directory (wherever this script lives)
APP_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$APP_DIR/logs"
PID_DIR="$APP_DIR/pids"
JAVA_OPTS="-Xmx384m -Xms256m"

# CORS: your Vercel URL goes here
export CORS_ALLOWED_ORIGINS="http://localhost:5173,https://wikimedia-dashboard.vercel.app,https://wikimedia-analytics.duckdns.org"

# Infrastructure (defaults match docker-compose on same server)
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export POSTGRES_HOST="localhost"
export POSTGRES_PORT="5433"
export POSTGRES_DB="wikimedia_analytics"
export POSTGRES_USER="wikimedia"
export POSTGRES_PASSWORD="wikimedia"
export REDIS_HOST="localhost"
export REDIS_PORT="6379"

# Service JAR directories
PRODUCER_DIR="$APP_DIR/wikimedia-producer/target"
STREAMS_DIR="$APP_DIR/wikimedia-streams-processor/target"
GATEWAY_DIR="$APP_DIR/wikimedia-websocket-gateway/target"

# Auto-detect JAR files (finds the main JAR, ignoring -sources, -javadoc, etc.)
find_jar() {
    local dir=$1
    local name=$2
    # Find the JAR matching the artifact name, exclude auxiliary JARs
    local jar=$(ls "$dir"/${name}-*.jar 2>/dev/null | grep -v sources | grep -v javadoc | grep -v original | head -1)
    echo "$jar"
}

PRODUCER_JAR=$(find_jar "$PRODUCER_DIR" "wikimedia-producer")
STREAMS_JAR=$(find_jar "$STREAMS_DIR" "wikimedia-streams-processor")
GATEWAY_JAR=$(find_jar "$GATEWAY_DIR" "wikimedia-websocket-gateway")

# Service names (for display and PID files)
SERVICES=("producer" "streams" "gateway")
JARS=("$PRODUCER_JAR" "$STREAMS_JAR" "$GATEWAY_JAR")

# ---- Helper Functions ----

mkdir -p "$LOG_DIR" "$PID_DIR"

get_pid() {
    local service=$1
    local pid_file="$PID_DIR/$service.pid"
    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        # Check if process is actually running
        if kill -0 "$pid" 2>/dev/null; then
            echo "$pid"
            return 0
        else
            rm -f "$pid_file"
        fi
    fi
    echo ""
    return 1
}

start_service() {
    local service=$1
    local jar=$2
    local delay=$3

    local existing_pid=$(get_pid "$service" 2>/dev/null || true)
    if [ -n "$existing_pid" ]; then
        echo "  ⚡ $service is already running (PID: $existing_pid)"
        return 0
    fi

    if [ ! -f "$jar" ]; then
        echo "  ❌ JAR not found: $jar"
        echo "     Run: cd $(dirname $jar) && mvn clean package -DskipTests"
        return 1
    fi

    echo "  🚀 Starting $service..."
    nohup java $JAVA_OPTS -jar "$jar" > "$LOG_DIR/$service.log" 2>&1 &
    local pid=$!
    echo "$pid" > "$PID_DIR/$service.pid"
    echo "  ✅ $service started (PID: $pid, log: $LOG_DIR/$service.log)"

    if [ -n "$delay" ] && [ "$delay" -gt 0 ]; then
        echo "  ⏳ Waiting ${delay}s for $service to initialize..."
        sleep "$delay"
    fi
}

stop_service() {
    local service=$1
    local pid=$(get_pid "$service" 2>/dev/null || true)

    if [ -z "$pid" ]; then
        echo "  ⚫ $service is not running"
        return 0
    fi

    echo "  🛑 Stopping $service (PID: $pid)..."
    kill "$pid" 2>/dev/null || true

    # Wait up to 10 seconds for graceful shutdown
    for i in $(seq 1 10); do
        if ! kill -0 "$pid" 2>/dev/null; then
            break
        fi
        sleep 1
    done

    # Force kill if still running
    if kill -0 "$pid" 2>/dev/null; then
        echo "  ⚠️  Force killing $service..."
        kill -9 "$pid" 2>/dev/null || true
    fi

    rm -f "$PID_DIR/$service.pid"
    echo "  ✅ $service stopped"
}

# ---- Commands ----

do_start() {
    echo ""
    echo "========================================="
    echo "  Starting Wikimedia Analytics Services"
    echo "========================================="
    echo ""

    # Check Docker infrastructure first
    if ! docker compose -f "$APP_DIR/docker-compose.yml" ps --status running 2>/dev/null | grep -q "wikimedia-kafka"; then
        echo "  ⚠️  Docker infrastructure may not be running."
        echo "     Run: cd $APP_DIR && docker compose up -d"
        echo ""
    fi

    start_service "producer" "$PRODUCER_JAR" 10
    start_service "streams"  "$STREAMS_JAR"  10
    start_service "gateway"  "$GATEWAY_JAR"  5

    echo ""
    echo "  All services started! Check status with: ./services.sh status"
    echo ""
}

do_stop() {
    echo ""
    echo "========================================="
    echo "  Stopping Wikimedia Analytics Services"
    echo "========================================="
    echo ""

    # Stop in reverse order
    stop_service "gateway"
    stop_service "streams"
    stop_service "producer"

    echo ""
    echo "  All services stopped."
    echo "  Note: Docker infrastructure (Kafka, Redis, PostgreSQL) is still running."
    echo "  To stop everything: docker compose -f $APP_DIR/docker-compose.yml down"
    echo ""
}

do_restart() {
    echo ""
    echo "========================================="
    echo "  Restarting Wikimedia Analytics Services"
    echo "========================================="
    do_stop
    sleep 2
    do_start
}

do_status() {
    echo ""
    echo "========================================="
    echo "  Wikimedia Analytics — Service Status"
    echo "========================================="
    echo ""

    for i in "${!SERVICES[@]}"; do
        local service="${SERVICES[$i]}"
        local pid=$(get_pid "$service" 2>/dev/null || true)
        if [ -n "$pid" ]; then
            local mem=$(ps -p "$pid" -o rss= 2>/dev/null | awk '{printf "%.0f", $1/1024}')
            echo "  ✅ $service — Running (PID: $pid, RAM: ${mem}MB)"
        else
            echo "  ❌ $service — Stopped"
        fi
    done

    echo ""

    # Docker status
    echo "  --- Docker Infrastructure ---"
    if command -v docker &> /dev/null; then
        docker compose -f "$APP_DIR/docker-compose.yml" ps --format "table {{.Name}}\t{{.Status}}" 2>/dev/null || echo "  Could not check Docker status"
    fi

    echo ""

    # Quick health check
    echo "  --- Health Check ---"
    local health=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null || echo "000")
    if [ "$health" = "200" ]; then
        echo "  ✅ Gateway API: http://localhost:8080/actuator/health → 200 OK"
    else
        echo "  ❌ Gateway API: http://localhost:8080/actuator/health → $health"
    fi
    echo ""
}

do_logs() {
    local service=${1:-"gateway"}
    local lines=${2:-50}
    echo ""
    echo "  Showing last $lines lines of $service log:"
    echo "  ================================================"
    tail -n "$lines" "$LOG_DIR/$service.log" 2>/dev/null || echo "  No log file found for $service"
    echo ""
    echo "  To follow live: tail -f $LOG_DIR/$service.log"
    echo ""
}

# ---- Main ----

case "${1:-}" in
    start)
        do_start
        ;;
    stop)
        do_stop
        ;;
    restart)
        do_restart
        ;;
    status)
        do_status
        ;;
    logs)
        do_logs "${2:-gateway}" "${3:-50}"
        ;;
    *)
        echo ""
        echo "  Wikimedia Analytics — Service Manager"
        echo ""
        echo "  Usage: $0 {start|stop|restart|status|logs}"
        echo ""
        echo "  Commands:"
        echo "    start     Start all services (Producer → Streams → Gateway)"
        echo "    stop      Stop all services gracefully"
        echo "    restart   Stop then start all services"
        echo "    status    Show running status and health check"
        echo "    logs      Show logs (usage: logs [service] [lines])"
        echo "              e.g., $0 logs gateway 100"
        echo "              services: producer, streams, gateway"
        echo ""
        ;;
esac
