#!/bin/bash
set -e

echo "=================================================="
echo "  ClickHouse OpenTelemetry Tracing Demo Launcher"
echo "=================================================="
echo ""

# Check if docker is available
if ! command -v docker &> /dev/null; then
    echo "❌ Error: Docker is not installed or not in PATH"
    echo "   Please install Docker first: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "⚠️  Warning: docker-compose not found, trying 'docker compose' instead..."
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi

echo "✅ Docker is available"
echo ""

# Start services
echo "🚀 Starting ClickHouse and Jaeger..."
$DOCKER_COMPOSE -f docker-compose.otel-demo.yml up -d

echo ""
echo "⏳ Waiting for services to be ready (15 seconds)..."
sleep 5

# Check ClickHouse
echo -n "   Checking ClickHouse... "
for i in {1..10}; do
    if docker exec clickhouse-otel-demo clickhouse-client --query "SELECT 1" &>/dev/null; then
        echo "✅"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "❌ Failed to connect"
        exit 1
    fi
    sleep 1
done

# Check Jaeger
echo -n "   Checking Jaeger... "
for i in {1..10}; do
    if curl -s http://localhost:16686 > /dev/null; then
        echo "✅"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "❌ Failed to connect"
        exit 1
    fi
    sleep 1
done

echo ""
echo "✅ All services are ready!"
echo ""

# Get dependencies
echo "📦 Installing Go dependencies..."
cd examples/clickhouse_api
if ! go get go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc &>/dev/null; then
    echo "⚠️  Warning: Could not install dependencies, continuing anyway..."
fi
go mod tidy &>/dev/null || true

echo ""
echo "=================================================="
echo "  Running OpenTelemetry Tracing Example"
echo "=================================================="
echo ""

# Run the example
go run otel_tracing_jaeger.go

echo ""
echo "=================================================="
echo "  Demo Complete!"
echo "=================================================="
echo ""
echo "📊 View traces in Jaeger UI:"
echo "   🌐 http://localhost:16686"
echo ""
echo "Steps:"
echo "1. Open the URL above in your browser"
echo "2. Select service: 'clickhouse-go-demo'"
echo "3. Click 'Find Traces'"
echo "4. Click on any trace to see details"
echo ""
echo "What to look for:"
echo "• Operation names: clickhouse.query, clickhouse.exec, etc."
echo "• Span duration: Total client-side time"
echo "• Attributes: db.statement, db.operation, etc."
echo "• Server time: db.clickhouse.server.elapsed_ns attribute"
echo ""
echo "Press Enter to stop services and cleanup..."
read

# Cleanup
cd ../..
echo ""
echo "🧹 Stopping services..."
$DOCKER_COMPOSE -f docker-compose.otel-demo.yml down

echo ""
echo "✅ Demo cleanup complete!"
echo ""
echo "To run again, simply execute: ./run_otel_demo.sh"
echo ""
