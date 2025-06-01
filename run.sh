#!/bin/bash

echo "Polymarket AI Trader - Docker Setup"
echo "=================================="

# Function to check Docker availability
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker is not installed or not in PATH"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        echo "❌ Docker daemon is not running"
        exit 1
    fi

    echo "✅ Docker is available"
}

# Function to clean up existing containers
cleanup() {
    echo "🧹 Cleaning up existing containers..."
    docker compose down --volumes --remove-orphans
    docker system prune -f
}

# Function to build with error handling
build_images() {
    echo "🔨 Building Docker images..."
    if ! docker compose build; then
        echo "❌ Failed to build Docker images"
        echo "💡 Try running: docker system prune -a"
        exit 1
    fi
    echo "✅ Docker images built successfully"
}

# Function to start services
start_services() {
    echo "🚀 Starting services..."
    if ! docker compose up -d; then
        echo "❌ Failed to start services"
        echo "💡 Checking for alternative Redis configuration..."

        # Try with alternative configuration
        if [ -f "docker-compose.alternative.yml" ]; then
            echo "🔄 Trying alternative configuration..."
            docker compose -f docker-compose.alternative.yml up -d
        else
            echo "💡 Try using: docker compose up -d --compatibility"
            echo "💡 Or run with: docker compose up (without -d) to see live logs"
        fi
        exit 1
    fi
    echo "✅ Services started successfully"
}

# Function to show status
show_status() {
    echo ""
    echo "📊 Service Status:"
    echo "=================="
    docker compose ps

    echo ""
    echo "🔍 Quick Health Check:"
    echo "======================"

    # Check Redis
    if docker compose exec redis redis-cli ping &> /dev/null; then
        echo "✅ Redis is responding"
    else
        echo "❌ Redis is not responding"
    fi

    # Check if backend is accessible (with timeout)
    sleep 5
    if curl -s --max-time 5 http://localhost:8000 &> /dev/null; then
        echo "✅ Backend is accessible"
    else
        echo "⚠️  Backend might still be starting up"
    fi
}

# Main execution
echo "Starting setup process..."

check_docker
cleanup
build_images
start_services
show_status

echo ""
echo "🎉 Setup Complete!"
echo "=================="
echo "📍 FastAPI backend: http://localhost:8000"
echo "📍 Redis: localhost:6379"
echo "📍 RedisInsight (if using redis-stack): http://localhost:8001"
echo ""
echo "🛠️  Useful commands:"
echo "   📜 View logs: docker compose logs -f"
echo "   📊 Check status: docker compose ps"
echo "   🔄 Restart: docker compose restart"
echo "   🛑 Stop: docker compose down"
echo ""
echo "🚨 If you encounter issues:"
echo "   1. Check logs with: docker compose logs"
echo "   2. Try: docker system prune -a"
echo "   3. Restart Docker daemon"
echo "   4. Use alternative config if provided"
