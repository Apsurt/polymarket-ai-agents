#!/bin/bash

echo "Stopping existing Docker containers (if any)..."
docker compose down

echo "Building Docker images..."
docker compose build

echo "Starting Redis, PostgreSQL, FastAPI backend, and RQ workers..."
echo "Note: Collectors will run once on startup of their containers for demonstration."
echo "For continuous collection, implement a dedicated scheduler service or cron jobs."

# Start services in detached mode
docker compose up -d

echo ""
echo "----------------------------------------------------"
echo "Services are starting up. Please allow some time."
echo "You can check their status with: docker compose ps"
echo "Logs can be viewed with: docker compose logs -f"
echo ""
echo "FastAPI backend should be available at: http://localhost:8000"
echo "Redis is on port 6379"
echo "PostgreSQL is on port 5432"
echo "----------------------------------------------------"
