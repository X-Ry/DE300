#!/bin/bash
echo "Starting restart.sh"

# Stop and remove existing containers
echo "Stopping and removing existing containers..."
docker stop postgres-container etl-container || true
docker rm postgres-container etl-container || true

# Remove existing volume
echo "Removing existing volume..."
docker volume rm car-database || true

# Remove existing network
echo "Removing existing network..."
docker network rm etl-database || true

# Run the original script
echo "Running the original script..."
./run.sh