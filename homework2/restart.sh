#!/bin/bash
echo "Starting restart.sh"

# Stop and remove existing container
echo "Stopping and removing existing container..."
docker stop etl-container || true
docker rm etl-container || true

# Run the original script
echo "Running the original script..."
./run.sh