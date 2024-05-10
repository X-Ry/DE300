#!/bin/bash
echo "Starting run.sh"

# Exit immediately if a command exits with a non-zero status
set -e

# Give docker permission
sudo chmod 666 /var/run/docker.sock

# Build Jupyter Docker image
echo "Building Jupyter Docker image from dockerfile-jupyter"
docker build -f dockerfiles/dockerfile-jupyter -t jupyter-image .

# Run Jupyter container with volume and network setup
echo "Starting Jupyter container"
docker run -it --name etl-container \
	   -v ./src:/app/src \
	   -v ./staging_data:/app/staging_data \
	   -p 8888:8888 \
	   jupyter-image