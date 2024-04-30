# Use a Python base image
FROM python:3.11-bullseye

# Set the working directory to /app
WORKDIR /app

# Environment variable to avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Install MariaDB client (compatible with MySQL) instead of MySQL client
RUN apt-get update && apt-get install -y mariadb-client

# Copy files into the WORKDIR
COPY . .

# Install necessary packages and AWS CLI version 2
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    git \
    && rm -rf /var/lib/apt/lists/* \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip ./aws \
    && pip install pandas mysql-connector-python jupyter sqlalchemy matplotlib scipy numpy

# Expose port 8888
EXPOSE 8888

# Start Jupyter Notebook when the container starts
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]