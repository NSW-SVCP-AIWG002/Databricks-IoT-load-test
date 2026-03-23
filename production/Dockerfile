# -------------------------------------------------------
# Locust load test image for Azure IoT / Event Hub
# Base: Python 3.11 slim (Debian Bookworm)
# -------------------------------------------------------
FROM python:3.11-slim-bookworm AS base

# Build-time metadata
LABEL maintainer="iotbricks-team"
LABEL description="Locust load testing image for Azure IoT Hub / Event Hub"

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libssl-dev \
        libffi-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /loadtest

# Install Python dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy load test scripts
COPY . .

# Create output directory for CSV statistics and logs
RUN mkdir -p /loadtest/output

# Expose Locust web UI port and worker communication port
EXPOSE 8089
EXPOSE 5557

# Default: start Locust master (override CMD for worker nodes)
ENTRYPOINT ["locust"]
CMD ["--host", "http://localhost", "--web-host", "0.0.0.0"]
