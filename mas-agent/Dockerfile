# Dockerfile — Monitor and Resource Agent (MRA)
#
# Multi-stage build:
#   Stage 1 (builder): install Python dependencies into /install
#   Stage 2 (runtime): copy only the installed packages + source code
#
# The runtime image has no build tools (gcc, pip, etc.) which reduces
# attack surface and image size.
#
# Build command (run from mas-agent/ directory):
#   docker build -t mas-mra:latest .
#
# Load into K3D (no registry needed):
#   k3d image import mas-mra:latest --cluster datacenter-lab
#
 
FROM python:3.11-slim AS builder
 
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt
 
# ---- Runtime image ----
FROM python:3.11-slim
 
WORKDIR /app
 
# Copy installed packages from the builder stage
COPY --from=builder /install /usr/local
 
# Copy MRA Python source package
COPY mra/ ./mra/
 
# Create a non-root user to run the agent
# uid 1001 avoids conflict with the root user (0) and common system users
RUN useradd -r -u 1001 -g root mra-agent
USER mra-agent
 
# Port 8000: Prometheus metrics endpoint
# This is the Domain 3 write path — Prometheus scrapes this to record metric history
EXPOSE 8000
 
# Health check used by Docker (Kubernetes uses the liveness/readiness probes in the manifest)
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/metrics')" || exit 1
 
CMD ["python3", "-m", "mra.main"]
 
