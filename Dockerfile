# ==============================================================================
# MINNESOTA BUSINESS SCRAPER - DOCKERFILE
# ==============================================================================
#
# This Dockerfile creates a container image for running the MN Business Scraper.
#
# WHAT IS DOCKER?
# ---------------
# Docker packages an application with all its dependencies into a "container".
# This ensures the scraper runs the same way on any computer with Docker installed.
#
# BUILDING THE IMAGE:
# -------------------
#     docker build -t mn-scraper .
#
# RUNNING THE SCRAPER:
# --------------------
#     # Run the parallel name search scraper
#     docker run -v $(pwd)/data:/app/data mn-scraper python search_by_name_parallel.py
#
#     # Run with visible browser (for debugging) - requires X server
#     docker run -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix mn-scraper python search_by_name_parallel.py --visible
#
# RUNNING THE DASHBOARD:
# ----------------------
#     docker run -p 5000:5000 -v $(pwd)/data:/app/data mn-scraper python dashboard.py --host 0.0.0.0
#     # Then open http://localhost:5000 in your browser
#
# FOR JUNIOR DEVELOPERS:
# ----------------------
# A Dockerfile is like a recipe. Each line is an instruction:
# - FROM: Start with this base image (like a template)
# - RUN: Execute a command (like installing software)
# - COPY: Copy files from your computer into the image
# - WORKDIR: Change the working directory
# - CMD: The default command to run when the container starts
#
# ==============================================================================

# -----------------------------------------------------------------------------
# BASE IMAGE
# -----------------------------------------------------------------------------
# Start with Python 3.11 on a slim Debian-based image
# "slim" means a smaller image size (faster to download, less disk space)
FROM python:3.11-slim

# -----------------------------------------------------------------------------
# SET WORKING DIRECTORY
# -----------------------------------------------------------------------------
# All following commands will run from /app
WORKDIR /app

# -----------------------------------------------------------------------------
# INSTALL SYSTEM DEPENDENCIES
# -----------------------------------------------------------------------------
# These are needed for Playwright to run a browser
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Required for Playwright/Chromium
    wget \
    gnupg \
    ca-certificates \
    # Required for browser rendering
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    # Required for fonts
    fonts-liberation \
    # Git for pushing to GitHub
    git \
    # Clean up to reduce image size
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------------
# COPY REQUIREMENTS AND INSTALL PYTHON PACKAGES
# -----------------------------------------------------------------------------
# Copy requirements first (for better Docker caching)
# If requirements.txt hasn't changed, Docker will use cached layer
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright and download browser
# This downloads a Chromium browser that Playwright will use
RUN playwright install chromium
RUN playwright install-deps chromium

# -----------------------------------------------------------------------------
# COPY APPLICATION CODE
# -----------------------------------------------------------------------------
# Copy all Python files and directories
COPY *.py ./
COPY config.py ./

# Create directories for output and logs
RUN mkdir -p data output logs

# -----------------------------------------------------------------------------
# COPY TEST FILES (if they exist)
# -----------------------------------------------------------------------------
# These are optional - the container will still build without them
COPY tests/ tests/ 2>/dev/null || true

# -----------------------------------------------------------------------------
# ENVIRONMENT VARIABLES
# -----------------------------------------------------------------------------
# Set Python to not buffer output (so logs appear immediately)
ENV PYTHONUNBUFFERED=1

# Default to headless mode (no visible browser window)
ENV HEADLESS=true

# -----------------------------------------------------------------------------
# VOLUMES
# -----------------------------------------------------------------------------
# These directories can be mounted from the host to persist data
# Usage: docker run -v /path/on/host:/app/data ...
VOLUME ["/app/data", "/app/output", "/app/logs"]

# -----------------------------------------------------------------------------
# EXPOSE PORTS
# -----------------------------------------------------------------------------
# Dashboard runs on port 5000
EXPOSE 5000

# -----------------------------------------------------------------------------
# DEFAULT COMMAND
# -----------------------------------------------------------------------------
# By default, show help message
# Override with: docker run mn-scraper python search_by_name_parallel.py
CMD ["python", "-c", "print('MN Business Scraper Docker Container\\n\\nUsage:\\n  docker run mn-scraper python search_by_name_parallel.py\\n  docker run -p 5000:5000 mn-scraper python dashboard.py --host 0.0.0.0\\n\\nFor help with a specific script:\\n  docker run mn-scraper python search_by_name_parallel.py --help')"]
