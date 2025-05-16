FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    libjpeg-dev \
    zlib1g-dev \
    libpng-dev \
    gdal-bin \
    libgdal-dev \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL version
ENV GDAL_VERSION=3.4.1

# Install UV
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    ls -l /root/.local/bin/uv && \
    /root/.local/bin/uv --version

# Add uv to PATH
ENV PATH="/root/.local/bin:$PATH"

# Copy pyproject.toml and uv.lock first to leverage Docker cache
COPY pyproject.toml uv.lock ./

# Copy the rest of the application
COPY . .

# Install Python dependencies
RUN uv pip install -e . --system

# Create logs directory
RUN mkdir -p logs

# Expose port
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"] 