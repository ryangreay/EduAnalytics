# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application
COPY . .

# Set environment to production by default
ENV ENV=production

# Expose port 8080 (fly.io default)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/_stcore/health || exit 1

# Run Streamlit on port 8080
CMD ["streamlit", "run", "ui/streamlit_app.py", "--server.port=8080", "--server.address=0.0.0.0", "--server.enableCORS=false", "--server.enableXsrfProtection=false"]

