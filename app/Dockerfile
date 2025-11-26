FROM python:3.10-slim

WORKDIR /app

# Prevent Python from writing pyc files & buffer stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install OS dependencies (optional for OCR later)
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ .

# Start all services
CMD ["sh", "-c", "python3 watcher_producer.py & python3 ocr_processor_service.py & python3 sink_writer_consumer.py"]
