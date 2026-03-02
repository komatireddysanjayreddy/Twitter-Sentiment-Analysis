FROM python:3.11-slim

WORKDIR /app

# System deps for confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default: mock producer. Override with:  docker-compose run producer python producer.py
CMD ["python", "mock_producer.py"]
