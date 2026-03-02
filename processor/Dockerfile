FROM python:3.11-slim

# Java is required for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Detect JAVA_HOME dynamically so this works on amd64 and arm64
RUN echo "JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))" >> /etc/environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Tell PySpark which Python interpreter to use
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Avoid hostname-resolution failures in Docker/Windows
ENV SPARK_LOCAL_IP=0.0.0.0

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "processor.py"]
