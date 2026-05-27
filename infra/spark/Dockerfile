# Spark runtime image for batch ETL, speed-layer streaming, and crawler jobs.
#
# This image contains:
#   - project source code
#   - Spark/Python runtime
#   - crawler dependencies such as beautifulsoup4, curl-cffi, tenacity
#   - Elasticsearch/Kafka integration dependencies
#
# Used by:
#   - batch-etl-crawl-jobs
#   - batch-etl-raw-to-bronze
#   - batch-etl-bronze-to-silver
#   - batch-etl-train-salary-model
#   - batch-etl-silver-to-gold
#   - batch-etl-gold-to-elasticsearch
#   - speed-stream-es-submit
#
# Build:
#   minikube image build -f infra/spark/Dockerfile -t spark-job-market:latest .

FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    gnupg \
    xvfb \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    && wget -q -O /tmp/google-chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y /tmp/google-chrome.deb \
    && rm -f /tmp/google-chrome.deb \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    "beautifulsoup4>=4.12.0" \
    "cloudscraper>=1.2.71" \
    "curl-cffi>=0.7.0" \
    "nodriver>=0.34" \
    "python-dotenv==1.0.1" \
    "requests>=2.31.0" \
    "pydantic-settings>=2.6.1" \
    "numpy>=1.26.0,<3.0.0" \
    "elasticsearch>=8.13.2" \
    "confluent-kafka==2.4.0" \
    "tenacity>=8.2.0"

ENV HOME=/tmp
ENV PATH="/usr/local/bin:/usr/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3


COPY apps/common/              /opt/spark/work-dir/apps/common/
COPY apps/ml/                  /opt/spark/work-dir/apps/ml/
COPY apps/stream_etl/          /opt/spark/work-dir/apps/stream_etl/
COPY apps/ingestion/           /opt/spark/work-dir/apps/ingestion/
COPY apps/producer/            /opt/spark/work-dir/apps/producer/
COPY apps/batch/               /opt/spark/work-dir/apps/batch/
COPY apps/batch/jobs/          /opt/spark/work-dir/jobs/
COPY configs/                  /opt/spark/work-dir/configs/
COPY shared/                   /opt/spark/work-dir/shared/
COPY data/raw/                 /opt/spark/work-dir/data/raw/

ENV PYTHONPATH="/opt/spark/work-dir"

WORKDIR /opt/spark/work-dir

USER spark
