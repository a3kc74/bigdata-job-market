FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

RUN pip3 install --no-cache-dir \
    python-dotenv==1.0.1 \
    requests>=2.31.0 \
    pydantic-settings>=2.6.1 \
    elasticsearch>=8.13.2 \
    confluent-kafka==2.4.0

ENV HOME=/tmp
ENV PATH="/usr/local/bin:/usr/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

COPY apps/stream_etl/          /opt/spark/work-dir/apps/stream_etl/
COPY apps/ingestion/           /opt/spark/work-dir/apps/ingestion/
COPY apps/producer/            /opt/spark/work-dir/apps/producer/
COPY apps/batch/jobs/          /opt/spark/work-dir/jobs/
COPY configs/                  /opt/spark/work-dir/configs/
COPY shared/                   /opt/spark/work-dir/shared/
COPY data/raw/                 /opt/spark/work-dir/data/raw/

ENV PYTHONPATH="/opt/spark/work-dir"

WORKDIR /opt/spark/work-dir

USER spark