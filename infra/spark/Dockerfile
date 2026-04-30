FROM apache/spark:4.1.1-python3

USER root

# Cài thêm Python deps cho batch ETL
RUN pip3 install --no-cache-dir \
    cassandra-driver \
    requests

ENV HOME=/tmp
ENV PATH="/usr/local/bin:/usr/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Copy toàn bộ batch jobs và shared utils
COPY apps/batch/jobs/          /opt/spark/work-dir/jobs/
COPY apps/spark/               /opt/spark/work-dir/stream/
COPY shared/                   /opt/spark/work-dir/shared/

WORKDIR /opt/spark/work-dir

USER spark