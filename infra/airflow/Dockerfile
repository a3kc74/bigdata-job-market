FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSLo /usr/local/bin/kubectl \
      https://dl.k8s.io/release/v1.30.0/bin/linux/amd64/kubectl \
    && chmod +x /usr/local/bin/kubectl

USER airflow

COPY infra/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY infra/airflow/dags /opt/airflow/dags
