FROM apache/spark:4.0.0-python3

USER root

RUN pybin="$(command -v python3)" && \
    ln -sf "$pybin" /usr/bin/python && \
    ln -sf "$pybin" /usr/local/bin/python && \
    pip3 install --no-cache-dir pymongo minio elasticsearch

RUN python3 - <<'PY'
import os
import urllib.request

jars = {
    "hadoop-aws-3.4.1.jar": "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar",
    "bundle-2.24.6.jar": "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar",
    "wildfly-openssl-1.1.3.Final.jar": "https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/wildfly-openssl-1.1.3.Final.jar",
}

target = "/opt/spark/jars"
os.makedirs(target, exist_ok=True)

for name, url in jars.items():
    out = os.path.join(target, name)
    print(f"Downloading {name} ...")
    urllib.request.urlretrieve(url, out)
    print(f"Saved to {out}")
PY

ENV HOME=/tmp
ENV PATH="/usr/local/bin:/usr/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

COPY apps/spark/ /opt/spark/work-dir/

USER spark