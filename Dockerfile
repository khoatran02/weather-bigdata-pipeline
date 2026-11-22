# syntax=docker/dockerfile:1
FROM apache/spark:4.1.4-scala2.13-java21-python3-ubuntu AS base
USER root
ENV SPARK_VERSION=4.1.4 DELTA_VERSION=4.4.1 HADOOP_VERSION=3.4.2 TZ=UTC
COPY bin/fetch-jars /tmp/fetch-jars
RUN bash /tmp/fetch-jars
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

FROM base AS app
COPY conf/spark-defaults.conf conf/metrics.properties /opt/spark/conf/
COPY bin /opt/app/bin
COPY src /opt/app/src
ENV PYTHONPATH=/opt/app/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    PYSPARK_PYTHON=python3 PYTHONUNBUFFERED=1 PATH=/opt/app/bin:/opt/spark/bin:$PATH
WORKDIR /opt/app
USER spark

FROM app AS test
USER root
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt pytest==8.3.5 ruff==0.9.10
COPY pyproject.toml ./
COPY tests ./tests
COPY dags ./dags
COPY scripts ./scripts
COPY docker ./docker
CMD ["sh", "-c", "ruff check . && ruff format --check . && python3 -m pytest -q"]

# Airflow runs Spark drivers, so it needs the same Spark, Java, code and Python minor version.
FROM apache/airflow:3.3.2-python3.10 AS airflow
COPY --from=app /opt/spark /opt/spark
COPY --from=app /opt/java/openjdk /opt/java/openjdk
COPY --from=app /opt/app /opt/app
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
ENV JAVA_HOME=/opt/java/openjdk SPARK_HOME=/opt/spark TZ=UTC PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/app/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip
RUN mkdir -p /opt/airflow/state
COPY dags /opt/airflow/dags
