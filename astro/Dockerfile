FROM eclipse-temurin:17-jre AS jre

FROM apache/airflow:3.4.0-python3.12

LABEL io.astronomer.docker.runtime.version="3.3-12"

ENV AIRFLOW_HOME=/usr/local/airflow \
    JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}"

USER root
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
RUN echo "astro:x:50000:0:astro:/home/airflow:/bin/bash" >> /etc/passwd \
 && mkdir -p /usr/local/airflow/logs /tmp/bench_out /tmp/bench_results \
 && chown -R airflow:root /usr/local/airflow /tmp/bench_out /tmp/bench_results
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /usr/local/airflow
COPY --chown=airflow:root . .
