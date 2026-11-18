FROM eclipse-temurin:17-jre AS jre

FROM apache/airflow:3.4.0-python3.12

USER root
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}"
# Confirm the JRE is on PATH for the airflow user.
USER airflow
