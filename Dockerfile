# Usa a imagem base do Airflow
FROM apache/airflow:2.7.2

USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless procps && \
    apt-get install -y python3-pip && \
    apt-get clean


# Define JAVA_HOME para PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin

# Retorna ao usuário airflow
USER airflow

COPY requirements.txt .
# Instala pacotes Python necessários
RUN pip install --no-cache-dir -r requirements.txt