FROM apache/airflow:2.7.0
USER root
RUN apt-get update \
  && apt-get install -y zip  
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

    