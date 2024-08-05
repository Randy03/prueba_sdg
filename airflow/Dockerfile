FROM apache/airflow:2.9.3
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim krb5-config krb5-user gcc heimdal-dev curl default-jdk ant procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER airflow
RUN pip install apache-airflow-providers-apache-hdfs apache-airflow-providers-apache-kafka apache-airflow-providers-apache-spark apache-airflow-providers-ssh

