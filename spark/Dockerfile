FROM apache/spark:3.5.5

USER root

RUN apt-get update -qq && apt-get install -y -qq curl && \
    curl -fsSL -o /opt/spark/jars/postgresql-42.7.3.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" && \
    mkdir -p /opt/extra-jars && \
    curl -fsSL -o /opt/extra-jars/clickhouse-jdbc-0.2.6.jar \
      "https://repo1.maven.org/maven2/ru/yandex/clickhouse/clickhouse-jdbc/0.2.6/clickhouse-jdbc-0.2.6.jar" && \
    curl -fsSL -o /opt/extra-jars/guava-31.1-jre.jar \
      "https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar" && \
    apt-get clean

USER spark
