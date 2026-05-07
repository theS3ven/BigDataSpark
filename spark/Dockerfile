FROM apache/spark:3.5.5

USER root

RUN apt-get update -qq && apt-get install -y -qq curl && \
    curl -fsSL -o /opt/spark/jars/postgresql-42.7.3.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" && \
    apt-get clean

USER spark
