FROM maven:3.9-eclipse-temurin-11 AS java
WORKDIR /build
COPY java/pom.xml .
COPY java/src ./src
RUN mvn -q -B package

FROM flink:1.18-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 && \
    ln -s /usr/bin/python3 /usr/bin/python

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar

COPY src/ /opt/flink/usrlib/src/
COPY --from=java /build/target/flink-sink.jar /opt/flink/usrlib/flink-sink.jar

WORKDIR /opt/flink/usrlib