FROM maven:3.9-eclipse-temurin-11 AS java
WORKDIR /build
COPY java/pom.xml .
COPY java/src ./src
RUN mvn -q -B -DskipTests package

FROM flink:1.18-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

COPY requirements.txt /opt/flink/usrlib/requirements.txt
RUN pip install -r /opt/flink/usrlib/requirements.txt

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar

COPY src/ /opt/flink/usrlib/src/
COPY --from=java /build/target/crypto-sink.jar /opt/flink/usrlib/crypto-sink.jar

# THÊM DÒNG NÀY ĐỂ BÁO CHO PYTHON BIẾT NƠI TÌM MODULE
ENV PYTHONPATH=/opt/flink/usrlib/src

WORKDIR /opt/flink/usrlib