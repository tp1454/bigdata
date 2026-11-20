FROM flink:1.18-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

COPY requirements.txt /opt/flink/usrlib/requirements.txt
RUN pip install -r /opt/flink/usrlib/requirements.txt

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.5/clickhouse-jdbc-0.6.5-all.jar

COPY src/ /opt/flink/usrlib/src/

# THÊM DÒNG NÀY ĐỂ BÁO CHO PYTHON BIẾT NƠI TÌM MODULE
ENV PYTHONPATH=/opt/flink/usrlib/src

WORKDIR /opt/flink/usrlib