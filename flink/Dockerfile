
FROM flink:1.20.1-scala_2.12-java17

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==1.20.1

COPY flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/

USER flink
