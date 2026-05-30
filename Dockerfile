ARG FLINK_VERSION=2.1

FROM apache/flink:${FLINK_VERSION}-java21

SHELL ["/bin/bash", "-c"]

ARG ICEBERG_FLINK_RUNTIME_VERSION=2.1
ARG ICEBERG_VERSION=1.11.0
ARG HADOOP_VERSION=3.4.2
ARG KAFKA_CONNECTOR_VERSION=4.0.1-2.0

USER flink
WORKDIR /opt/flink

RUN echo "-> Iceberg Flink runtime 2.1" && \
    mkdir -p ./lib/iceberg && pushd ./lib/iceberg && \
    curl -fsSL -O "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${ICEBERG_FLINK_RUNTIME_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${ICEBERG_FLINK_RUNTIME_VERSION}-${ICEBERG_VERSION}.jar" && \
    curl -fsSL -O "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" && \
    popd

RUN echo "-> Hadoop client" && \
    mkdir -p ./lib/hadoop && pushd ./lib/hadoop && \
    curl -fsSL -O "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar" && \
    curl -fsSL -O "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar" && \
    popd

# Kafka connector for 2.1 not on Maven yet; 4.0.1-2.0 works with Flink 2.1 runtime
RUN echo "-> Kafka connector" && \
    curl -fsSL -o ./lib/flink-connector-kafka.jar \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" && \
    curl -fsSL -o ./lib/kafka-clients.jar \
      "https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar"

RUN mv ./lib/iceberg/*.jar ./lib/ && rmdir ./lib/iceberg && \
    mv ./lib/hadoop/*.jar ./lib/ && rmdir ./lib/hadoop
