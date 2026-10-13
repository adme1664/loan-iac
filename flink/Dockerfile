ARG FLINK_VERSION
FROM flink:${FLINK_VERSION}

ARG FLINK_KAFKA_CONNECTOR_VERSION

# Kafka connector for Flink SQL (source, sink and upsert-kafka)
ADD --chown=flink:flink \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar \
    /opt/flink/lib/

# Owned by flink so the checkpoint volume mounted here is writable
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints
