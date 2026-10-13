ARG KAFKA_CONNECT_VERSION
FROM confluentinc/cp-kafka-connect:${KAFKA_CONNECT_VERSION}

ARG KAFKA_CONNECT_JDBC_VERSION
ARG KAFKA_CONNECT_JSON_SCHEMA_VERSION

# JDBC source/sink connector (bundles the PostgreSQL driver) and the
# jcustenborder JSON Schema transformations (FromJson SMT)
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:${KAFKA_CONNECT_JDBC_VERSION} \
 && confluent-hub install --no-prompt jcustenborder/kafka-connect-json-schema:${KAFKA_CONNECT_JSON_SCHEMA_VERSION}
