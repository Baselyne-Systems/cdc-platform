FROM quay.io/debezium/connect:2.7
ARG CONFLUENT_VERSION=7.9.0
RUN mkdir -p /kafka/connect/confluent-avro-converter && \
    cd /kafka/connect/confluent-avro-converter && \
    curl -sL https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/$CONFLUENT_VERSION/kafka-connect-avro-converter-$CONFLUENT_VERSION.jar -o kafka-connect-avro-converter.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/$CONFLUENT_VERSION/kafka-connect-avro-data-$CONFLUENT_VERSION.jar -o kafka-connect-avro-data.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/$CONFLUENT_VERSION/kafka-schema-registry-client-$CONFLUENT_VERSION.jar -o kafka-schema-registry-client.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/$CONFLUENT_VERSION/kafka-avro-serializer-$CONFLUENT_VERSION.jar -o kafka-avro-serializer.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/$CONFLUENT_VERSION/kafka-schema-serializer-$CONFLUENT_VERSION.jar -o kafka-schema-serializer.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/$CONFLUENT_VERSION/kafka-schema-converter-$CONFLUENT_VERSION.jar -o kafka-schema-converter.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/common-config/$CONFLUENT_VERSION/common-config-$CONFLUENT_VERSION.jar -o common-config.jar && \
    curl -sL https://packages.confluent.io/maven/io/confluent/common-utils/$CONFLUENT_VERSION/common-utils-$CONFLUENT_VERSION.jar -o common-utils.jar && \
    curl -sL https://repo1.maven.org/maven2/org/apache/avro/avro/1.12.0/avro-1.12.0.jar -o avro.jar && \
    curl -sL https://repo1.maven.org/maven2/com/google/guava/guava/33.4.0-jre/guava-33.4.0-jre.jar -o guava.jar && \
    curl -sL https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.2/failureaccess-1.0.2.jar -o failureaccess.jar && \
    curl -sL https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.27.1/commons-compress-1.27.1.jar -o commons-compress.jar
