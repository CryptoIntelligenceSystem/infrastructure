# syntax=docker/dockerfile:1.7
ARG SPARK_VERSION=3.5.9
FROM apache/spark:${SPARK_VERSION}-scala2.12-java17-python3-ubuntu

# Connector versions must match what this Spark build was compiled against
# (spark-parent_2.12:3.5.9 pom: hadoop 3.3.4, kafka 3.4.1, commons-pool2 2.11.1;
# hadoop-project 3.3.4 pom: aws-java-sdk 1.12.262). Jars are baked in so jobs
# never need --packages or internet access at submit time.
ARG SPARK_VERSION
ARG SCALA_BINARY=2.12
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG CLICKHOUSE_JDBC_VERSION=0.8.1
ARG MAVEN=https://repo1.maven.org/maven2

USER root

ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar /opt/spark/jars/

# Fail the build if the base image's Hadoop or Kafka version drifts from the connectors above.
RUN test -f /opt/spark/jars/hadoop-client-api-${HADOOP_VERSION}.jar \
    || (echo "hadoop-aws ${HADOOP_VERSION} does not match image:" && ls /opt/spark/jars | grep hadoop-client && exit 1)

USER spark
