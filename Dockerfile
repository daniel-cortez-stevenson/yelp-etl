FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

ENV ICEBERG_VERSION 1.4.0

RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
        -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar \
        -o /opt/spark/jars/wildfly-openssl-1.0.7.Final.jar && \
    curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
        -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar && \
    curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
        -Lo /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

ADD spark-defaults.conf /opt/spark/conf/

ADD run-all-pipelines.sh README.md ./
