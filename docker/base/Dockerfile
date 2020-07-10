FROM openjdk:8-jre-slim

ENV DAEMON_RUN=true
ENV SPARK_VERSION=3.0.0
ENV HADOOP_VERSION=2.7
ENV SPARK_HOME=/spark

RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools ca-certificates jq

#     apt update && apt -y upgrade \
#     apt install -y wget ca-certificates && \
#     apt install -y curl bash jq && \

# RUN apk add --no-cache --virtual=.build-dependencies wget ca-certificates && \
#     apk add --no-cache bash curl jq && \
#     cd "/tmp" && \
#     wget --no-verbose "https://downloads.typesafe.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz" && \
#     tar xzf "scala-${SCALA_VERSION}.tgz" && \
#     mkdir "${SCALA_HOME}" && \
#     rm "/tmp/scala-${SCALA_VERSION}/bin/"*.bat && \
#     mv "/tmp/scala-${SCALA_VERSION}/bin" "/tmp/scala-${SCALA_VERSION}/lib" "${SCALA_HOME}" && \
#     ln -s "${SCALA_HOME}/bin/"* "/usr/bin/" && \
#     apk del .build-dependencies && \
#     rm -rf "/tmp/"*


# Add Dependencies for PySpark
RUN apt-get install -y python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy
RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1


RUN wget --no-verbose http://apache.stu.edu.tw/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Pyspark installation
RUN pip3  install pyspark

ENV PYSPARK_PYTHON="/usr/bin/python3"

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV PYTHONHASHSEED 1
