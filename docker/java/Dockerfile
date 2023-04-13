FROM debian:11.6-slim
RUN apt update && apt upgrade -y
RUN apt install -y \
      openjdk-11-jdk \
      wget \
      jq
ENV JAVA_HOME /etc/alternatives/jre
