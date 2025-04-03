FROM ubuntu:22.04

# Install necessary tools
RUN apt-get update && apt-get install -y \
	curl \
	git \
	wget \
	unzip \
	tar \
	openjdk-8-jdk
# Set a default timezone, can be overriden via ARG
ARG tz="America/New_York"
ARG DEBIAN_FRONTEND="noninteractive"
ENV PROMPT_ALWAYS_RESPOND=n
ENV SUDO=" "
# TZ and DEBIAN_FRONTEND="noninteractive"
# are required to avoid tzdata installation
# to prompt for region selection.
ENV TZ=${tz}
WORKDIR presto-native-execution
RUN make submodules
RUN mkdir -p /scripts /velox/scripts
COPY scripts /scripts
COPY velox/scripts /velox/scripts
# setup-adapters.sh does not install rpm needed for minio install.
RUN mkdir build && \
    (cd build && ../scripts/setup-ubuntu.sh && \
                         apt install -y rpm && \
                 ../velox/scripts/setup-adapters.sh && \
                 ../scripts/setup-adapters.sh ) && \
    rm -rf build

WORKDIR cd ..
