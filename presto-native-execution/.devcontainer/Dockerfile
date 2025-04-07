# Presto native Dev Container Dockerfile
# @author Miguel Blanco Godón (mblanco@denodo.com)

# Set base image
FROM quay.io/centos/centos:stream9

# Set toolchain and platform environment variables
ENV PROMPT_ALWAYS_RESPOND=n
ENV CC=/opt/rh/gcc-toolset-12/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-12/root/bin/g++

ARG OSNAME=centos
ARG BUILD_TYPE=Debug
ARG EXTRA_CMAKE_FLAGS=''
ARG NUM_THREADS=8

ENV PROMPT_ALWAYS_RESPOND=n
ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=""
ENV DEPENDENCY_DIR="build"
ENV INSTALL_PREFIX="build"

# Install necessary packages
RUN dnf config-manager --enable crb
RUN dnf update && dnf install -y epel-release
RUN dnf install -y git \
    make \
    which\
    ninja-build \
    ccache \
    boost \
    boost-devel

# Load Velox submodule
#RUN make submodules

# Install dependencies
#RUN mkdir $INSTALL_PREFIX && \
#    ./scripts/setup-centos.sh && \
#    ./velox/scripts/setup-adapters.sh && \
#    ./scripts/setup-adapters.sh && \
#    ./velox/scripts/setup-centos9.sh install_clang15 && \
#    rm -rf $INSTALL_PREFIX

# Compile Presto native
#RUN EXTRA_CMAKE_FLAGS=$EXTRA_CMAKE_FLAGS \
#    NUM_THREADS=$NUM_THREADS make cmake-and-build BUILD_TYPE=$BUILD_TYPE BUILD_DIR=$BUILD_DIR BUILD_BASE_DIR=$BUILD_BASE_DIR

# Check for missing dependencies in the build
#RUN !(LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib64 ldd $BUILD_BASE_DIR/$BUILD_DIR/presto_cpp/main/presto_server  | grep "not found") && \
#    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib64 ldd $BUILD_BASE_DIR/$BUILD_DIR/presto_cpp/main/presto_server | awk 'NF == 4 { system("cp " $3 " /runtime-libraries") }'
