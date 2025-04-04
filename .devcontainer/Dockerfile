
FROM quay.io/centos/centos:stream9

ENV PROMPT_ALWAYS_RESPOND=n
ENV CC=/opt/rh/gcc-toolset-12/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-12/root/bin/g++

RUN dnf update && dnt install git

