# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM registry.access.redhat.com/ubi9-minimal:9.6

# Set this when build arm with common flags
# from https://github.com/facebookincubator/velox/pull/14366
ARG ARM_BUILD_TARGET

ENV PROMPT_ALWAYS_RESPOND=y
ENV CC=/opt/rh/gcc-toolset-12/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-12/root/bin/g++
ENV ARM_BUILD_TARGET=${ARM_BUILD_TARGET}

# ubi9-minimal does not have dnf so we have install it
RUN microdnf install -y dnf
# Manually define the CRB repository because some packages still not found
RUN echo -e "[centos-baseos]\n\
name=CentOS Stream 9 - BaseOS\n\
baseurl=http://mirror.stream.centos.org/9-stream/BaseOS/x86_64/os/\n\
gpgcheck=0\n\
enabled=1\n\
\n\
[centos-appstream]\n\
name=CentOS Stream 9 - AppStream\n\
baseurl=http://mirror.stream.centos.org/9-stream/AppStream/x86_64/os/\n\
gpgcheck=0\n\
enabled=1\n\
\n\
[centos-crb]\n\
name=CentOS Stream 9 - CRB\n\
baseurl=http://mirror.stream.centos.org/9-stream/CRB/x86_64/os/\n\
gpgcheck=0\n\
enabled=1" > /etc/yum.repos.d/centos-deps.repo
# In UBI we need to install epel manually
RUN rpm -ivh https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm
RUN dnf install -y dnf-plugins-core
# Enable CRB
RUN dnf config-manager --add-repo https://mirror.stream.centos.org/9-stream/CRB/x86_64/os/ \
    && echo "gpgcheck=0" >> /etc/yum.repos.d/mirror.stream.centos.org_9-stream_CRB_x86_64_os_.repo
# Set up repos
RUN microdnf install -y epel-release
RUN dnf upgrade -y && dnf clean all && dnf makecache
# Remove conflicting FIPS stub package manually
RUN rpm -e --nodeps openssl-fips-provider-so || true

RUN mkdir -p /scripts /velox/scripts
COPY scripts /scripts
COPY velox/scripts /velox/scripts
# Copy extra script called during setup.
# from https://github.com/facebookincubator/velox/pull/14016
COPY velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch /velox
ENV VELOX_ARROW_CMAKE_PATCH=/velox/cmake-compatibility.patch

RUN bash -c "mkdir build && \
    (cd build && ../scripts/setup-centos.sh && \
                 ../scripts/setup-adapters.sh && \
                 source ../velox/scripts/setup-centos9.sh && \
                 source ../velox/scripts/setup-centos-adapters.sh && \
                 install_adapters && \
                 install_clang15 && \
                 install_cuda 12.8) && \
    rm -rf build"

# put CUDA binaries on the PATH
ENV PATH=/usr/local/cuda/bin:${PATH}

# configuration for nvidia-container-toolkit
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES="compute,utility"
