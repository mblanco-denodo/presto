# Prestissimo: Functional Testing Using Containers

This Java test framework allows you to run test cases by deploying Presto coordinator and worker nodes in containers.
For more information, see [Testcontainers for Java](https://java.testcontainers.org/).
#### To set up Docker services and basic build tools on Ubuntu 22.04 x86_64 machine, run the following commands:
```
apt install podman-docker
apt install make
apt install cmake
```
Run the following commands to fix an open bug related to the CNI network in podman.
Reference: https://bugs.launchpad.net/ubuntu/+source/libpod/+bug/2024394
```
curl -O http://archive.ubuntu.com/ubuntu/pool/universe/g/golang-github-containernetworking-plugins/containernetworking-plugins_1.1.1+ds1-3build1_amd64.deb
dpkg -i containernetworking-plugins_1.1.1+ds1-3build1_amd64.deb
```

## Quick Start

### 1. Build Presto using Maven
The container images required for functional tests are integrated into the presto-native-execution and are built under a Maven profile named _docker-build_.

```bash
./mvnw clean install -DskipTests -Pdocker-build
```

### 2. Run functional tests
#### Using Command Line
Export the following environment variables:
```bash
export TESTCONTAINERS_RYUK_DISABLED=true
export DOCKER_HOST=unix:///run/podman/podman.sock
```
Then, run the functional test using a command similar to this example for `TestPrestoContainerBasicQueries`:
```bash
./mvnw test -pl presto-native-execution -Dtest=com.facebook.presto.nativeworker.TestPrestoContainerBasicQueries
```

#### Using IntelliJ
Go to the tests with containers at  `TestPrestoContainerBasicQueries`.
Edit the run/debug configuration of the test or test case, and add the following as environment variables:
```
TESTCONTAINERS_RYUK_DISABLED=true
DOCKER_HOST=unix:///run/podman/podman.sock
```
Then, run or debug the test.

##### Note
* Existing java and native docker files are reused for functional testing. The coordinator and worker configurations are generated in the utility class.
* The functional test framework has been tested with the tpch.tiny schema, using standard column naming. Please note that this configuration is a current limitation, as it has only been tested with this schema and does not require any data loading.
