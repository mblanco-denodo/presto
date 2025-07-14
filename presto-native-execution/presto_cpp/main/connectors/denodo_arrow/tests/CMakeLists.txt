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

add_executable(
        denodo_arrow_flight_connector_tests
        DenodoArrowFlightConfigTest.cpp)

add_test(denodo_arrow_flight_connector_tests denodo_arrow_flight_connector_tests)

target_link_libraries(
        denodo_arrow_flight_connector_tests
        velox_exec_test_lib
        presto_flight_connector
        denodo_arrow_flight_connector
        gtest
        gtest_main
        presto_protocol)
