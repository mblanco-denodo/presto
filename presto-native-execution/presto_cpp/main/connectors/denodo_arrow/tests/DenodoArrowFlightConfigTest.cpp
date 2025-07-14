/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "presto_cpp/main/connectors/denodo_arrow/DenodoArrowFlightConfig.h"
#include <exception>
#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::presto;

TEST(DenodoArrowFlightConfigTest, defaultConfig) {
  std::shared_ptr<config::ConfigBase> rawConfig = std::make_shared<config::ConfigBase>(
      std::move(std::unordered_map<std::string, std::string>{}));
  DenodoArrowFlightConfig config = DenodoArrowFlightConfig(rawConfig);
  ASSERT_EQ(config.connectionUsername(), std::nullopt);
  ASSERT_EQ(config.connectionPassword(), std::nullopt);
  ASSERT_EQ(config.connectionQueryTimeout(), 900000);
  ASSERT_EQ(config.connectionUserAgent(), std::nullopt);
  ASSERT_EQ(config.connectionAuthType(), std::nullopt);
  ASSERT_EQ(config.timestampPrecisionUnit(), std::nullopt);
  ASSERT_EQ(config.timePrecisionUnit(), std::nullopt);
  ASSERT_EQ(config.autocommit(), false);
  ASSERT_EQ(config.workspace(), std::nullopt);
}

TEST(DenodoArrowFlightConfigTest, overrideConfig) {
  std::unordered_map<std::string, std::string> configMap = {
    {"connection.username", "user"},
    {"connection.password", "passwd"},
    {"connection.query-timeout", "9900"},
    {"connection.auth-type", "basic"},
    {"connection.user-agent", "t-user-agent"},
    {"time-precision-unit", "milliseconds"},
    {"timestamp-precision-unit", "nanoseconds"},
    {"autocommit", "true"},
    {"workspace", "default"}
  };
  DenodoArrowFlightConfig config = DenodoArrowFlightConfig(
      std::make_shared<config::ConfigBase>(std::move(configMap)));
  ASSERT_EQ(config.connectionUsername(), "user");
  ASSERT_EQ(config.connectionPassword(), "passwd");
  ASSERT_EQ(config.connectionQueryTimeout(), 9900);
  ASSERT_EQ(config.connectionAuthType(), "basic");
  ASSERT_EQ(config.connectionUserAgent(), "t-user-agent");
  ASSERT_EQ(config.timePrecisionUnit(), "milliseconds");
  ASSERT_EQ(config.timestampPrecisionUnit(), "nanoseconds");
  ASSERT_EQ(config.autocommit(), true);
  ASSERT_EQ(config.workspace(), "default");
}

TEST(DenodoArrowFlightConfigTest, invalidTimePrecisionUnit) {
  std::unordered_map<std::string, std::string> configMap = {
    {"time-precision-unit", "asfl"}
  };
  DenodoArrowFlightConfig config = DenodoArrowFlightConfig(
    std::make_shared<config::ConfigBase>(std::move(configMap)));
  ASSERT_THROW(auto _ = config.timePrecisionUnit(), VeloxRuntimeError);
}

TEST(DenodoArrowFlightConfigTest, invalidTimestampPrecisionUnit) {
  std::unordered_map<std::string, std::string> configMap = {
    {"timestamp-precision-unit", "asfl"}
  };
  DenodoArrowFlightConfig config = DenodoArrowFlightConfig(
    std::make_shared<config::ConfigBase>(std::move(configMap)));
  ASSERT_THROW(auto _ = config.timestampPrecisionUnit(), VeloxRuntimeError);
}

TEST(DenodoArrowFlightConfigTest, invalidQueryTimeout) {
  std::unordered_map<std::string, std::string> configMap = {
    {"connection.query-timeout", "asfl"}
  };
  DenodoArrowFlightConfig config = DenodoArrowFlightConfig(
    std::make_shared<config::ConfigBase>(std::move(configMap)));
  ASSERT_THROW(auto _ = config.connectionQueryTimeout(), folly::ConversionError);
}

TEST(DenodoArrowFlightConfigTest, nonPositiveQueryTimeout) {
  std::unordered_map<std::string, std::string> configMap = {
    {"connection.query-timeout", "-1239"}
  };
  DenodoArrowFlightConfig config = DenodoArrowFlightConfig(
    std::make_shared<config::ConfigBase>(std::move(configMap)));
  ASSERT_THROW(auto _ = config.connectionQueryTimeout(), folly::ConversionError);
}

TEST(DenodoArrowFlightConfigTest, nonIntegerQueryTimeout) {
  std::unordered_map<std::string, std::string> configMap = {
    {"connection.query-timeout", "1.124"}
  };
  DenodoArrowFlightConfig config = DenodoArrowFlightConfig(
    std::make_shared<config::ConfigBase>(std::move(configMap)));
  ASSERT_THROW(auto _ = config.connectionQueryTimeout(), folly::ConversionError);
}

TEST(DenodoArrowFlightConfigTest, nonIntegerNonPositiveQueryTimeout) {
  std::unordered_map<std::string, std::string> configMap = {
    {"connection.query-timeout", "-1.124"}
  };
  DenodoArrowFlightConfig config = DenodoArrowFlightConfig(
    std::make_shared<config::ConfigBase>(std::move(configMap)));
  ASSERT_THROW(auto _ = config.connectionQueryTimeout(), folly::ConversionError);
}


TEST(DenodoArrowFlightConfigTest, test){}
