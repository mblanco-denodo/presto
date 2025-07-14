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
#pragma once

#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConfig.h"

namespace facebook::presto {

class DenodoArrowFlightConfig final : public ArrowFlightConfig {
public:
  explicit DenodoArrowFlightConfig(
     std::shared_ptr<const velox::config::ConfigBase> config)
     : ArrowFlightConfig(config), m_config{config} {}

  [[nodiscard]] std::optional<std::string> connectionUsername() const;
  [[nodiscard]] std::optional<std::string> connectionPassword() const;
  [[nodiscard]] std::optional<std::string> connectionUserAgent() const;
  [[nodiscard]] std::optional<std::string> connectionAuthType() const;
  [[nodiscard]] uint64_t connectionQueryTimeout() const;
  [[nodiscard]] std::optional<std::string> timePrecisionUnit() const;
  [[nodiscard]] std::optional<std::string> timestampPrecisionUnit() const;
  [[nodiscard]] bool autocommit() const;
  [[nodiscard]] std::optional<std::string> workspace() const;

 private:
  static constexpr const char* keyConnectionUsername = "connection.username";
  static constexpr const char* keyConnectionPassword = "connection.password";
  static constexpr const char* keyConnectionUserAgent = "connection.user-agent";
  static constexpr const char* keyConnectionAuthType = "connection.auth-type";
  static constexpr const char* keyConnectionQueryTimeout = "connection.query-timeout";
  static constexpr const char* keyTimePrecisionUnit = "time-precision-unit";
  static constexpr const char* keyTimestampPrecisionUnit = "timestamp-precision-unit";
  static constexpr const char* keyAutocommit = "autocommit";
  static constexpr const char* keyWorkspace = "workspace";
  const std::string cMilliseconds = "milliseconds";
  const std::string cMicroseconds = "microseconds";
  const std::string cNanoseconds = "nanoseconds";
  static const uint64_t cDefaultTimeout = 900000;
  const std::shared_ptr<const velox::config::ConfigBase> m_config;

  void validatePrecisionValues(
    const std::optional<std::string>& optionalValue) const;
};
} // namespace facebook::presto
