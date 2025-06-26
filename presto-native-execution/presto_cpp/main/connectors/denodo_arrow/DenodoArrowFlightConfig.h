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
  [[nodiscard]] static std::optional<std::string> connectionUsername(
    const velox::config::ConfigBase* configBase);
  [[nodiscard]] std::optional<std::string> connectionPassword() const;
  [[nodiscard]] static std::optional<std::string> connectionPassword(
    const velox::config::ConfigBase* configBase);
  [[nodiscard]] std::optional<std::string> connectionUserAgent() const;
  [[nodiscard]] static std::optional<std::string> connectionUserAgent(
    const velox::config::ConfigBase* configBase);
  [[nodiscard]] std::optional<std::string> connectionAuthType() const;
  [[nodiscard]] static std::optional<std::string> connectionAuthType(
    const velox::config::ConfigBase* configBase);

 private:
  static constexpr const char* keyConnectionUsername = "connection.username";
  static constexpr const char* keyConnectionPassword = "connection.password";
  static constexpr const char* keyConnectionUserAgent = "connection.user-agent";
  static constexpr const char* keyConnectionAuthType = "connection.auth-type";
  const std::shared_ptr<const velox::config::ConfigBase> m_config;
};
} // namespace facebook::presto
