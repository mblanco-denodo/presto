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

namespace facebook::presto {
std::optional<std::string> DenodoArrowFlightConfig::connectionUsername()
    const {
  return std::optional<std::string>(
    m_config->get<std::string>(keyConnectionUsername));
}

std::optional<std::string> DenodoArrowFlightConfig::connectionUsername(
    const velox::config::ConfigBase* configBase) {
  return std::optional<std::string>(
    configBase->get<std::string>(keyConnectionUsername));
}


std::optional<std::string> DenodoArrowFlightConfig::connectionPassword()
    const {
  return std::optional<std::string>(
    m_config->get<std::string>(keyConnectionPassword));
}

std::optional<std::string> DenodoArrowFlightConfig::connectionPassword(
    const velox::config::ConfigBase* configBase) {
  return std::optional<std::string>(
    configBase->get<std::string>(keyConnectionPassword));
}

std::optional<std::string> DenodoArrowFlightConfig::connectionUserAgent()
    const {
  return std::optional<std::string>(
    m_config->get<std::string>(keyConnectionUserAgent));
}

std::optional<std::string> DenodoArrowFlightConfig::connectionUserAgent(
    const velox::config::ConfigBase* configBase) {
  return std::optional<std::string>(
    configBase->get<std::string>(keyConnectionUserAgent));
}

std::optional<std::string> DenodoArrowFlightConfig::connectionAuthType()
    const {
  return std::optional<std::string>(
    m_config->get<std::string>(keyConnectionAuthType));
}

std::optional<std::string> DenodoArrowFlightConfig::connectionAuthType(
    const velox::config::ConfigBase* configBase) {
  return std::optional<std::string>(
    configBase->get<std::string>(keyConnectionAuthType));
}
} // namespace facebook::presto