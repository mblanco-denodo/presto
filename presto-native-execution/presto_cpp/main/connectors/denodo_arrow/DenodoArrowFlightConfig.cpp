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

std::optional<std::string> DenodoArrowFlightConfig::connectionPassword()
    const {
  return std::optional<std::string>(
    m_config->get<std::string>(keyConnectionPassword));
}

std::optional<std::string> DenodoArrowFlightConfig::connectionUserAgent()
    const {
  return std::optional<std::string>(
    m_config->get<std::string>(keyConnectionUserAgent));
}

std::optional<std::string> DenodoArrowFlightConfig::connectionAuthType()
    const {
  return std::optional<std::string>(
    m_config->get<std::string>(keyConnectionAuthType));
}

uint64_t DenodoArrowFlightConfig::connectionQueryTimeout()
    const {
  auto connectionQueryTimeout = static_cast<std::optional<uint64_t>>(
    m_config->get<uint64_t>(keyConnectionQueryTimeout));
  return connectionQueryTimeout.has_value() ?
    connectionQueryTimeout.value() : cDefaultTimeout;
}

std::optional<std::string> DenodoArrowFlightConfig::timePrecisionUnit() const {
  const auto timePrecisionUnit =  std::optional<std::string>(
    m_config->get<std::string>(keyTimePrecisionUnit));
  validatePrecisionValues(timePrecisionUnit);
  return timePrecisionUnit;
}

std::optional<std::string> DenodoArrowFlightConfig::timestampPrecisionUnit()
    const {
  const auto timestampPrecisionUnit = std::optional<std::string>(
    m_config->get<std::string>(keyTimestampPrecisionUnit));
  validatePrecisionValues(timestampPrecisionUnit);
  return timestampPrecisionUnit;
}

bool DenodoArrowFlightConfig::autocommit() const {
  auto value = std::optional<bool>(
    m_config->get<bool>(keyAutocommit));
  return value.has_value() ? value.value() : false;
}

std::optional<std::string> DenodoArrowFlightConfig::workspace() const {
  return std::optional<std::string>(m_config->get<std::string>(keyWorkspace));
}

void DenodoArrowFlightConfig::validatePrecisionValues(
    const std::optional<std::string>& optionalValue) const {
  if (optionalValue.has_value()) {
    std::string unitValue = optionalValue.value();
    VELOX_CHECK(
      cMilliseconds.compare(unitValue) == 0 ||
      cMicroseconds.compare(unitValue) == 0 ||
      cNanoseconds.compare(unitValue) == 0);
  }
}
} // namespace facebook::presto
