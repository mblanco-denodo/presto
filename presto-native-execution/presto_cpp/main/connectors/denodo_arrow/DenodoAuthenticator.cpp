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

#include "presto_cpp/main/connectors/denodo_arrow/DenodoArrowFlightSqlConnectionConstants.h"
#include "presto_cpp/main/connectors/denodo_arrow/DenodoAuthenticator.h"

#include <arrow/flight/middleware.h>

namespace facebook::presto {
DenodoAuthenticator::DenodoAuthenticator(
  std::optional<std::string> userAgent,
  std::optional<std::string> timePrecision,
  std::optional<std::string> timestampPrecision,
  const uint64_t queryTimeout,
  const bool autocommit,
  std::optional<std::string> workspace) {
  VELOX_CHECK(userAgent.has_value(), "missing 'user-agent' value");
  userAgent_ = userAgent.value();
  timePrecision_ = timePrecision.has_value() ? timePrecision.value() : "";
  timestampPrecision_ = timestampPrecision.has_value() ?
    timestampPrecision.value() : "";
  queryTimeout_ = queryTimeout;
  autocommit_ = autocommit;
  workspace_ = workspace.has_value() ? workspace.value() : "";
}

void DenodoAuthenticator::authenticateClient(
    std::unique_ptr<arrow::flight::FlightClient>& client,
    const velox::config::ConfigBase* sessionProperties,
    arrow::flight::AddCallHeaders& headerWriter) {
  headerWriter.AddHeader(
    DenodoArrowFlightSqlConnectionConstants::xForwardedUserAgentKey,
    userAgent_);
  headerWriter.AddHeader(
    DenodoArrowFlightSqlConnectionConstants::mppQueryIdKey,
    getQueryContext()->queryId());
  if (!timePrecision_.empty()) {
    headerWriter.AddHeader(
      DenodoArrowFlightSqlConnectionConstants::timePrecisionKey,
      timePrecision_);
  }
  if (!timestampPrecision_.empty()) {
    headerWriter.AddHeader(
      DenodoArrowFlightSqlConnectionConstants::timestampPrecisionKey,
      timestampPrecision_);
  }
  headerWriter.AddHeader(
    DenodoArrowFlightSqlConnectionConstants::autocommitKey,
    autocommit_ ? "true" : "false");
  if (!workspace_.empty()) {
    headerWriter.AddHeader(
      DenodoArrowFlightSqlConnectionConstants::workspaceKey,
      workspace_);
  }
  auto callOptions =
    dynamic_cast<arrow::flight::FlightCallOptions*>(&headerWriter);
  std::chrono::milliseconds milliseconds(queryTimeout_);
  arrow::flight::TimeoutDuration timeoutMillis =
    std::chrono::duration_cast<arrow::flight::TimeoutDuration>(milliseconds);
  callOptions->timeout = timeoutMillis;
  populateAuthenticationCallHeaders(headerWriter);
}
} // namespace facebook::presto