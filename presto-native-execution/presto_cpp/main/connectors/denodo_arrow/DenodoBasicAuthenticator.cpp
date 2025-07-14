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
#include "presto_cpp/main/connectors/denodo_arrow/DenodoBasicAuthenticator.h"
#include "presto_cpp/main/connectors/denodo_arrow/DenodoArrowFlightConfig.h"

#include <arrow/flight/middleware.h>
#include <folly/base64.h>

#include "DenodoArrowFlightSqlConnectionConstants.h"

namespace facebook::presto {

DenodoBasicAuthenticator::DenodoBasicAuthenticator(
    std::optional<std::string> username,
    std::optional<std::string> password,
    std::optional<std::string> userAgent,
    std::optional<std::string> timePrecision,
    std::optional<std::string> timestampPrecision,
    const uint64_t queryTimeout,
    const bool autocommit,
    std::optional<std::string> workspace) :
      DenodoAuthenticator (
        userAgent,
        timePrecision,
        timestampPrecision,
        queryTimeout,
        autocommit,
        workspace
        ) {
  VELOX_CHECK(username.has_value(), "missing 'username' value");
  VELOX_CHECK(password.has_value(), "missing 'password' value");
  username_ = username.value();
  password_ = password.value();
}

void DenodoBasicAuthenticator::populateAuthenticationCallHeaders(
    arrow::flight::AddCallHeaders& headerWriter) {
  const std::string base64EncodedUsernameAndPassword =
      folly::base64Encode(username_ + ':' + password_);
  headerWriter.AddHeader(
    DenodoArrowFlightSqlConnectionConstants::authorizationKey,
    DenodoArrowFlightSqlConnectionConstants::basicAuthenticationKey +
    base64EncodedUsernameAndPassword);
  LOG(INFO) << "Writing basic authentication headers for user '" << username_ <<
    "' in query: '" << getQueryContext()->queryId() << "'";
}
} // namespace facebook presto