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


namespace facebook::presto {

void DenodoBasicAuthenticator::authenticateClient(
    std::unique_ptr<arrow::flight::FlightClient>& client,
    const velox::config::ConfigBase* sessionProperties,
    arrow::flight::AddCallHeaders& headerWriter) {
  std::optional<std::string> username =
    DenodoArrowFlightConfig::connectionUsername(sessionProperties);
  if (!username.has_value()) {
    // TODO throw RE
  }
  std::optional<std::string> password =
    DenodoArrowFlightConfig::connectionPassword(sessionProperties);
  if (!password.has_value()) {
    // TODO throw RE
  }
  std::optional<std::string> userAgent =
    DenodoArrowFlightConfig::connectionUserAgent(sessionProperties);
  if (!userAgent.has_value()) {
    // TODO throw RE
  }
  const std::string base64EncodedUsernameAndPassword =
      folly::base64Encode(username.value() + ':' + password.value());
  headerWriter.AddHeader(authorizationKey,
    basicAuthenticationKey + base64EncodedUsernameAndPassword);
  headerWriter.AddHeader(xForwardedUserAgentKey, userAgent.value());
  headerWriter.AddHeader(mppQueryIdKey, getQueryContext()->queryId());
}
}