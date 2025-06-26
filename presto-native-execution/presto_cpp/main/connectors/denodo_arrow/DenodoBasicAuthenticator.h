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

#include "presto_cpp/main/connectors/denodo_arrow/DenodoAuthenticator.h"

namespace facebook::presto {
class DenodoBasicAuthenticator final : public DenodoAuthenticator {
public:
  DenodoBasicAuthenticator() : DenodoAuthenticator(){}

  void authenticateClient(
      std::unique_ptr<arrow::flight::FlightClient>& client,
      const velox::config::ConfigBase* sessionProperties,
      arrow::flight::AddCallHeaders& headerWriter) override;

private:
  static constexpr const char* authorizationKey = "Authorization";
  static constexpr const char* basicAuthenticationKey = "Basic ";
  static constexpr const char* xForwardedUserAgentKey = "x-forwarded-user-agent";
  static constexpr const char* mppQueryIdKey = "mpp-query-id";
};
}
