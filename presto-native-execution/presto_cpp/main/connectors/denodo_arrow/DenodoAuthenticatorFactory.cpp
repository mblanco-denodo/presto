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
#include "presto_cpp/main/connectors/denodo_arrow/DenodoAuthenticatorFactory.h"
#include "presto_cpp/main/connectors/denodo_arrow/DenodoBasicAuthenticator.h"
#include "presto_cpp/main/connectors/denodo_arrow/DenodoArrowFlightConfig.h"

namespace facebook::presto {
std::shared_ptr<Authenticator> DenodoAuthenticatorFactory::newAuthenticator(
  std::shared_ptr<const velox::config::ConfigBase> config) {
  const auto denodoArrowFlightConfig =
    std::make_unique<DenodoArrowFlightConfig>(config);
  const std::optional<std::string> denodoArrowFlightAuthType =
    denodoArrowFlightConfig->connectionAuthType();
  VELOX_CHECK(denodoArrowFlightAuthType.has_value(),
    "Authentication type is null");
  if (strcmp(basicAuth, denodoArrowFlightAuthType.value().c_str()) == 0) {
    return std::make_shared<DenodoBasicAuthenticator>(
      denodoArrowFlightConfig->connectionUsername(),
      denodoArrowFlightConfig->connectionPassword(),
      denodoArrowFlightConfig->connectionUserAgent(),
      denodoArrowFlightConfig->timePrecisionUnit(),
      denodoArrowFlightConfig->timestampPrecisionUnit(),
      denodoArrowFlightConfig->connectionQueryTimeout(),
      denodoArrowFlightConfig->autocommit(),
      denodoArrowFlightConfig->workspace());
  }
  if (strcmp(oauthAuth, denodoArrowFlightAuthType.value().c_str()) == 0) {
    // TODO
  }
  return nullptr;
}

AFC_REGISTER_AUTH_FACTORY(std::make_shared<DenodoAuthenticatorFactory>());
} // namespace facebook::presto
