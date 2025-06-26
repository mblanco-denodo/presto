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

#include "presto_cpp/main/connectors/arrow_flight/auth/Authenticator.h"
#include "velox/connectors/Connector.h"

namespace facebook::presto {

#define REGISTER_DENODO_AUTH_FACTORY(factory)                                \
namespace {                                                                  \
static bool FB_ANONYMOUS_VARIABLE(g_ConnectorFactory) =                      \
::facebook::presto::registerAuthenticatorFactory((factory)); \
}

class DenodoAuthenticatorFactory final : public AuthenticatorFactory {
public:
  DenodoAuthenticatorFactory() : AuthenticatorFactory{kAuthenticatorName} {}

  explicit DenodoAuthenticatorFactory(const std::string_view name)
      : AuthenticatorFactory{name} {}

  // the static function has to be defined in the header as it is used
  // outside the matching .cpp file definitions. Inline functions have
  // to be defined in every .cpp file that use them, otherwise, while the
  // compiler might have knowledge of it, there might be linker issues
  static constexpr const char* authenticatorName() {
    return kAuthenticatorName;
  }

  std::shared_ptr<Authenticator> newAuthenticator(
      std::shared_ptr<const velox::config::ConfigBase> config) override;

private:
  static constexpr const char* kAuthenticatorName = "DenodoAuthenticatorFactory";
  static constexpr const char* basicAuth = "basic";
  static constexpr const char* oauthAuth = "oauth";
  const velox::connector::ConnectorQueryCtx * connectorQueryContext{nullptr};
};
}