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

#include "presto_cpp/main/connectors/denodo_arrow/DenodoArrowFlightConnector.h"
#include "velox/connectors/Connector.h"

namespace facebook::presto {
class DenodoArrowFlightConnectorFactory final :
public velox::connector::ConnectorFactory {
public:
  DenodoArrowFlightConnectorFactory() : ConnectorFactory(kConnectorName) {}

  explicit DenodoArrowFlightConnectorFactory(
      const char* name,
      const char* authenticatorName = nullptr)
      : ConnectorFactory(name), authenticatorName_(authenticatorName) {}

  std::shared_ptr<velox::connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const velox::config::ConfigBase> config,
      folly::Executor* ioExecutor,
      folly::Executor* cpuExecutor) override {
    return std::make_shared<DenodoArrowFlightConnector>(
        id, config, authenticatorName_);
  }

  static constexpr const char* keyConnectorName() {
    return kConnectorName;
  }

private:
  static constexpr const char* kConnectorName = "denodo-arrow";
  const char* authenticatorName_{nullptr};
};
} // namespace facebook::presto