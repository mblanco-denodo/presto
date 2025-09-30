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

#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/auth/Authenticator.h"
#include "presto_cpp/main/connectors/denodo_arrow/DenodoArrowFlightConfig.h"
#include "velox/connectors/Connector.h"

namespace facebook::presto {
class DenodoArrowFlightConnector final : public ArrowFlightConnector {
public:
  explicit DenodoArrowFlightConnector(
     const std::string& id,
     std::shared_ptr<const velox::config::ConfigBase> config,
     const char* authenticatorName = nullptr)
     : ArrowFlightConnector(id, config, authenticatorName),
       flightConfig_(std::make_shared<DenodoArrowFlightConfig>(config)),
       clientOpts_(initClientOpts(flightConfig_)),
       authenticator_(getAuthenticatorFactory(
                          authenticatorName
                              ? authenticatorName
                              : flightConfig_->authenticatorName())
                          ->newAuthenticator(config)) {}

  std::unique_ptr<velox::connector::DataSource> createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::connector::ConnectorQueryCtx* connectorQueryCtx) override;

private:
  static std::shared_ptr<arrow::flight::FlightClientOptions> initClientOpts(
      const std::shared_ptr<DenodoArrowFlightConfig>& config);

  const std::shared_ptr<DenodoArrowFlightConfig> flightConfig_;
  const std::shared_ptr<arrow::flight::FlightClientOptions> clientOpts_;
  const std::shared_ptr<Authenticator> authenticator_;
};
} // namespace facebook::presto
