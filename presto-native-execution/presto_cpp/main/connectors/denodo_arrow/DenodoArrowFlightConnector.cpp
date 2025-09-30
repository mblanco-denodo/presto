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
#include "presto_cpp/main/connectors/denodo_arrow/DenodoArrowFlightConnector.h"
#include <arrow/flight/api.h>

#include "DenodoArrowFlightDataSource.h"
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"

namespace facebook::presto {
std::shared_ptr<arrow::flight::FlightClientOptions>
DenodoArrowFlightConnector::initClientOpts(
    const std::shared_ptr<DenodoArrowFlightConfig>& config) {
  auto clientOpts = std::make_shared<arrow::flight::FlightClientOptions>();
  clientOpts->disable_server_verification = !config->serverVerify();

  auto certPath = config->serverSslCertificate();
  if (certPath.has_value()) {
    std::ifstream file(certPath.value());
    VELOX_CHECK(file.is_open(), "Could not open TLS certificate");
    std::string cert(
        (std::istreambuf_iterator<char>(file)),
        (std::istreambuf_iterator<char>()));
    clientOpts->tls_root_certs = cert;
  }
  // TODO
  // verify auth properties here

  return clientOpts;
}

std::unique_ptr<velox::connector::DataSource>
DenodoArrowFlightConnector::createDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ConnectorTableHandlePtr& tableHandle,
    const velox::connector::ColumnHandleMap& columnHandles,
    velox::connector::ConnectorQueryCtx* connectorQueryCtx) {
  return std::make_unique<DenodoArrowFlightDataSource>(
      outputType,
      columnHandles,
      authenticator_,
      connectorQueryCtx,
      flightConfig_,
      clientOpts_);
}
} // namespace facebook::presto
