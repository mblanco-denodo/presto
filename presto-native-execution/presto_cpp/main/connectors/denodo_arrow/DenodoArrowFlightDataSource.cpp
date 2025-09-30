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
#include "presto_cpp/main/connectors/denodo_arrow/DenodoArrowFlightDataSource.h"
#include <arrow/c/bridge.h>
#include <arrow/flight/client.h>
#include <utility>

#include "presto_cpp/main/connectors/denodo_arrow/DenodoAuthenticator.h"
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/connectors/arrow_flight/auth/Authenticator.h"
#include "velox/vector/arrow/Bridge.h"

namespace facebook::presto {
DenodoArrowFlightDataSource::DenodoArrowFlightDataSource(
    const velox::RowTypePtr& outputType,
    const velox::connector::ColumnHandleMap& columnHandles,
    std::shared_ptr<Authenticator> authenticator,
    const velox::connector::ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<ArrowFlightConfig>& flightConfig,
    const std::shared_ptr<arrow::flight::FlightClientOptions>& clientOpts)
    : ArrowFlightDataSource(outputType, columnHandles, authenticator,
      connectorQueryCtx, flightConfig, clientOpts) {
  authenticator_ = std::move(authenticator);
  connectorQueryCtx_ = connectorQueryCtx;
}

void DenodoArrowFlightDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  std::shared_ptr<DenodoAuthenticator> denodoAuthenticator =
    std::dynamic_pointer_cast<DenodoAuthenticator>(authenticator_);
  denodoAuthenticator->setQueryContext(connectorQueryCtx_);
  ArrowFlightDataSource::addSplit(split);
}
} // namespace facebook presto
