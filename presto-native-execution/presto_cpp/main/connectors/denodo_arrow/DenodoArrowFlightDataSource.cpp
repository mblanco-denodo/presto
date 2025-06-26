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
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/flight/api.h>
#include <boost/asio/query.hpp>
#include <folly/base64.h>
#include <utility>
#include "presto_cpp/main/connectors/denodo_arrow/DenodoCallOptions.h"

#include "presto_cpp/main/connectors/denodo_arrow/DenodoAuthenticator.h"
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/common/ConfigReader.h"
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"
#include "presto_cpp/main/connectors/arrow_flight/auth/Authenticator.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook::velox::connector;

namespace facebook::presto {

std::shared_ptr<arrow::flight::Location> getDefaultLocation(
    const std::shared_ptr<ArrowFlightConfig>& config) {
  auto defaultHost = config->defaultServerHostname();
  auto defaultPort = config->defaultServerPort();
  if (!defaultHost.has_value() || !defaultPort.has_value()) {
    return nullptr;
  }

  AFC_ASSIGN_OR_RAISE(
      auto defaultLocation,
      config->defaultServerSslEnabled()
          ? arrow::flight::Location::ForGrpcTls(
                defaultHost.value(), defaultPort.value())
          : arrow::flight::Location::ForGrpcTcp(
                defaultHost.value(), defaultPort.value()));

  return std::make_shared<arrow::flight::Location>(std::move(defaultLocation));
}

DenodoArrowFlightDataSource::DenodoArrowFlightDataSource(
    const velox::RowTypePtr& outputType,
    const std::unordered_map<std::string,
    std::shared_ptr<velox::connector::ColumnHandle>>& columnHandles,
    std::shared_ptr<Authenticator> authenticator,
    const velox::connector::ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<ArrowFlightConfig>& flightConfig,
    const std::shared_ptr<arrow::flight::FlightClientOptions>& clientOpts)
    : ArrowFlightDataSource(outputType, columnHandles, authenticator,
      connectorQueryCtx, flightConfig, clientOpts),
      outputType_{outputType},
      authenticator_{std::move(authenticator)},
      connectorQueryCtx_{connectorQueryCtx},
      flightConfig_{flightConfig},
      clientOpts_{clientOpts},
      defaultLocation_(getDefaultLocation(flightConfig_)) {
  VELOX_CHECK_NOT_NULL(clientOpts_, "FlightClientOptions is not initialized");

  // columnMapping_ contains the real column names in the expected order.
  // This is later used by projectOutputColumns to filter out unnecessary
  // columns from the fetched chunk.
  columnMapping_.reserve(outputType_->size());

  for (const auto& columnName : outputType_->names()) {
    auto it = columnHandles.find(columnName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "missing columnHandle for column '{}'",
        columnName);

    auto handle =
        std::dynamic_pointer_cast<ArrowFlightColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(
        handle,
        "handle for column '{}' is not an ArrowFlightColumnHandle",
        columnName);

    columnMapping_.push_back(handle->name());
  }
}

void DenodoArrowFlightDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  auto flightSplit = std::dynamic_pointer_cast<ArrowFlightSplit>(split);
  VELOX_CHECK(
      flightSplit, "ArrowFlightDataSource received wrong type of split");

  auto flightEndpointStr =
      folly::base64Decode(flightSplit->flightEndpointBytes_);

  arrow::flight::FlightEndpoint flightEndpoint;
  AFC_ASSIGN_OR_RAISE(
      flightEndpoint,
      arrow::flight::FlightEndpoint::Deserialize(flightEndpointStr));

  arrow::flight::Location loc;
  if (!flightEndpoint.locations.empty()) {
    loc = flightEndpoint.locations[0];
  } else {
    VELOX_CHECK_NOT_NULL(
        defaultLocation_,
        "No location from Flight endpoint, default host or port is missing");
    loc = *defaultLocation_;
  }

  AFC_ASSIGN_OR_RAISE(
      auto client, arrow::flight::FlightClient::Connect(loc, *clientOpts_));

  CallOptionsAddHeaders callOptsAddHeaders{};

  std::shared_ptr<DenodoAuthenticator> denodoAuthenticator =
    std::dynamic_pointer_cast<DenodoAuthenticator>(authenticator_);

  denodoAuthenticator->setQueryContext(connectorQueryCtx_);

  authenticator_->authenticateClient(
      client, connectorQueryCtx_->sessionProperties(), callOptsAddHeaders);

  auto readerResult = client->DoGet(callOptsAddHeaders, flightEndpoint.ticket);
  AFC_ASSIGN_OR_RAISE(currentReader_, readerResult);
}
}
