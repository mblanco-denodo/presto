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
#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/compute/expression.h>
#include <arrow/flight/client.h>
#include <arrow/flight/client_middleware.h>
#include <arrow/flight/middleware.h>
#include <arrow/flight/sql/client.h>
#include <thrift/lib/cpp2/op/Create.h>
#include "arrow/result.h"

#include <utility>

#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightConnector.h"
#include "presto_cpp/main/connectors/arrow_flight/Macros.h"
#include "presto_cpp/main/connectors/arrow_flight/auth/Authenticator.h"
#include "presto_cpp/main/connectors/denodo_arrow/DenodoArrowFlightConfig.h"
#include "presto_cpp/main/connectors/denodo_arrow/DenodoAuthenticator.h"
#include "velox/vector/arrow/Abi.h"
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
      connectorQueryCtx, flightConfig, clientOpts),
      outputType_{outputType},
      flightConfig_{flightConfig},
      clientOpts_{clientOpts} {
  authenticator_ = std::move(authenticator);
  connectorQueryCtx_ = connectorQueryCtx;
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
        std::dynamic_pointer_cast<const ArrowFlightColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(
        handle,
        "handle for column '{}' is not an ArrowFlightColumnHandle",
        columnName);

    columnMapping_.push_back(handle->name());
  }
}

std::shared_ptr<arrow::flight::Location> getDefaultLocation(
    const std::shared_ptr<DenodoArrowFlightConfig>& config) {
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

// Wrapper for CallOptions which does not add any member variables,
// but provides a write-only interface for adding call headers.
class CallOptionsAddHeaders : public arrow::flight::FlightCallOptions,
                              public arrow::flight::AddCallHeaders {
public:
  void AddHeader(const std::string& key, const std::string& value) override {
    headers.emplace_back(key, value);
  }
};

// Constants defined in the Flight SQL specification
const std::string kActionCreatePreparedStatement = "CreatePreparedStatement";

/**
 * Closes FlightSqlConnectionResources and logs if any error occurs
 */
void closeFlightSqlResources(
  std::shared_ptr<arrow::flight::sql::PreparedStatement> preparedStatement,
  std::unique_ptr<arrow::flight::sql::FlightSqlClient> flightSqlClient,
  std::unique_ptr<arrow::flight::FlightClient> flightClient) {
  if (preparedStatement != nullptr) {
    const arrow::Status preparedStatementCloseStatus = preparedStatement->Close();
    if (!preparedStatementCloseStatus.ok()) {
      LOG(ERROR) << "Could not close prepared statement: " <<
        preparedStatementCloseStatus.message();
    }
  }
  if (flightSqlClient != nullptr) {
    const arrow::Status flightSqlClientCloseStatus = flightSqlClient->Close();
    if (!flightSqlClientCloseStatus.ok()) {
      LOG(ERROR) << "Could not close prepared statement: " <<
        flightSqlClientCloseStatus.message();
    }
  }
  if (flightClient != nullptr) {
    const arrow::Status flightClientCloseStatus = flightClient->Close();
    if (!flightClientCloseStatus.ok()) {
      LOG(ERROR) << "Could not close prepared statement: " <<
        flightClientCloseStatus.message();
    }
  }
}

void DenodoArrowFlightDataSource::addSplit(
  std::shared_ptr<velox::connector::ConnectorSplit> split) {
  std::shared_ptr<DenodoAuthenticator> denodoAuthenticator =
    std::dynamic_pointer_cast<DenodoAuthenticator>(authenticator_);
  denodoAuthenticator->setQueryContext(connectorQueryCtx_);
  std::shared_ptr<DenodoArrowSplit> denodoArrowFlightSplit =
    std::dynamic_pointer_cast<DenodoArrowSplit>(split);
  LOG(INFO) <<
    "addSplit: adding DenodoArrowFlightSplit with executionIdentifier: " <<
      denodoArrowFlightSplit->denodoArrowSplitData_.identity_;
  denodoAuthenticator->setQueryId(connectorQueryCtx_->queryId());
  denodoAuthenticator->setExecutionIdentifier(
    denodoArrowFlightSplit->denodoArrowSplitData_.identity_);
  std::shared_ptr<DenodoArrowFlightConfig> config =
    std::static_pointer_cast<DenodoArrowFlightConfig>(flightConfig_);
  std::shared_ptr<arrow::flight::Location> location =
    getDefaultLocation(config);

  AFC_ASSIGN_OR_RAISE(
    std::unique_ptr<arrow::flight::FlightClient> flightClient,
    arrow::flight::FlightClient::Connect(*location, *clientOpts_))

  CallOptionsAddHeaders callOptsAddHeaders{};
  authenticator_->authenticateClient(
      flightClient, connectorQueryCtx_->sessionProperties(),
      callOptsAddHeaders);


  std::unique_ptr<arrow::flight::sql::FlightSqlClient> sqlClient(
    new arrow::flight::sql::FlightSqlClient(std::move(flightClient)));

  std::string vqlQuery =
    denodoArrowFlightSplit->denodoArrowSplitData_.vqlQuery_;
  AFC_ASSIGN_OR_RAISE(
    std::shared_ptr<arrow::flight::sql::PreparedStatement> preparedStatement,
    sqlClient->Prepare(callOptsAddHeaders, vqlQuery,
      arrow::flight::sql::no_transaction()));

  AFC_ASSIGN_OR_RAISE(
    arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> flightInfoResult,
    preparedStatement->Execute(callOptsAddHeaders)
    )
  if (!flightInfoResult.ok()) {
    std::string error = "Error executing prepared statement: " +
      flightInfoResult.status().message();
    LOG(ERROR) << error;
    // there has been an error executing the prepared statement against VDP
    closeFlightSqlResources(preparedStatement, std::move(sqlClient),
      std::move(flightClient));
    return;
  }
  const std::unique_ptr<arrow::flight::FlightInfo> flightInfo =
    std::move(flightInfoResult.ValueOrDie());
  if (flightInfo->endpoints().size() > 1) {
    std::string error = "Error: Multiple endpoints for split: " +
      std::to_string(flightInfo->endpoints().size());
    LOG(ERROR) << error;
    closeFlightSqlResources(preparedStatement, std::move(sqlClient),
      std::move(flightClient));
    return;
  }
  // VDP only gives us an endpoint as it does not provide parallel access
  const arrow::flight::FlightEndpoint& endpoint = flightInfo->endpoints()[0];
  AFC_ASSIGN_OR_RAISE(
    std::unique_ptr<arrow::flight::FlightStreamReader> stream,
    sqlClient->DoGet(callOptsAddHeaders, endpoint.ticket))
  currentReader_ = std::move(stream);
  //closeFlightSqlResources(preparedStatement, std::move(sqlClient),
  //  std::move(flightClient));
}

std::optional<velox::RowVectorPtr> DenodoArrowFlightDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /* unused */) {
  VELOX_CHECK_NOT_NULL(currentReader_, "Missing split, call addSplit() first");

  AFC_ASSIGN_OR_RAISE(auto chunk, currentReader_->Next());

  // Null values in the chunk indicates that the Flight stream is complete.
  if (!chunk.data) {
    currentReader_ = nullptr;
    return nullptr;
  }

  // Extract only required columns from the record batch as a velox RowVector.
  auto output = projectOutputColumns(chunk.data);

  completedRows_ += output->size();
  completedBytes_ += output->estimateFlatSize();
  return output;
}

velox::RowVectorPtr DenodoArrowFlightDataSource::projectOutputColumns(
    const std::shared_ptr<arrow::RecordBatch>& input) {
  velox::memory::MemoryPool* pool = connectorQueryCtx_->memoryPool();
  std::vector<velox::VectorPtr> children;
  children.reserve(columnMapping_.size());

  // Extract and convert desired columns in the correct order.
  for (const auto& name : columnMapping_) {
    auto column = input->GetColumnByName(name);
    VELOX_CHECK_NOT_NULL(column, "column with name '{}' not found", name);
    ArrowArray array;
    ArrowSchema schema;
    AFC_RAISE_NOT_OK(arrow::ExportArray(*column, &array, &schema));
    children.push_back(velox::importFromArrowAsOwner(schema, array, pool));
  }

  return std::make_shared<velox::RowVector>(
      pool,
      outputType_,
      velox::BufferPtr() /*nulls*/,
      input->num_rows(),
      std::move(children));
}
} // namespace facebook presto
