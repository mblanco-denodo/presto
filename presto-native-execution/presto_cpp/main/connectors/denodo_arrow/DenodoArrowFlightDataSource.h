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

namespace arrow {
class RecordBatch;
namespace flight {
class FlightClientOptions;
class Location;
} // namespace flight
} // namespace arrow

namespace facebook::presto {

struct DenodoArrowSplitData {
  /// @param vqlQuery VQL query to send to VDP through arrow flight
  DenodoArrowSplitData(
    const std::string& vqlQuery,
    const std::string& identity) :
  vqlQuery_(vqlQuery), identity_(identity){}
  const std::string vqlQuery_;
  const std::string identity_;
};

struct DenodoArrowSplit : public ArrowFlightSplit {
  /// @param connectorId
  /// @param flightEndpointBytes Base64 Serialized `FlightEndpoint`
  /// @param executionIdentifier String that identifies the query execution
  DenodoArrowSplit(
      const std::string& connectorId,
      const std::string& flightEndpointBytes,
      const DenodoArrowSplitData& denodoArrowSplitData)
      : ArrowFlightSplit(connectorId, flightEndpointBytes),
        denodoArrowSplitData_(denodoArrowSplitData){}

  const DenodoArrowSplitData denodoArrowSplitData_;
};

class DenodoArrowFlightDataSource : public ArrowFlightDataSource {
public:
  DenodoArrowFlightDataSource(
      const velox::RowTypePtr& outputType,
      const velox::connector::ColumnHandleMap& columnHandles,
      std::shared_ptr<Authenticator> authenticator,
      const velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<ArrowFlightConfig>& flightConfig,
      const std::shared_ptr<arrow::flight::FlightClientOptions>& clientOpts);
  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override;

  std::optional<velox::RowVectorPtr> next(
      uint64_t size,
      velox::ContinueFuture& /* unused */) override;

  void addDynamicFilter(
      velox::column_index_t outputChannel,
      const std::shared_ptr<velox::common::Filter>& filter) override {
    VELOX_UNSUPPORTED("Arrow Flight connector doesn't support dynamic filters");
  }

  uint64_t getCompletedBytes() override {
    return completedBytes_;
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, velox::RuntimeCounter> runtimeStats()
      override {
    return {};
  }

private:
  velox::RowVectorPtr projectOutputColumns(
      const std::shared_ptr<arrow::RecordBatch>& input);

  velox::RowTypePtr outputType_;
  std::vector<std::string> columnMapping_;
  std::unique_ptr<arrow::flight::FlightStreamReader> currentReader_;
  uint64_t completedRows_ = 0;
  uint64_t completedBytes_ = 0;
  std::shared_ptr<Authenticator> authenticator_;
  const velox::connector::ConnectorQueryCtx* connectorQueryCtx_;
  const std::shared_ptr<ArrowFlightConfig> flightConfig_;
  const std::shared_ptr<arrow::flight::FlightClientOptions> clientOpts_;
};

} // namespace facebook::presto

