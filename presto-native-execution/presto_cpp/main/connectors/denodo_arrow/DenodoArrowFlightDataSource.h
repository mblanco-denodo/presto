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
#include "velox/connectors/Connector.h"

namespace arrow {
class RecordBatch;
namespace flight {
class FlightClientOptions;
class FlightStreamReader;
class Location;
} // namespace flight
} // namespace arrow

namespace facebook::presto {
class DenodoArrowFlightDataSource : public ArrowFlightDataSource {
public:
  DenodoArrowFlightDataSource(
      const velox::RowTypePtr& outputType,
      const std::unordered_map<
          std::string,
          std::shared_ptr<velox::connector::ColumnHandle>>& columnHandles,
      std::shared_ptr<Authenticator> authenticator,
      const velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      const std::shared_ptr<ArrowFlightConfig>& flightConfig,
      const std::shared_ptr<arrow::flight::FlightClientOptions>& clientOpts);
  void addSplit(
      std::shared_ptr<velox::connector::ConnectorSplit> split) override;
private:
  /// Convert an Arrow record batch to Velox RowVector.
  /// Process only those columns that are present in outputType_.
  velox::RowVectorPtr projectOutputColumns(
      const std::shared_ptr<arrow::RecordBatch>& input);

  velox::RowTypePtr outputType_;
  std::vector<std::string> columnMapping_;
  std::unique_ptr<arrow::flight::FlightStreamReader> currentReader_;
  uint64_t completedRows_ = 0;
  uint64_t completedBytes_ = 0;
  std::shared_ptr<Authenticator> authenticator_;
  const velox::connector::ConnectorQueryCtx* const connectorQueryCtx_;
  const std::shared_ptr<ArrowFlightConfig> flightConfig_;
  const std::shared_ptr<arrow::flight::FlightClientOptions> clientOpts_;
  const std::shared_ptr<arrow::flight::Location> defaultLocation_;
};
} // namespace facebook::presto

