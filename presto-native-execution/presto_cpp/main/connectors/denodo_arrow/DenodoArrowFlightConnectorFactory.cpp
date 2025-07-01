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
#include "presto_cpp/main/connectors/denodo_arrow/DenodoArrowFlightConnectorFactory.h"

namespace facebook::presto {
std::shared_ptr<velox::connector::Connector>
  DenodoArrowFlightConnectorFactory::newConnector(
    const std::string& id,
    std::shared_ptr<const velox::config::ConfigBase> config,
    folly::Executor* ioExecutor,
    folly::Executor* cpuExecutor) {
  return std::make_shared<DenodoArrowFlightConnector>(
      id, config, authenticatorName_);
}
} // namespace facebook::presto