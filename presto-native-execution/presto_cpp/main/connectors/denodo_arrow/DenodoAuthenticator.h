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
#include <arrow/flight/client.h>

namespace facebook::presto {
class DenodoAuthenticator : public Authenticator {
public:
  DenodoAuthenticator(
    std::optional<std::string> userAgent,
    std::optional<std::string> timePrecision,
    std::optional<std::string> timestampPrecision,
    const uint64_t queryTimeout,
    const bool autocommit,
    std::optional<std::string> workspace);

  virtual ~DenodoAuthenticator() = default;

  [[nodiscard]] virtual const velox::connector::ConnectorQueryCtx*
    getQueryContext() const {
    return kConnectorQueryCtx;
  }

  virtual void setQueryContext(
    const velox::connector::ConnectorQueryCtx* context) {
    kConnectorQueryCtx = context;
  }

  virtual void setExecutionIdentifier(const std::string executionIdentifier) {
    executionIdentifier_ = executionIdentifier;
  }

  virtual void setQueryId(const std::string queryId) {
    queryId_ = queryId;
  }

  virtual void populateAuthenticationCallHeaders(
    arrow::flight::AddCallHeaders& headerWriter) = 0;

  void authenticateClient(
      std::unique_ptr<arrow::flight::FlightClient>& client,
      const velox::config::ConfigBase* sessionProperties,
      arrow::flight::AddCallHeaders& headerWriter) final;

private:
  std::string userAgent_;
  std::string timePrecision_;
  std::string timestampPrecision_;
  uint64_t queryTimeout_;
  bool autocommit_;
  std::string workspace_;
  const velox::connector::ConnectorQueryCtx* kConnectorQueryCtx{nullptr};
  thread_local static std::string queryId_;
  thread_local static std::string executionIdentifier_;
};
}