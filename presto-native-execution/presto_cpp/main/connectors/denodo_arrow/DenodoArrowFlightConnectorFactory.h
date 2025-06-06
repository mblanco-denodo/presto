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
      folly::Executor* ioExecutor = nullptr,
      folly::Executor* cpuExecutor = nullptr) override {
    return std::make_shared<DenodoArrowFlightConnector>(
        id, config, authenticatorName_);
  }

  static std::string keyConnectorName();

private:
  static constexpr const char* kConnectorName = "denodo-arrow";
  const char* authenticatorName_{nullptr};
};
} // namespace facebook::presto