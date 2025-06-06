#include "presto_cpp/main/connectors/denodo_arrow/DenodoArrowFlightConnector.h"
#include <arrow/flight/api.h>

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

  return clientOpts;
}
} // namespace facebook::presto