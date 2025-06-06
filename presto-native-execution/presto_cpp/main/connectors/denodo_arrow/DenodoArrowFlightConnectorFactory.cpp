#include "presto_cpp/main/connectors/denodo_arrow/DenodoArrowFlightConnectorFactory.h"

namespace facebook::presto {

std::string DenodoArrowFlightConnectorFactory::keyConnectorName() {
  return kConnectorName;
}

} // namespace facebook::presto