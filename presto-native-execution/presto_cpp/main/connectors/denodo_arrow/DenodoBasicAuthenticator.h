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

#include "presto_cpp/main/connectors/denodo_arrow/DenodoAuthenticator.h"

namespace facebook::presto {
class DenodoBasicAuthenticator final : public DenodoAuthenticator {
public:
  DenodoBasicAuthenticator(
     std::optional<std::string> username,
     std::optional<std::string> password,
     std::optional<std::string> userAgent,
     std::optional<std::string> timePrecision,
     std::optional<std::string> timestampPrecision,
     const uint64_t queryTimeout,
     const bool autocommit,
     std::optional<std::string> workspace);

  void populateAuthenticationCallHeaders(
      arrow::flight::AddCallHeaders& headerWriter) override;

private:
  std::string username_;
  std::string password_;
};
}
