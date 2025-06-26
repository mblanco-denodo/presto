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

#include <arrow/flight/client.h>
#include <arrow/flight/middleware.h>

namespace facebook::presto {
// Wrapper for CallOptions which does not add any member variables,
// but provides a write-only interface for adding call headers.
class CallOptionsAddHeaders : public arrow::flight::FlightCallOptions,
                              public arrow::flight::AddCallHeaders {
public:
  void AddHeader(const std::string& key, const std::string& value) override;
};
}