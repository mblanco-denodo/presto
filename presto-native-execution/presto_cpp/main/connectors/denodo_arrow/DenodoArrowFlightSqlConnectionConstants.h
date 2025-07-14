#pragma once

namespace facebook::presto {
class DenodoArrowFlightSqlConnectionConstants {
public:
  static constexpr const char* authorizationKey = "authorization";
  static constexpr const char* basicAuthenticationKey = "Basic ";
  static constexpr const char* xForwardedUserAgentKey = "x-forwarded-user-agent";
  static constexpr const char* mppQueryIdKey = "mpp-query-id";
  static constexpr const char* timePrecisionKey = "timePrecision";
  static constexpr const char* timestampPrecisionKey = "timeStampPrecision";
  static constexpr const char* queryTimeoutKey = "queryTimeout";
  static constexpr const char* autocommitKey = "autocommit";
  static constexpr const char* workspaceKey = "workspaceKey";
};
} // namespace facebook::presto