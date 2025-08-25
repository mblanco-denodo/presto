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
package com.facebook.presto.plugin.denodo.arrow.auth;

import org.apache.arrow.flight.CallHeaders;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class DenodoCallOptions
        implements Consumer<CallHeaders>
{
    private final DenodoAuthenticator authenticator;
    private final String userAgent;
    private final String mppQueryId;
    private final String timePrecision;
    private final String timestampPrecision;
    private final Integer queryTimeout;
    private final Boolean autocommit;
    private final String workspace;

    public DenodoCallOptions(DenodoAuthenticator authenticator,
                             String userAgent,
                             String mppQueryId,
                             String timePrecision,
                             String timestampPrecision,
                             Integer queryTimeout,
                             Boolean autocommit,
                             String workspace)
    {
        requireNonNull(authenticator, "'DenodoAuthenticator' is null");
        requireNonNull(userAgent, "'user-agent' is null");
        this.authenticator = authenticator;
        this.userAgent = userAgent;
        this.mppQueryId = mppQueryId;
        this.timePrecision = timePrecision;
        this.timestampPrecision = timestampPrecision;
        this.queryTimeout = queryTimeout;
        this.autocommit = autocommit;
        this.workspace = workspace;
    }

    @Override
    public void accept(CallHeaders callHeaders)
    {
        authenticator.populateAuthenticationCallHeaders(callHeaders);
        callHeaders.insert(DenodoArrowFlightSqlConnectionConstants.ORIGINAL_USER_AGENT_KEY, userAgent);
        callHeaders.insert(DenodoArrowFlightSqlConnectionConstants.EMBEDDED_MPP_QUERY_ID_KEY, mppQueryId);
        if (timePrecision != null) {
            callHeaders.insert(DenodoArrowFlightSqlConnectionConstants.TIME_PRECISION_KEY, timePrecision);
        }
        if (timestampPrecision != null) {
            callHeaders.insert(DenodoArrowFlightSqlConnectionConstants.TIMESTAMP_PRECISION_KEY, timestampPrecision);
        }
        if (queryTimeout != null) {
            callHeaders.insert(DenodoArrowFlightSqlConnectionConstants.QUERY_TIMEOUT_KEY, queryTimeout.toString());
        }
        if (autocommit != null) {
            callHeaders.insert(DenodoArrowFlightSqlConnectionConstants.AUTOCOMMIT_KEY, autocommit.toString());
        }
        if (workspace != null) {
            callHeaders.insert(DenodoArrowFlightSqlConnectionConstants.WORKSPACE_KEY, workspace);
        }
    }
}
