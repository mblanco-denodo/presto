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

public class DenodoArrowFlightSqlConnectionConstants
{
    public static final String ORIGINAL_USER_AGENT_KEY = "x-forwarded-user-agent";
    public static final String EMBEDDED_MPP_QUERY_ID_KEY = "mpp-query-id";
    public static final String TIME_PRECISION_KEY = "timePrecision";
    public static final String TIMESTAMP_PRECISION_KEY = "timeStampPrecision";
    public static final String QUERY_TIMEOUT_KEY = "queryTimeout";
    public static final String AUTOCOMMIT_KEY = "autoCommit";
    public static final String WORKSPACE_KEY = "workspace";

    private DenodoArrowFlightSqlConnectionConstants()
    {
        // private constructor to hide the implicit one
    }
}
