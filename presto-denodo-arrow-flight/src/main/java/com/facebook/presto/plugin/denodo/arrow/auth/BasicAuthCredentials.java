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

import com.facebook.airlift.log.Logger;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.auth2.Auth2Constants;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Consumer;

public class BasicAuthCredentials
        implements Consumer<CallHeaders>
{
    public static final String ORIGINAL_USER_AGENT_KEY = "x-forwarded-user-agent";
    public static final String EMBEDDED_MPP_QUERY_ID = "mpp-query-id";
    private final Logger logger = Logger.get(BasicAuthCredentials.class);
    private final String username;
    private final String password;
    private final String userAgent;
    private final String queryId;

    public BasicAuthCredentials(String username, String password, String queryId)
    {
        this.username = username;
        this.password = password;
        this.userAgent = "Denodo-Embedded-MPP";
        this.queryId = queryId;
    }

    @Override
    public void accept(CallHeaders callHeaders)
    {
        String credentials = username + ':' + password;
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        callHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BASIC_PREFIX + encodedCredentials);
        //callHeaders.insert(USER_AGENT_KEY, userAgent);
        callHeaders.insert(ORIGINAL_USER_AGENT_KEY, userAgent);
        callHeaders.insert(EMBEDDED_MPP_QUERY_ID, queryId);
        logger.debug("Added Basic Auth header.");
    }
}
