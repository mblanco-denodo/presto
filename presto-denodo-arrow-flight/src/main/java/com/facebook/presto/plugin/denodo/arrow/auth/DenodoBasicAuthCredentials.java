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

public class DenodoBasicAuthCredentials
        implements DenodoAuthenticator
{
    private final Logger logger = Logger.get(DenodoBasicAuthCredentials.class);
    private final String username;
    private final String password;

    public DenodoBasicAuthCredentials(String username, String password)
    {
        this.username = username;
        this.password = password;
    }

    @Override
    public void populateAuthenticationCallHeaders(CallHeaders callHeaders)
    {
        String credentials = username + ':' + password;
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        callHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BASIC_PREFIX + encodedCredentials);
        logger.debug("Added Basic Auth header: {}", callHeaders);
    }
}
