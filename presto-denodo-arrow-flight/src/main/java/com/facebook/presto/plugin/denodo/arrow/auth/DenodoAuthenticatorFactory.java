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

import com.facebook.presto.plugin.denodo.arrow.DenodoArrowFlightConfig;
import com.facebook.presto.plugin.denodo.arrow.exception.DenodoArrowFlightRuntimeException;

public class DenodoAuthenticatorFactory
{
    private static final String BASIC_AUTH_TYPE = "basic";
    private static final String OAUTH_AUTH_TYPE = "oauth";

    private DenodoAuthenticatorFactory()
    {
        // private constructor to hide the implicit one
    }

    public static DenodoAuthenticator getAuthenticator(DenodoArrowFlightConfig config)
    {
        switch (config.getConnectionAuthType()) {
            case BASIC_AUTH_TYPE:
                return new DenodoBasicAuthCredentials(
                        config.getConnectionUserName(),
                        config.getConnectionPassword());
            case OAUTH_AUTH_TYPE:
                throw new DenodoArrowFlightRuntimeException("OAuth authentication not implemented");
            default:
                throw new DenodoArrowFlightRuntimeException("Missing 'connection.authType' property");
        }
    }
}
