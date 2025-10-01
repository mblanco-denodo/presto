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
package com.facebook.presto.plugin.denodo.arrow;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.plugin.arrow.ArrowFlightConfig;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

public class TestDenodoArrowFlightConnectorConfig
{
    @Test
    public void defaultPropertyMappingsTest()
    {
        DenodoArrowFlightConfig defaults = ConfigAssertions.recordDefaults(DenodoArrowFlightConfig.class);
        defaults
            .setConnectionUserName(null)
            .setConnectionPassword(null)
            .setConnectionUserAgent(null)
            .setConnectionI18n(null)
            .setConnectionAuthType(null)
            .setQueryTimeout(900000)
            .setTimePrecisionUnit(null)
            .setTimestampPrecisionUnit(null)
            .setAutocommit(false)
            .setWorkspace(null)
            .setVerifyServer(true)
            .setFlightServerName(null)
            .setFlightServerSSLCertificate(null)
            .setArrowFlightServerSslEnabled(false)
            .setArrowFlightPort(null);
        ConfigAssertions.assertRecordedDefaults(defaults);
    }

    @Test
    public void explicitPropertyMappingsTest()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("arrow-flight.server", "vdp-cluster")
                .put("arrow-flight.server.port", "9999")
                .put("arrow-flight.server-ssl-certificate", "/certificates")
                .put("arrow-flight.server-ssl-enabled", "true")
                .put("arrow-flight.server.verify", "false")
                .put("connection.i18n", "us-pst")
                .put("connection.username", "test-username")
                .put("connection.password", "test-password")
                .put("connection.auth-type", "test-authType")
                .put("connection.user-agent", "test-user-agent")
                .put("connection.query-timeout", "100")
                .put("connection.time-precision", "milliseconds")
                .put("connection.timestamp-precision", "milliseconds")
                .put("autocommit", "true")
                .put("workspace", "default")
                .build();
        DenodoArrowFlightConfig expected = new DenodoArrowFlightConfig()
                .setConnectionUserName("test-username")
                .setConnectionPassword("test-password")
                .setConnectionAuthType("test-authType")
                .setConnectionUserAgent("test-user-agent")
                .setConnectionI18n("us-pst")
                .setQueryTimeout(100)
                .setTimePrecisionUnit("milliseconds")
                .setTimestampPrecisionUnit("milliseconds")
                .setAutocommit(true)
                .setWorkspace("default");
        ((ArrowFlightConfig) expected)
                .setVerifyServer(false)
                .setFlightServerName("vdp-cluster")
                .setFlightServerSSLCertificate("/certificates")
                .setArrowFlightServerSslEnabled(true)
                .setArrowFlightPort(9999);
        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Invalid time precision unit: 'invalid-time-unit'")
    public void invalidTimePrecisionTest()
    {
        DenodoArrowFlightConfig config = new DenodoArrowFlightConfig();
        config.setTimePrecisionUnit("invalid-time-unit");
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Invalid timestamp precision unit: 'invalid-timestamp-unit'")
    public void invalidTimeStampPrecisionTest()
    {
        DenodoArrowFlightConfig config = new DenodoArrowFlightConfig();
        config.setTimestampPrecisionUnit("invalid-timestamp-unit");
    }
}
