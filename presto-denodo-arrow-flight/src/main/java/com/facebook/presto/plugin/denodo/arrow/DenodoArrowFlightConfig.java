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

import com.facebook.airlift.configuration.Config;
import com.facebook.plugin.arrow.ArrowFlightConfig;

public class DenodoArrowFlightConfig
        extends ArrowFlightConfig
{
    private static final String MILLISECONDS = "milliseconds";
    private static final String MICROSECONDS = "microseconds";
    private static final String NANOSECONDS = "nanoseconds";
    private static final String INVALID_ERROR = "Invalid %s precision unit: '%s'";
    private static final String DEFAULT_I18N = "us_utc_iso";
    private String authType;
    private String username;
    private String password;
    private String userAgent;
    private String i18n;
    private Integer queryTimeout = 900000;
    private String timePrecisionUnit;
    private String timestampPrecisionUnit;
    private Boolean autocommit = false;
    private String workspace;

    public String getConnectionAuthType()
    {
        return this.authType;
    }

    @Config("connection.auth-type")
    public DenodoArrowFlightConfig setConnectionAuthType(String authType)
    {
        this.authType = authType;
        return this;
    }

    public String getConnectionUserName()
    {
        return this.username;
    }

    @Config("connection.username")
    public DenodoArrowFlightConfig setConnectionUserName(String username)
    {
        this.username = username;
        return this;
    }

    public String getConnectionPassword()
    {
        return this.password;
    }

    @Config("connection.password")
    public DenodoArrowFlightConfig setConnectionPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getConnectionUserAgent()
    {
        return this.userAgent;
    }

    @Config("connection.user-agent")
    public DenodoArrowFlightConfig setConnectionUserAgent(String userAgent)
    {
        this.userAgent = userAgent;
        return this;
    }

    public String getConnectionI18n()
    {
        if (this.i18n == null) {
            return DEFAULT_I18N;
        }
        return this.i18n;
    }

    @Config("connection.i18n")
    public DenodoArrowFlightConfig setConnectionI18n(String i18n)
    {
        this.i18n = i18n;
        return this;
    }

    public Integer getQueryTimeout()
    {
        return this.queryTimeout;
    }

    @Config("connection.query-timeout")
    public DenodoArrowFlightConfig setQueryTimeout(Integer queryTimeout)
    {
        if (queryTimeout != null) {
            if (queryTimeout < 0) {
                throw new RuntimeException("'connection.query-timeout' must be a positive integer");
            }
            this.queryTimeout = queryTimeout;
        }
        return this;
    }

    public String getTimePrecisionUnit()
    {
        return this.timePrecisionUnit;
    }

    @Config("connection.time-precision")
    public DenodoArrowFlightConfig setTimePrecisionUnit(String timePrecisionUnit)
    {
        if (validateTimePrecisionValue(timePrecisionUnit)) {
            throw new IllegalArgumentException(String.format(INVALID_ERROR, "time", timePrecisionUnit));
        }
        this.timePrecisionUnit = timePrecisionUnit;
        return this;
    }

    public String getTimestampPrecisionUnit()
    {
        return this.timestampPrecisionUnit;
    }

    @Config("connection.timestamp-precision")
    public DenodoArrowFlightConfig setTimestampPrecisionUnit(String timestampPrecisionUnit)
    {
        if (validateTimePrecisionValue(timestampPrecisionUnit)) {
            throw new IllegalArgumentException(String.format(INVALID_ERROR, "timestamp", timestampPrecisionUnit));
        }
        this.timestampPrecisionUnit = timestampPrecisionUnit;
        return this;
    }

    public Boolean getAutocommit()
    {
        return this.autocommit;
    }

    @Config("autocommit")
    public DenodoArrowFlightConfig setAutocommit(Boolean autocommit)
    {
        this.autocommit = autocommit;
        return this;
    }

    public String getWorkspace()
    {
        return this.workspace;
    }

    @Config("workspace")
    public DenodoArrowFlightConfig setWorkspace(String workspace)
    {
        this.workspace = workspace;
        return this;
    }

    private boolean validateTimePrecisionValue(String value)
    {
        return value != null &&
                !(MICROSECONDS.equals(value) || MILLISECONDS.equals(value) || NANOSECONDS.equals(value));
    }
}
