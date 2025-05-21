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
package com.facebook.presto.plugin.denodo.arrow.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

public class DenodoArrowFlightRequest
{
    private final Optional<String> schema;
    private final Optional<String> table;
    private final Optional<String> query;

    @JsonCreator
    public DenodoArrowFlightRequest(
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("table") Optional<String> table,
            @JsonProperty("query") Optional<String> query)
    {
        this.schema = schema;
        this.table = table;
        this.query = query;
    }

    public static DenodoArrowFlightRequest createListVdpDatabasesRequest()
    {
        return new DenodoArrowFlightRequest(
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public static DenodoArrowFlightRequest createListVdpTablesRequest(String vdpDatabase)
    {
        return new DenodoArrowFlightRequest(
                Optional.of(vdpDatabase),
                Optional.empty(),
                Optional.empty());
    }

    public static DenodoArrowFlightRequest createListVdpTableRequest(String vdpDatabase, String vdpTable)
    {
        return new DenodoArrowFlightRequest(
                Optional.of(vdpDatabase),
                Optional.of(vdpTable),
                Optional.empty());
    }

    public static DenodoArrowFlightRequest createQueryRequest(String vdpDatabase, String vdpTable, String query)
    {
        return new DenodoArrowFlightRequest(
                Optional.of(vdpDatabase),
                Optional.of(vdpTable),
                Optional.of(query));
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return this.schema;
    }

    @JsonProperty
    public Optional<String> getTable()
    {
        return this.table;
    }

    @JsonProperty
    public Optional<String> getQuery()
    {
        return this.query;
    }
}
