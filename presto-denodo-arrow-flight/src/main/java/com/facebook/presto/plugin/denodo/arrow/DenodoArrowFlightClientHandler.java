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

import com.facebook.plugin.arrow.ArrowTableHandle;
import com.facebook.plugin.arrow.ArrowTableLayoutHandle;
import com.facebook.plugin.arrow.BaseArrowFlightClientHandler;
import com.facebook.presto.plugin.denodo.arrow.auth.DenodoAuthenticatorFactory;
import com.facebook.presto.plugin.denodo.arrow.auth.DenodoCallOptions;
import com.facebook.presto.plugin.denodo.arrow.cache.DenodoArrowFlightMetadataCache;
import com.facebook.presto.plugin.denodo.arrow.exception.DenodoArrowFlightRuntimeException;
import com.facebook.presto.plugin.denodo.arrow.vdp.VdpSqlBuilder;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.protobuf.Any;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class DenodoArrowFlightClientHandler
        extends BaseArrowFlightClientHandler
{
    private static final Logger log = LoggerFactory.getLogger(DenodoArrowFlightClientHandler.class);
    private final DenodoArrowFlightConfig config;
    // Add cache for metadata names with expiration
    private final DenodoArrowFlightMetadataCache<String, List<String>> schemaCache;
    private final DenodoArrowFlightMetadataCache<String, List<SchemaTableName>> tableCache;

    @Inject
    public DenodoArrowFlightClientHandler(BufferAllocator allocator,
                                          DenodoArrowFlightConfig config)
    {
        super(allocator, config);
        this.config = config;
        // Initialize cache
        this.schemaCache = new DenodoArrowFlightMetadataCache<>(
                (key, connectorSession, params) -> fetchSchemaNames(connectorSession));
        this.tableCache = new DenodoArrowFlightMetadataCache<>(
                (key, connectorSession, params) -> fetchTableNames(
                        connectorSession,
                        ((Optional<String>) params[0]) /*optional schema name*/));
    }

    @Override
    public CallOption[] getCallOptions(ConnectorSession connectorSession)
    {
        DenodoCallOptions denodoCallOptions = new DenodoCallOptions(
                DenodoAuthenticatorFactory.getAuthenticator(this.config),
                this.config.getConnectionUserAgent(),
                connectorSession.getQueryId(),
                this.config.getTimePrecisionUnit(),
                this.config.getTimestampPrecisionUnit(),
                this.config.getQueryTimeout(),
                this.config.getAutocommit(),
                this.config.getWorkspace());
        return new CallOption[] {
                new CredentialCallOption(denodoCallOptions),
                CallOptions.timeout(
                        this.config.getQueryTimeout(),
                        TimeUnit.MILLISECONDS)
        };
    }

    private List<String> fetchSchemaNames(ConnectorSession session)
    {
        try (FlightClient client = createFlightClient()) {
            CallOption[] callOptions = getCallOptions(session);
            FlightSqlClient sqlClient = new FlightSqlClient(client);
            FlightInfo info = sqlClient.getSchemas(null, null, callOptions);
            List<String> schemas = Collections.synchronizedList(new ArrayList<>(0));
            info.getEndpoints().parallelStream().forEach(endpoint -> {
                try (FlightStream stream = client.getStream(endpoint.getTicket(), callOptions)) {
                    while (stream.next()) {
                        VectorSchemaRoot root = stream.getRoot();
                        if (!root.getFieldVectors().isEmpty()) {
                            VarCharVector catalogVector = (VarCharVector) root.getVector("catalog_name");
                            for (int i = 0; i < catalogVector.getValueCount(); i++) {
                                schemas.add(catalogVector.getObject(i).toString());
                            }
                        }
                    }
                }
                catch (Exception e) {
                    throw new DenodoArrowFlightRuntimeException("Cannot list VDP database names: " + e.getMessage(), e);
                }
            });
            return schemas;
        }
        catch (InterruptedException e) {
            throw new DenodoArrowFlightRuntimeException("Cannot list VDP database names: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        try {
            return this.schemaCache.get(session.getQueryId(), session);
        }
        catch (ExecutionException e) {
            log.error("Error retrieving schemas from cache", e);
            return fetchSchemaNames(session);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        try {
            return this.tableCache.get(session.getQueryId(), session, schemaName);
        }
        catch (ExecutionException e) {
            log.error("Error retrieving tables from cache", e);
            return fetchTableNames(session, schemaName);
        }
    }

    public List<SchemaTableName> fetchTableNames(ConnectorSession session, Optional<String> schemaName)
    {
        if (!schemaName.isPresent()) {
            throw new DenodoArrowFlightRuntimeException("Cannot list tables for empty schema");
        }
        try (FlightClient client = createFlightClient()) {
            CallOption[] callOptions = getCallOptions(session);
            FlightSqlClient sqlClient = new FlightSqlClient(client);
            FlightInfo info = sqlClient.getTables(
                    null,
                    schemaName.get(),
                    null,
                    null,
                    false,
                    callOptions);
            List<SchemaTableName> tables = Collections.synchronizedList(new ArrayList<>(0));
            info.getEndpoints().parallelStream().forEach(endpoint -> {
                try (FlightStream stream = client.getStream(endpoint.getTicket(), callOptions)) {
                    while (stream.next()) {
                        VectorSchemaRoot root = stream.getRoot();
                        if (!root.getFieldVectors().isEmpty()) {
                            VarCharVector catalogVector = (VarCharVector) root.getVector("table_name");
                            for (int i = 0; i < catalogVector.getValueCount(); i++) {
                                SchemaTableName stn = new SchemaTableName(schemaName.get(),
                                        catalogVector.getObject(i).toString());
                                tables.add(stn);
                            }
                        }
                    }
                }
                catch (Exception e) {
                    throw new DenodoArrowFlightRuntimeException("Cannot list table names: " + e.getMessage(), e);
                }
            });
            return tables;
        }
        catch (InterruptedException e) {
            throw new DenodoArrowFlightRuntimeException("Cannot list table names: " + e.getMessage(), e);
        }
    }

    @Override
    protected FlightDescriptor getFlightDescriptorForSchema(ConnectorSession session,
                                                            String schemaName, String tableName)
    {
        FlightSql.CommandStatementQuery getTable = FlightSql.CommandStatementQuery
                .newBuilder()
                .setQuery("select * from \"" + schemaName + "\".\"" + tableName + "\" where 1=0")
                .build();
        return FlightDescriptor.command(Any.pack(getTable).toByteArray());
    }

    @Override
    protected FlightDescriptor getFlightDescriptorForTableScan(ConnectorSession connectorSession,
                                                               ArrowTableLayoutHandle tableLayoutHandle)
    {
        ArrowTableHandle tableHandle = tableLayoutHandle.getTable();
        String query = new VdpSqlBuilder().buildSql(
                tableHandle.getSchema(),
                tableHandle.getTable(),
                tableLayoutHandle.getColumnHandles(), ImmutableMap.of(),
                tableLayoutHandle.getTupleDomain());
        query = query + String.format(" CONTEXT ('i18n'='%s')", this.config.getConnectionI18n());
        CallOption[] callOptions = getCallOptions(connectorSession);
        Action action = new Action(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(),
                Any.pack(FlightSql.ActionCreatePreparedStatementRequest
                                .newBuilder()
                                .setQuery(query)
                                .build())
                        .toByteArray());
        try (FlightClient client = createFlightClient()) {
            Iterator<Result> preparedStatementResults = client.doAction(action, callOptions);
            FlightSql.ActionCreatePreparedStatementResult preparedStatementResult =
                    Any.parseFrom(preparedStatementResults.next().getBody())
                            .unpack(FlightSql.ActionCreatePreparedStatementResult.class);
            return FlightDescriptor
                    .command(Any.pack(FlightSql.CommandPreparedStatementQuery.newBuilder()
                                    .setPreparedStatementHandle(preparedStatementResult.getPreparedStatementHandle())
                                    .build())
                            .toByteArray());
        }
        catch (Exception e) {
            throw new DenodoArrowFlightRuntimeException("Cannot create PreparedStatement", e);
        }
    }
}
