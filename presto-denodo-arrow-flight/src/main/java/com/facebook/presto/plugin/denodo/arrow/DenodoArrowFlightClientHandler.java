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
import com.facebook.presto.plugin.denodo.arrow.exception.DenodoArrowFlightRuntimeException;
import com.facebook.presto.plugin.denodo.arrow.vdp.VdpSqlBuilder;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

// TODO REFACTOR DE TODO PARA QUE NON HAXA CÓDIGO REPETIDO E TAL
// TODO intentar paralelizar o acceso aos endpoints con un concurrent map e un multithreaded stream
public class DenodoArrowFlightClientHandler
        extends BaseArrowFlightClientHandler
{
    private static final Logger log = LoggerFactory.getLogger(DenodoArrowFlightClientHandler.class);
    private final DenodoArrowFlightConfig config;

    @Inject
    public DenodoArrowFlightClientHandler(BufferAllocator allocator,
                                          DenodoArrowFlightConfig config)
    {
        super(allocator, config);
        this.config = config;
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

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        try (FlightClient client = createFlightClient()) {
            CallOption[] callOptions = getCallOptions(session);
            List<String> schemas = new ArrayList<>(0);
            FlightSql.CommandGetDbSchemas commandGetDbSchemas = FlightSql.CommandGetDbSchemas.getDefaultInstance();
            FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(commandGetDbSchemas).toByteArray());
            FlightInfo info = client.getInfo(descriptor, callOptions);
            info.getEndpoints().forEach(endpoint -> {
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
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (!schemaName.isPresent()) {
            throw new DenodoArrowFlightRuntimeException("Cannot list tables for empty schema");
        }
        try (FlightClient client = createFlightClient()) {
            CallOption[] callOptions = getCallOptions(session);
            List<SchemaTableName> tables = new ArrayList<>(0);
            FlightSql.CommandGetTables commandGetTables = FlightSql.CommandGetTables
                    .newBuilder()
                    .setDbSchemaFilterPattern(schemaName.get())
                    .setIncludeSchema(true)
                    .build();

            FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(commandGetTables).toByteArray());
            FlightInfo info = client.getInfo(descriptor, callOptions);
            info.getEndpoints().forEach(endpoint -> {
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
    protected FlightDescriptor getFlightDescriptorForSchema(String schemaName, String tableName)
    {
        FlightSql.CommandStatementQuery getTable = FlightSql.CommandStatementQuery
                .newBuilder()
                .setQuery("select * from \"" + schemaName + "\".\"" + tableName + "\" where 1=0")
                .build();
        return FlightDescriptor.command(Any.pack(getTable).toByteArray());
    }

    public static final ActionType FLIGHT_SQL_CREATE_PREPARED_STATEMENT = new ActionType("CreatePreparedStatement",
            "Creates a reusable prepared statement resource on the server. \n" +
                    "Request Message: ActionCreatePreparedStatementRequest\n" +
                    "Response Message: ActionCreatePreparedStatementResult");

    @Override
    public FlightInfo getFlightInfoForTableScan(ArrowTableLayoutHandle tableLayoutHandle, ConnectorSession session)
    {
        ArrowTableHandle tableHandle = tableLayoutHandle.getTable();
        String query = new VdpSqlBuilder().buildSql(
                tableHandle.getSchema(),
                tableHandle.getTable(),
                tableLayoutHandle.getColumnHandles(), ImmutableMap.of(),
                tableLayoutHandle.getTupleDomain());
        query = query + String.format(" CONTEXT ('i18n'='%s')", this.config.getConnectionI18n());
        Action action = new Action(FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(),
                Any.pack(FlightSql.ActionCreatePreparedStatementRequest
                        .newBuilder()
                        .setQuery(query)
                        .build())
                        .toByteArray());
        try (FlightClient client = createFlightClient()) {
            Iterator<Result> preparedStatementResults = client.doAction(action, getCallOptions(session));
            FlightSql.ActionCreatePreparedStatementResult preparedStatementResult =
                    Any.parseFrom(preparedStatementResults.next().getBody())
                            .unpack(FlightSql.ActionCreatePreparedStatementResult.class);
            FlightDescriptor descriptor = FlightDescriptor
                    .command(Any.pack(FlightSql.CommandPreparedStatementQuery.newBuilder()
                            .setPreparedStatementHandle(preparedStatementResult.getPreparedStatementHandle())
                            .build())
                            .toByteArray());
            return getFlightInfo(descriptor, session);
        }
        catch (Exception e) {
            throw new DenodoArrowFlightRuntimeException("Cannot create PreparedStatement", e);
        }
    }

    @Override
    protected FlightDescriptor getFlightDescriptorForTableScan(ArrowTableLayoutHandle tableLayoutHandle)
    {
        ArrowTableHandle tableHandle = tableLayoutHandle.getTable();
        String query = new VdpSqlBuilder().buildSql(
                tableHandle.getSchema(),
                tableHandle.getTable(),
                tableLayoutHandle.getColumnHandles(), ImmutableMap.of(),
                tableLayoutHandle.getTupleDomain());
        FlightSql.CommandStatementQuery request = FlightSql.CommandStatementQuery
                .newBuilder()
                .setQuery(query)
                .setTransactionId(ByteString.copyFrom(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)))
                .build();
        log.error("executing query: {}", query);
        return FlightDescriptor.command(Any.pack(request).toByteArray());
    }
}
