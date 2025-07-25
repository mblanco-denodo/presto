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
package com.facebook.presto.plugin.jdbc;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.mapping.ReadMapping;
import com.facebook.presto.plugin.jdbc.mapping.WriteMapping;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.base.Joiner;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.JdbcWarningCode.USE_OF_DEPRECATED_CONFIGURATION_PROPERTY;
import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.jdbcTypeToReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.prestoTypeToWriteMapping;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.ResultSetMetaData.columnNullable;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BaseJdbcClient
        implements JdbcClient
{
    private static final Logger log = Logger.get(BaseJdbcClient.class);

    private static final Map<Type, String> SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(BOOLEAN, "boolean")
            .put(BIGINT, "bigint")
            .put(INTEGER, "integer")
            .put(SMALLINT, "smallint")
            .put(TINYINT, "tinyint")
            .put(DOUBLE, "double precision")
            .put(REAL, "real")
            .put(VARBINARY, "varbinary")
            .put(DATE, "date")
            .put(TIME, "time")
            .put(TIME_WITH_TIME_ZONE, "time with timezone")
            .put(TIMESTAMP, "timestamp")
            .put(TIMESTAMP_WITH_TIME_ZONE, "timestamp with timezone")
            .put(UuidType.UUID, "uuid")
            .build();

    protected final String connectorId;
    protected final ConnectionFactory connectionFactory;
    protected final String identifierQuote;
    protected final boolean caseInsensitiveNameMatching;
    protected final Cache<JdbcIdentity, Map<String, String>> remoteSchemaNames;
    protected final Cache<RemoteTableNameCacheKey, Map<String, String>> remoteTableNames;
    protected final Set<String> listSchemasIgnoredSchemas;
    protected final boolean caseSensitiveNameMatchingEnabled;

    public BaseJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config, String identifierQuote, ConnectionFactory connectionFactory)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        requireNonNull(config, "config is null"); // currently unused, retained as parameter for future extensions
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");

        this.caseInsensitiveNameMatching = config.isCaseInsensitiveNameMatching();
        CacheBuilder<Object, Object> remoteNamesCacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS);
        this.remoteSchemaNames = remoteNamesCacheBuilder.build();
        this.remoteTableNames = remoteNamesCacheBuilder.build();
        this.listSchemasIgnoredSchemas = config.getlistSchemasIgnoredSchemas();
        this.caseSensitiveNameMatchingEnabled = config.isCaseSensitiveNameMatching();
    }

    @PreDestroy
    public void destroy()
            throws Exception
    {
        connectionFactory.close();
    }

    @Override
    public String getIdentifierQuote()
    {
        return identifierQuote;
    }

    @Override
    public final Set<String> getSchemaNames(ConnectorSession session, JdbcIdentity identity)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            return listSchemas(connection).stream()
                    .map(schemaName -> normalizeIdentifier(session, schemaName))
                    .collect(toImmutableSet());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (!listSchemasIgnoredSchemas.contains(schemaName.toLowerCase(ENGLISH))) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public List<SchemaTableName> getTableNames(ConnectorSession session, JdbcIdentity identity, Optional<String> schema)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            Optional<String> remoteSchema = schema.map(schemaName -> toRemoteSchemaName(session, identity, connection, schemaName));
            try (ResultSet resultSet = getTables(connection, remoteSchema, Optional.empty())) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String tableSchema = getTableSchemaName(resultSet);
                    String tableName = resultSet.getString("TABLE_NAME");
                    list.add(new SchemaTableName(normalizeIdentifier(session, tableSchema),
                            normalizeIdentifier(session, tableName)));
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String remoteSchema = toRemoteSchemaName(session, identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(session, identity, connection, remoteSchema, schemaTableName.getTableName());
            try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.of(remoteTable))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            connectorId,
                            schemaTableName,
                            resultSet.getString("TABLE_CAT"),
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                int allColumns = 0;
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    allColumns++;
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            resultSet.getString("TYPE_NAME"),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"));
                    Optional<ReadMapping> readMapping = toPrestoType(session, typeHandle);
                    // skip unsupported column types
                    if (readMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        boolean nullable = columnNullable == resultSet.getInt("NULLABLE");
                        Optional<String> comment = Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
                        columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle, readMapping.get().getType(), nullable, comment));
                    }
                }
                if (columns.isEmpty()) {
                    // A table may have no supported columns. In rare cases (e.g. PostgreSQL) a table might have no columns at all.
                    // Throw an exception if the table has no supported columns.
                    // This can occur if all columns in the table are of unsupported types, or in rare cases, if the table has no columns at all.
                    throw new TableNotFoundException(
                            tableHandle.getSchemaTableName(),
                            format("Table '%s' has no supported columns (all %s columns are not supported)", tableHandle.getSchemaTableName(), allColumns));
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        return jdbcTypeToReadMapping(typeHandle);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcIdentity identity, JdbcTableLayoutHandle layoutHandle)
    {
        JdbcTableHandle tableHandle = layoutHandle.getTable();
        JdbcSplit jdbcSplit = new JdbcSplit(
                connectorId,
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                layoutHandle.getTupleDomain(),
                layoutHandle.getAdditionalPredicate());
        return new FixedSplitSource(ImmutableList.of(jdbcSplit));
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        Connection connection = connectionFactory.openConnection(identity);
        try {
            connection.setReadOnly(true);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return new QueryBuilder(identifierQuote).buildSql(
                this,
                session,
                connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                split.getTupleDomain(),
                split.getAdditionalPredicate());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(tableMetadata, session, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return beginWriteTable(session, tableMetadata);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return beginWriteTable(session, tableMetadata);
    }

    private JdbcOutputTableHandle beginWriteTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            return createTable(tableMetadata, session, generateTemporaryTableName());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected JdbcOutputTableHandle createTable(ConnectorTableMetadata tableMetadata, ConnectorSession session, String tableName)
            throws SQLException
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        JdbcIdentity identity = JdbcIdentity.from(session);
        if (!getSchemaNames(session, identity).contains(schemaTableName.getSchemaName())) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schemaTableName.getSchemaName());
        }

        try (Connection connection = connectionFactory.openConnection(identity)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(session, identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(session, identity, connection, remoteSchema, schemaTableName.getTableName());
            if (uppercase) {
                tableName = tableName.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(getColumnString(column, columnName));
            }

            String sql = format(
                    "CREATE TABLE %s (%s)",
                    quoted(catalog, remoteSchema, tableName),
                    join(", ", columnList.build()));
            execute(connection, sql);

            return new JdbcOutputTableHandle(
                    connectorId,
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    tableName);
        }
    }

    private String getColumnString(ColumnMetadata column, String columnName)
    {
        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ")
                .append(toSqlType(column.getType()));
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    protected String generateTemporaryTableName()
    {
        return "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
    }

    //todo
    @Override
    public void commitCreateTable(ConnectorSession session, JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        renameTable(
                identity,
                handle.getCatalogName(),
                new SchemaTableName(handle.getSchemaName(), handle.getTemporaryTableName()),
                new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTable)
    {
        renameTable(identity, handle.getCatalogName(), handle.getSchemaTableName(), newTable);
    }

    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            String schemaName = oldTable.getSchemaName();
            String tableName = oldTable.getTableName();
            String newSchemaName = newTable.getSchemaName();
            String newTableName = newTable.getTableName();
            if (metadata.storesUpperCaseIdentifiers()) {
                schemaName = schemaName.toUpperCase(ENGLISH);
                tableName = tableName.toUpperCase(ENGLISH);
                newSchemaName = newSchemaName.toUpperCase(ENGLISH);
                newTableName = newTableName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME TO %s",
                    quoted(catalogName, schemaName, tableName),
                    quoted(catalogName, newSchemaName, newTableName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        String temporaryTable = quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName());
        String targetTable = quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName());
        String insertSql = format("INSERT INTO %s SELECT * FROM %s", targetTable, temporaryTable);
        String cleanupSql = "DROP TABLE " + temporaryTable;

        try (Connection connection = getConnection(session, identity, handle)) {
            execute(connection, insertSql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        try (Connection connection = getConnection(session, identity, handle)) {
            execute(connection, cleanupSql);
        }
        catch (SQLException e) {
            log.warn(e, "Failed to cleanup temporary table: %s", temporaryTable);
        }
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, ColumnMetadata column)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String schema = handle.getSchemaName();
            String table = handle.getTableName();
            String columnName = column.getName();
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                schema = schema != null ? schema.toUpperCase(ENGLISH) : null;
                table = table.toUpperCase(ENGLISH);
                columnName = columnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s ADD %s",
                    quoted(handle.getCatalogName(), schema, table),
                    getColumnString(column, columnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME COLUMN %s TO %s",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    jdbcColumn.getColumnName(),
                    newColumnName);
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "ALTER TABLE %s DROP COLUMN %s",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    column.getColumnName());
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle)
    {
        StringBuilder sql = new StringBuilder()
                .append("DROP TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle jdbcTableHandle)
    {
        StringBuilder sql = new StringBuilder()
                .append("TRUNCATE TABLE ")
                .append(quoted(jdbcTableHandle.getCatalogName(), jdbcTableHandle.getSchemaName(), jdbcTableHandle.getTableName()));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        dropTable(session, identity, new JdbcTableHandle(
                handle.getConnectorId(),
                new SchemaTableName(handle.getSchemaName(), handle.getTemporaryTableName()),
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTemporaryTableName()));
    }

    @Override
    public String buildInsertSql(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        String vars = Joiner.on(',').join(nCopies(handle.getColumnNames().size(), "?"));
        return new StringBuilder()
                .append("INSERT INTO ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(" VALUES (").append(vars).append(")")
                .toString();
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcIdentity identity, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return connectionFactory.openConnection(identity);
    }

    @Override
    public PreparedStatement getPreparedStatement(ConnectorSession session, Connection connection, String sql)
            throws SQLException
    {
        return connection.prepareStatement(sql);
    }

    @Override
    public String normalizeIdentifier(ConnectorSession session, String identifier)
    {
        return identifier.toLowerCase(ENGLISH);
    }

    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString("TABLE_SCHEM");
    }

    protected String toRemoteSchemaName(ConnectorSession session, JdbcIdentity identity, Connection connection, String schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");

        if (caseInsensitiveNameMatching) {
            session.getWarningCollector().add(new PrestoWarning(USE_OF_DEPRECATED_CONFIGURATION_PROPERTY,
                    "'case-insensitive-name-matching' is deprecated. Use of this configuration value may lead to query failures. " +
                            "Please switch to using 'case-sensitive-name-matching' for proper case sensitivity behavior."));
            try {
                Map<String, String> mapping = remoteSchemaNames.getIfPresent(identity);
                if (mapping != null && !mapping.containsKey(schemaName)) {
                    // This might be a schema that has just been created. Force reload.
                    mapping = null;
                }
                if (mapping == null) {
                    mapping = listSchemasByLowerCase(connection);
                    remoteSchemaNames.put(identity, mapping);
                }
                String remoteSchema = mapping.get(schemaName);
                if (remoteSchema != null) {
                    return remoteSchema;
                }
            }
            catch (RuntimeException e) {
                throw new PrestoException(JDBC_ERROR, "Failed to find remote schema name: " + firstNonNull(e.getMessage(), e), e);
            }
        }

        try {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                return schemaName.toUpperCase(ENGLISH);
            }
            return schemaName;
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected Map<String, String> listSchemasByLowerCase(Connection connection)
    {
        return listSchemas(connection).stream()
                .collect(toImmutableMap(schemaName -> schemaName.toLowerCase(ENGLISH), schemaName -> schemaName));
    }

    protected String toRemoteTableName(ConnectorSession session, JdbcIdentity identity, Connection connection, String remoteSchema, String tableName)
    {
        requireNonNull(remoteSchema, "remoteSchema is null");
        requireNonNull(tableName, "tableName is null");

        if (caseInsensitiveNameMatching) {
            session.getWarningCollector().add(new PrestoWarning(USE_OF_DEPRECATED_CONFIGURATION_PROPERTY,
                    "'case-insensitive-name-matching' is deprecated. Use of this configuration value may lead to query failures. " +
                            "Please switch to using 'case-sensitive-name-matching' for proper case sensitivity behavior."));
            try {
                RemoteTableNameCacheKey cacheKey = new RemoteTableNameCacheKey(identity, remoteSchema);
                Map<String, String> mapping = remoteTableNames.getIfPresent(cacheKey);
                if (mapping != null && !mapping.containsKey(tableName)) {
                    // This might be a table that has just been created. Force reload.
                    mapping = null;
                }
                if (mapping == null) {
                    mapping = listTablesByLowerCase(connection, remoteSchema);
                    remoteTableNames.put(cacheKey, mapping);
                }
                String remoteTable = mapping.get(tableName);
                if (remoteTable != null) {
                    return remoteTable;
                }
            }
            catch (RuntimeException e) {
                throw new PrestoException(JDBC_ERROR, "Failed to find remote table name: " + firstNonNull(e.getMessage(), e), e);
            }
        }

        try {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                return tableName.toUpperCase(ENGLISH);
            }
            return tableName;
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected Map<String, String> listTablesByLowerCase(Connection connection, String remoteSchema)
    {
        try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.empty())) {
            ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                map.put(tableName.toLowerCase(ENGLISH), tableName);
            }
            return map.build();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, List<JdbcColumnHandle> columnHandles, TupleDomain<ColumnHandle> tupleDomain)
    {
        return TableStatistics.empty();
    }

    protected void execute(Connection connection, String query)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            log.debug("Execute: %s", query);
            statement.execute(query);
        }
    }
    protected String toSqlType(Type type)
    {
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return "varchar";
            }
            return "varchar(" + varcharType.getLengthSafe() + ")";
        }
        if (type instanceof CharType) {
            if (((CharType) type).getLength() == CharType.MAX_LENGTH) {
                return "char";
            }
            return "char(" + ((CharType) type).getLength() + ")";
        }
        if (type instanceof DecimalType) {
            return format("decimal(%s, %s)", ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
        }

        String sqlType = SQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    public WriteMapping toWriteMapping(Type type)
    {
        Optional<WriteMapping> writeMapping = prestoTypeToWriteMapping(type);
        if (writeMapping.isPresent()) {
            return writeMapping.get();
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    protected String quoted(String name)
    {
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    protected String quoted(String catalog, String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sb.append(quoted(catalog)).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    protected static Optional<String> escapeNamePattern(Optional<String> name, Optional<String> escape)
    {
        if (!name.isPresent() || !escape.isPresent()) {
            return name;
        }
        return Optional.of(escapeNamePattern(name.get(), escape.get()));
    }

    private static String escapeNamePattern(String name, String escape)
    {
        requireNonNull(name, "name is null");
        requireNonNull(escape, "escape is null");
        checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        name = name.replace(escape, escape + escape);
        name = name.replace("_", escape + "_");
        name = name.replace("%", escape + "%");
        return name;
    }

    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(Optional.ofNullable(tableHandle.getSchemaName()), escape).orElse(null),
                escapeNamePattern(Optional.ofNullable(tableHandle.getTableName()), escape).orElse(null),
                null);
    }
}
