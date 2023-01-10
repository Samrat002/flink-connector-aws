/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.glue.operator;

import org.apache.flink.connector.aws.config.AWSConfig;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants;
import org.apache.flink.table.catalog.glue.util.GlueUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utilities for Glue Table related operations. */
public class GlueTableOperator extends GlueOperator {

    private static final Logger LOG = LoggerFactory.getLogger(GlueTableOperator.class);

    public GlueTableOperator(
            String catalogName, String catalogPath, AWSConfig awsConfig, GlueClient glueClient) {
        super(catalogName, catalogPath, awsConfig, glueClient);
    }

    /**
     * Create table in glue data catalog.
     *
     * @param tablePath Fully qualified name of table. {@link ObjectPath}
     * @param table instance of {@link CatalogBaseTable} containing table related information.
     * @throws CatalogException Any Exception thrown due to glue error
     */
    public void createGlueTable(final ObjectPath tablePath, final CatalogBaseTable table)
            throws CatalogException {

        checkNotNull(table, "Table cannot be Null");
        checkNotNull(tablePath, "TablePath cannot be Null");

        final Map<String, String> tableProperties = new HashMap<>(table.getOptions());
        String tableOwner = GlueUtils.extractTableOwner(tableProperties);
        Set<Column> glueTableColumns = GlueUtils.getGlueColumnsFromCatalogTable(table);

        StorageDescriptor.Builder storageDescriptorBuilder =
                StorageDescriptor.builder()
                        .inputFormat(GlueUtils.extractInputFormat(tableProperties))
                        .outputFormat(GlueUtils.extractOutputFormat(tableProperties));

        TableInput.Builder tableInputBuilder =
                TableInput.builder()
                        .name(tablePath.getObjectName())
                        .description(table.getComment())
                        .tableType(table.getTableKind().name())
                        .lastAccessTime(Instant.now())
                        .owner(tableOwner)
                        .viewExpandedText(GlueUtils.getExpandedQuery(table))
                        .viewOriginalText(GlueUtils.getOriginalQuery(table));

        CreateTableRequest.Builder requestBuilder =
                CreateTableRequest.builder()
                        .catalogId(getGlueCatalogId())
                        .databaseName(tablePath.getDatabaseName());

        if (table instanceof CatalogTable) {
            CatalogTable catalogTable = (CatalogTable) table;
            if (catalogTable.isPartitioned()) {
                LOG.info("Catalog table is partitioned");
                Collection<Column> partitionKeys =
                        GlueUtils.getPartitionKeys(catalogTable, glueTableColumns);
                LOG.info(
                        "Partition columns are -> "
                                + partitionKeys.stream()
                                        .map(Column::name)
                                        .collect(Collectors.joining(",")));
                tableInputBuilder.partitionKeys(partitionKeys);
            }
        }

        try {
            storageDescriptorBuilder.columns(glueTableColumns);
            tableInputBuilder.storageDescriptor(storageDescriptorBuilder.build());
            tableInputBuilder.parameters(tableProperties);
            requestBuilder.tableInput(tableInputBuilder.build());

            CreateTableResponse response = glueClient.createTable(requestBuilder.build());
            GlueUtils.validateGlueResponse(response);
            if (LOG.isDebugEnabled()) {
                LOG.debug(GlueUtils.getDebugLog(response));
            }

            LOG.info(String.format("Table created. %s", tablePath.getFullName()));
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Update Table in glue data catalog.
     *
     * @param tablePath fully Qualified Table Path.
     * @param newTable instance of {@link CatalogBaseTable} containing information for table.
     * @throws CatalogException Glue related exception.
     */
    public void alterGlueTable(ObjectPath tablePath, CatalogBaseTable newTable)
            throws CatalogException {

        Map<String, String> tableProperties = new HashMap<>(newTable.getOptions());
        String tableOwner = GlueUtils.extractTableOwner(tableProperties);
        Set<Column> glueColumns = GlueUtils.getGlueColumnsFromCatalogTable(newTable);

        StorageDescriptor.Builder storageDescriptorBuilder =
                StorageDescriptor.builder()
                        .inputFormat(GlueUtils.extractInputFormat(tableProperties))
                        .outputFormat(GlueUtils.extractOutputFormat(tableProperties))
                        .parameters(tableProperties)
                        .columns(glueColumns);

        TableInput.Builder tableInputBuilder =
                TableInput.builder()
                        .name(tablePath.getObjectName())
                        .description(newTable.getComment())
                        .tableType(newTable.getTableKind().name())
                        .lastAccessTime(Instant.now())
                        .owner(tableOwner);

        UpdateTableRequest.Builder requestBuilder =
                UpdateTableRequest.builder()
                        .tableInput(tableInputBuilder.build())
                        .catalogId(getGlueCatalogId())
                        .databaseName(tablePath.getDatabaseName());

        if (newTable instanceof CatalogTable) {
            CatalogTable catalogTable = (CatalogTable) newTable;
            if (catalogTable.isPartitioned()) {
                tableInputBuilder.partitionKeys(
                        GlueUtils.getPartitionKeys(catalogTable, glueColumns));
            }
        }

        tableInputBuilder.storageDescriptor(storageDescriptorBuilder.build());
        requestBuilder.tableInput(tableInputBuilder.build());

        try {
            UpdateTableResponse response = glueClient.updateTable(requestBuilder.build());
            LOG.debug(GlueUtils.getDebugLog(response));
            GlueUtils.validateGlueResponse(response);
            LOG.info(String.format("Table updated. %s", tablePath.getFullName()));
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Get names of all tables or views under this database based on type identifier. An empty list
     * is returned if none exists.
     *
     * @param databaseName fully qualified database name.
     * @return a list of the names of all tables or views in this database based on type identifier.
     * @throws CatalogException in case of any runtime exception
     */
    public List<String> getGlueTableList(String databaseName, String type) throws CatalogException {
        GetTablesRequest.Builder tablesRequestBuilder =
                GetTablesRequest.builder().databaseName(databaseName).catalogId(getGlueCatalogId());
        GetTablesResponse response = glueClient.getTables(tablesRequestBuilder.build());
        GlueUtils.validateGlueResponse(response);
        List<String> finalTableList =
                response.tableList().stream()
                        .filter(table -> table.tableType().equalsIgnoreCase(type))
                        .map(Table::name)
                        .collect(Collectors.toList());
        String tableResultNextToken = response.nextToken();

        if (Optional.ofNullable(tableResultNextToken).isPresent()) {
            do {
                tablesRequestBuilder.nextToken(tableResultNextToken);
                response = glueClient.getTables(tablesRequestBuilder.build());
                GlueUtils.validateGlueResponse(response);
                finalTableList.addAll(
                        response.tableList().stream()
                                .filter(table -> table.tableType().equalsIgnoreCase(type))
                                .map(Table::name)
                                .collect(Collectors.toList()));
                tableResultNextToken = response.nextToken();
            } while (Optional.ofNullable(tableResultNextToken).isPresent());
        }
        return finalTableList;
    }

    /**
     * Returns a {@link Table} identified by the given Table Path. {@link ObjectPath}.
     *
     * @param tablePath Path of the table or view
     * @return The requested table. Glue encapsulates whether table or view in its attribute called
     *     type.
     * @throws TableNotExistException if the target does not exist
     * @throws CatalogException in case of any runtime exception
     */
    public Table getGlueTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {

        checkNotNull(tablePath, "TablePath cannot be Null");

        GetTableRequest tablesRequest =
                GetTableRequest.builder()
                        .databaseName(tablePath.getDatabaseName())
                        .name(tablePath.getObjectName())
                        .catalogId(getGlueCatalogId())
                        .build();
        try {
            GetTableResponse response = glueClient.getTable(tablesRequest);
            GlueUtils.validateGlueResponse(response);
            return response.table();
        } catch (EntityNotFoundException e) {
            throw new TableNotExistException(catalogName, tablePath, e);
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Check if a table or view exists in glue data catalog.
     *
     * @param tablePath Path of the table or view
     * @return true if the given table exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    public boolean glueTableExists(ObjectPath tablePath) throws CatalogException {
        try {
            Table glueTable = getGlueTable(tablePath);
            return glueTable != null && glueTable.name().equals(tablePath.getObjectName());
        } catch (TableNotExistException e) {
            LOG.warn(
                    String.format(
                            "%s\nDatabase: %s Table: %s",
                            GlueCatalogConstants.TABLE_NOT_EXISTS_IDENTIFIER,
                            tablePath.getDatabaseName(),
                            tablePath.getObjectName()));
            return false;
        } catch (CatalogException e) {
            throw new CatalogException(
                    GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e.getCause());
        }
    }

    /**
     * Drop a table or view from glue data catalog.
     *
     * @param tablePath fully qualified Table Path
     * @throws CatalogException on runtime errors.
     */
    public void dropGlueTable(ObjectPath tablePath) throws CatalogException {
        DeleteTableRequest.Builder tableRequestBuilder =
                DeleteTableRequest.builder()
                        .databaseName(tablePath.getDatabaseName())
                        .name(tablePath.getObjectName())
                        .catalogId(getGlueCatalogId());
        try {
            DeleteTableResponse response = glueClient.deleteTable(tableRequestBuilder.build());
            GlueUtils.validateGlueResponse(response);
            LOG.info(String.format("Dropped Table %s.", tablePath.getObjectName()));
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Create a {@link CatalogTable} using all the information from {@link Table}.
     *
     * @param glueTable Instance of Table from glue Data catalog.
     * @return {@link CatalogTable}.
     */
    public CatalogBaseTable getCatalogBaseTableFromGlueTable(Table glueTable) {

        checkNotNull(glueTable, "Glue Table cannot be null");
        Schema schemaInfo = GlueUtils.getSchemaFromGlueTable(glueTable);

        List<String> partitionKeys =
                glueTable.partitionKeys().stream().map(Column::name).collect(Collectors.toList());
        Map<String, String> properties = new HashMap<>(glueTable.parameters());

        if (glueTable.owner() != null) {
            properties.put(GlueCatalogConstants.TABLE_OWNER, glueTable.owner());
        }

        if (glueTable.storageDescriptor().hasParameters()) {
            properties.putAll(glueTable.storageDescriptor().parameters());
        }

        if (glueTable.storageDescriptor().inputFormat() != null) {
            properties.put(
                    GlueCatalogConstants.TABLE_INPUT_FORMAT,
                    glueTable.storageDescriptor().inputFormat());
        }

        if (glueTable.storageDescriptor().outputFormat() != null) {
            properties.put(
                    GlueCatalogConstants.TABLE_OUTPUT_FORMAT,
                    glueTable.storageDescriptor().outputFormat());
        }

        if (glueTable.tableType().equals(CatalogBaseTable.TableKind.TABLE.name())) {
            return CatalogTable.of(schemaInfo, glueTable.description(), partitionKeys, properties);
        } else if (glueTable.tableType().equals(CatalogBaseTable.TableKind.VIEW.name())) {
            return CatalogView.of(
                    schemaInfo,
                    glueTable.description(),
                    glueTable.viewOriginalText(),
                    glueTable.viewExpandedText(),
                    properties);

        } else {
            throw new CatalogException("Unknown TableType.");
        }
    }

    /**
     * Rename glue table. Glue catalog don't support renaming table. For renaming in Flink, it has
     * to be done in 3 step. 1. fetch existing table info from glue 2. Create a table with new-name
     * and use properties of existing table 3. Delete existing table Note: This above steps are not
     * Atomic in nature.
     *
     * <p>Associated issue :- <a href="https://issues.apache.org/jira/browse/FLINK-31926">...</a>
     *
     * @param oldTablePath old table name
     * @param newTablePath new renamed table
     */
    public void renameGlueTable(ObjectPath oldTablePath, ObjectPath newTablePath)
            throws CatalogException, TableNotExistException {

        // Glue doesn't Support renaming of table.
        // Probable steps to implement renaming:
        // 1. Get Current Glue Table
        // 2. Derive input with renamed values
        // 3. create new table
        // 4. create partitions with renamed values
        // 5. delete old partitions
        // 6. delete old table

        // todo: when statistics features are implemented in GlueCatalog
        // 1. update table statistics
        // 2. update table column statistics
        // 3. partition statistics
    }
}
