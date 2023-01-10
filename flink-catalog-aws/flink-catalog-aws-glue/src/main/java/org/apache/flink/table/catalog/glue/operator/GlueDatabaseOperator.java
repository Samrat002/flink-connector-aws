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
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants;
import org.apache.flink.table.catalog.glue.util.GlueUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableRequest;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableResponse;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utilities for Glue catalog Database related operations. */
public class GlueDatabaseOperator extends GlueOperator {

    private static final Logger LOG = LoggerFactory.getLogger(GlueDatabaseOperator.class);

    public GlueDatabaseOperator(
            String catalogName, String catalogPath, AWSConfig awsConfig, GlueClient glueClient) {
        super(catalogName, catalogPath, awsConfig, glueClient);
    }

    /**
     * List all databases present.
     *
     * @return List of fully qualified database names
     */
    public List<String> listGlueDatabases() throws CatalogException {
        try {
            GetDatabasesRequest.Builder databasesRequestBuilder =
                    GetDatabasesRequest.builder().catalogId(getGlueCatalogId());

            GetDatabasesResponse response =
                    glueClient.getDatabases(databasesRequestBuilder.build());
            List<String> databaseList =
                    response.databaseList().stream()
                            .map(Database::name)
                            .collect(Collectors.toList());
            String dbResultNextToken = response.nextToken();
            if (Optional.ofNullable(dbResultNextToken).isPresent()) {
                do {
                    databasesRequestBuilder.nextToken(dbResultNextToken);
                    response = glueClient.getDatabases(databasesRequestBuilder.build());
                    databaseList.addAll(
                            response.databaseList().stream()
                                    .map(Database::name)
                                    .collect(Collectors.toList()));
                    dbResultNextToken = response.nextToken();
                } while (Optional.ofNullable(dbResultNextToken).isPresent());
            }
            return databaseList;
        } catch (GlueException e) {
            throw new CatalogException(
                    GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e.getCause());
        }
    }

    /**
     * Create database in glue data catalog.
     *
     * @param databaseName fully qualified name of database.
     * @param database Instance of {@link CatalogDatabase}.
     * @throws CatalogException when unknown error from glue servers.
     * @throws DatabaseAlreadyExistException when database exists already in glue data catalog.
     */
    public void createGlueDatabase(String databaseName, CatalogDatabase database)
            throws CatalogException, DatabaseAlreadyExistException {

        GlueUtils.validate(databaseName);
        Map<String, String> properties = new HashMap<>(database.getProperties());
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    properties.entrySet().stream()
                            .map(e -> e.getKey() + ":" + e.getValue())
                            .collect(Collectors.joining("|")));
        }
        DatabaseInput.Builder databaseInputBuilder =
                DatabaseInput.builder()
                        .name(databaseName)
                        .description(database.getComment())
                        .parameters(properties);
        CreateDatabaseRequest.Builder requestBuilder =
                CreateDatabaseRequest.builder()
                        .databaseInput(databaseInputBuilder.build())
                        .catalogId(getGlueCatalogId());
        try {
            CreateDatabaseResponse response = glueClient.createDatabase(requestBuilder.build());
            if (LOG.isDebugEnabled()) {
                LOG.debug(GlueUtils.getDebugLog(response));
            }
            GlueUtils.validateGlueResponse(response);
        } catch (EntityNotFoundException e) {
            throw new DatabaseAlreadyExistException(catalogName, databaseName, e);
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Delete a database from Glue data catalog only when database is empty.
     *
     * @param databaseName fully qualified name of database
     * @throws CatalogException Any Exception thrown due to glue error
     * @throws DatabaseNotExistException when database doesn't exists in glue catalog.
     */
    public void dropGlueDatabase(String databaseName)
            throws CatalogException, DatabaseNotExistException {

        GlueUtils.validate(databaseName);
        DeleteDatabaseRequest deleteDatabaseRequest =
                DeleteDatabaseRequest.builder()
                        .name(databaseName)
                        .catalogId(getGlueCatalogId())
                        .build();
        try {
            DeleteDatabaseResponse response = glueClient.deleteDatabase(deleteDatabaseRequest);
            if (LOG.isDebugEnabled()) {
                LOG.debug(GlueUtils.getDebugLog(response));
            }
            GlueUtils.validateGlueResponse(response);
        } catch (EntityNotFoundException e) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        } catch (GlueException e) {
            throw new CatalogException(catalogName, e);
        }
    }

    /**
     * Drops list of table in database from glue data catalog.
     *
     * @param databaseName fully qualified name of database
     * @param tables List of tables to remove from database.
     * @throws CatalogException Any Exception thrown due to glue error
     */
    public void deleteTablesFromDatabase(String databaseName, Collection<String> tables)
            throws CatalogException {
        GlueUtils.validate(databaseName);
        BatchDeleteTableRequest batchTableRequest =
                BatchDeleteTableRequest.builder()
                        .databaseName(databaseName)
                        .catalogId(getGlueCatalogId())
                        .tablesToDelete(tables)
                        .build();

        try {
            BatchDeleteTableResponse response = glueClient.batchDeleteTable(batchTableRequest);
            if (response.hasErrors()) {
                String errorMsg =
                        String.format(
                                "Glue Table errors:- %s",
                                response.errors().stream()
                                        .map(
                                                e ->
                                                        "Table: "
                                                                + e.tableName()
                                                                + "\nErrorDetail:  "
                                                                + e.errorDetail().errorMessage())
                                        .collect(Collectors.joining("\n")));
                LOG.error(errorMsg);
                throw new CatalogException(errorMsg);
            }
            GlueUtils.validateGlueResponse(response);
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Drops list of user defined function in database from glue data catalog.
     *
     * @param databaseName fully qualified name of database
     * @param functions List of tables to remove from database.
     * @throws CatalogException Any Exception thrown due to glue error
     */
    public void deleteFunctionsFromDatabase(String databaseName, Collection<String> functions)
            throws CatalogException {
        GlueUtils.validate(databaseName);
        try {
            DeleteUserDefinedFunctionRequest.Builder requestBuilder =
                    DeleteUserDefinedFunctionRequest.builder()
                            .databaseName(databaseName)
                            .catalogId(getGlueCatalogId());
            for (String functionName : functions) {
                requestBuilder.functionName(functionName);
                DeleteUserDefinedFunctionResponse response =
                        glueClient.deleteUserDefinedFunction(requestBuilder.build());
                if (LOG.isDebugEnabled()) {
                    LOG.debug(GlueUtils.getDebugLog(response));
                }
                GlueUtils.validateGlueResponse(response);
            }

        } catch (GlueException e) {
            LOG.error(String.format("Error deleting functions in database: %s", databaseName));
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Get a database from this glue data catalog.
     *
     * @param databaseName fully qualified name of database.
     * @return Instance of {@link CatalogDatabase } .
     * @throws DatabaseNotExistException when database doesn't exists in Glue data catalog.
     * @throws CatalogException when any unknown error occurs in glue.
     */
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        GlueUtils.validate(databaseName);
        GetDatabaseRequest getDatabaseRequest =
                GetDatabaseRequest.builder()
                        .name(databaseName)
                        .catalogId(getGlueCatalogId())
                        .build();
        try {
            GetDatabaseResponse response = glueClient.getDatabase(getDatabaseRequest);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER
                                + ": existing database. Client call response :- "
                                + response.sdkHttpResponse().statusText());
            }
            GlueUtils.validateGlueResponse(response);
            return response.database().name().equals(databaseName)
                    ? GlueUtils.getCatalogDatabase(response.database())
                    : null;

        } catch (EntityNotFoundException e) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        } catch (GlueException e) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER, e);
        }
    }

    /**
     * Update Database in Glue Metastore.
     *
     * @param databaseName Database name.
     * @param newDatabase instance of {@link CatalogDatabase}.
     * @throws CatalogException in case of Errors.
     */
    public void updateGlueDatabase(String databaseName, CatalogDatabase newDatabase)
            throws CatalogException {

        GlueUtils.validate(databaseName);
        Map<String, String> newProperties = new HashMap<>(newDatabase.getProperties());
        DatabaseInput.Builder databaseInputBuilder =
                DatabaseInput.builder()
                        .parameters(newProperties)
                        .description(newDatabase.getComment())
                        .name(databaseName);

        UpdateDatabaseRequest updateRequest =
                UpdateDatabaseRequest.builder()
                        .databaseInput(databaseInputBuilder.build())
                        .name(databaseName)
                        .catalogId(getGlueCatalogId())
                        .build();
        UpdateDatabaseResponse response = glueClient.updateDatabase(updateRequest);
        if (LOG.isDebugEnabled()) {
            LOG.debug(GlueUtils.getDebugLog(response));
        }
        GlueUtils.validateGlueResponse(response);
        LOG.info(String.format("Database Updated. %s", databaseName));
    }
}
