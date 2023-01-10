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

package org.apache.flink.table.catalog.glue.util;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.glue.GlueCatalogOptions;
import org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GlueResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** Utilities related glue Operation. */
public class GlueUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GlueUtils.class);

    /**
     * Glue supports lowercase naming convention.
     *
     * @param name fully qualified name.
     * @return modified name according to glue convention.
     */
    public static String getGlueConventionalName(String name) {

        return name.toLowerCase(Locale.ROOT);
    }

    /**
     * Extract location from database properties if present and remove location from properties.
     * fallback to create default location if not present
     *
     * @param databaseProperties database properties.
     * @param databaseName fully qualified name for database.
     * @return location for database.
     */
    public static String extractDatabaseLocation(
            final Map<String, String> databaseProperties,
            final String databaseName,
            final String catalogPath) {
        if (databaseProperties.containsKey(GlueCatalogConstants.LOCATION_URI)) {
            return databaseProperties.remove(GlueCatalogConstants.LOCATION_URI);
        } else {
            LOG.info("No location URI Set. Using Catalog Path as default");
            return catalogPath + GlueCatalogConstants.LOCATION_SEPARATOR + databaseName;
        }
    }

    /**
     * Extract location from database properties if present and remove location from properties.
     * fallback to create default location if not present
     *
     * @param tableProperties table properties.
     * @param tablePath fully qualified object for table.
     * @return location for table.
     */
    public static String extractTableLocation(
            final Map<String, String> tableProperties,
            final ObjectPath tablePath,
            final String catalogPath) {
        if (tableProperties.containsKey(GlueCatalogConstants.LOCATION_URI)) {
            return tableProperties.remove(GlueCatalogConstants.LOCATION_URI);
        } else {
            return catalogPath
                    + GlueCatalogConstants.LOCATION_SEPARATOR
                    + tablePath.getDatabaseName()
                    + GlueCatalogConstants.LOCATION_SEPARATOR
                    + tablePath.getObjectName();
        }
    }

    /**
     * Build CatalogDatabase instance using information from glue Database instance.
     *
     * @param glueDatabase {@link Database }
     * @return {@link CatalogDatabase } instance.
     */
    public static CatalogDatabase getCatalogDatabase(final Database glueDatabase) {
        Map<String, String> properties = new HashMap<>(glueDatabase.parameters());
        return new CatalogDatabaseImpl(properties, glueDatabase.description());
    }

    /**
     * A Glue database name cannot be longer than 252 characters. The only acceptable characters are
     * lowercase letters, numbers, and the underscore character. More details: <a
     * href="https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html">...</a>
     *
     * @param name name
     */
    public static void validate(String name) {
        checkArgument(
                name != null && GlueCatalogConstants.GLUE_DB_PATTERN.matcher(name).find(),
                "Database name is not according to Glue Norms, "
                        + "check here https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html");
    }

    /** validate response from client call. */
    public static void validateGlueResponse(GlueResponse response) {
        if (response != null && !response.sdkHttpResponse().isSuccessful()) {
            throw new CatalogException(GlueCatalogConstants.GLUE_EXCEPTION_MSG_IDENTIFIER);
        }
    }

    /**
     * @param udf Instance of UserDefinedFunction
     * @return ClassName for function
     */
    public static String getCatalogFunctionClassName(final UserDefinedFunction udf) {
        validateUDFClassName(udf.className());
        return udf.functionName().split(GlueCatalogConstants.DEFAULT_SEPARATOR)[1];
    }

    /**
     * Validates UDF class name from glue.
     *
     * @param name name of UDF.
     */
    private static void validateUDFClassName(final String name) {
        checkArgument(!isNullOrWhitespaceOnly(name));
        if (name.split(GlueCatalogConstants.DEFAULT_SEPARATOR).length != 3) {
            throw new ValidationException("Improper Classname");
        }
    }

    /**
     * Derive functionalLanguage from glue function name. Glue doesn't have any attribute to save
     * the functionalLanguage Name. Thus, storing FunctionalLanguage in the name itself.
     *
     * @param glueFunction Function name from glue.
     * @return Identifier for FunctionalLanguage.
     */
    public static FunctionLanguage getFunctionalLanguage(final UserDefinedFunction glueFunction) {
        if (glueFunction.className().startsWith(GlueCatalogConstants.FLINK_JAVA_FUNCTION_PREFIX)) {
            return FunctionLanguage.JAVA;
        } else if (glueFunction
                .className()
                .startsWith(GlueCatalogConstants.FLINK_PYTHON_FUNCTION_PREFIX)) {
            return FunctionLanguage.PYTHON;
        } else if (glueFunction
                .className()
                .startsWith(GlueCatalogConstants.FLINK_SCALA_FUNCTION_PREFIX)) {
            return FunctionLanguage.SCALA;
        } else {
            throw new CatalogException("Invalid Functional Language");
        }
    }

    /**
     * Get expanded Query from CatalogBaseTable.
     *
     * @param table Instance of catalogBaseTable.
     * @return expandedQuery for Glue Table.
     */
    public static String getExpandedQuery(CatalogBaseTable table) {
        // https://issues.apache.org/jira/browse/FLINK-31961
        return "";
    }

    /**
     * Get Original Query from CatalogBaseTable.
     *
     * @param table Instance of CatalogBaseTable.
     * @return OriginalQuery for Glue Table.
     */
    public static String getOriginalQuery(CatalogBaseTable table) {
        // https://issues.apache.org/jira/browse/FLINK-31961
        return "";
    }

    /**
     * Extract table owner name and remove from properties.
     *
     * @param properties Map of properties.
     * @return fully qualified owner name.
     */
    public static String extractTableOwner(Map<String, String> properties) {
        return properties.containsKey(GlueCatalogConstants.TABLE_OWNER)
                ? properties.remove(GlueCatalogConstants.TABLE_OWNER)
                : null;
    }

    /**
     * Derive Instance of Glue Column from {@link CatalogBaseTable}.
     *
     * @param catalogBaseTable Instance of {@link CatalogBaseTable}.
     * @param tableSchema TableSchema.
     * @param fieldName name of {@link Column}.
     * @return Instance of {@link Column}.
     * @throws CatalogException Throws exception in case of failure.
     */
    public static Column getGlueColumn(
            final CatalogBaseTable catalogBaseTable,
            final TableSchema tableSchema,
            final String fieldName)
            throws CatalogException {
        LOG.info("Getting glue column details.");
        Optional<DataType> dataType = tableSchema.getFieldDataType(fieldName);
        if (dataType.isPresent()) {
            String glueDataType = dataType.get().toString();
            return Column.builder()
                    .comment(catalogBaseTable.getComment())
                    .type(glueDataType)
                    .name(fieldName)
                    .build();
        } else {
            throw new CatalogException("DataType information missing from table schema");
        }
    }

    /**
     * Build set of {@link Column} associated with table.
     *
     * @param catalogBaseTable instance of {@link CatalogBaseTable}.
     * @return Set of Column
     */
    public static Set<Column> getGlueColumnsFromCatalogTable(
            final CatalogBaseTable catalogBaseTable) {
        checkNotNull(catalogBaseTable);
        TableSchema tableSchema = catalogBaseTable.getSchema();
        return Arrays.stream(tableSchema.getFieldNames())
                .map(fieldName -> getGlueColumn(catalogBaseTable, tableSchema, fieldName))
                .collect(Collectors.toSet());
    }

    /**
     * Extract InputFormat from properties if present and remove inputFormat from properties.
     * fallback to default format if not present
     *
     * @param tableProperties Key/Value properties
     * @return input Format.
     */
    public static String extractInputFormat(final Map<String, String> tableProperties) {
        return tableProperties.containsKey(GlueCatalogConstants.TABLE_INPUT_FORMAT)
                ? tableProperties.remove(GlueCatalogConstants.TABLE_INPUT_FORMAT)
                : GlueCatalogOptions.INPUT_FORMAT.defaultValue();
    }

    /**
     * Extract OutputFormat from properties if present and remove outputFormat from properties.
     * fallback to default format if not present
     *
     * @param tableProperties Key/Value properties
     * @return output Format.
     */
    public static String extractOutputFormat(Map<String, String> tableProperties) {
        return tableProperties.containsKey(GlueCatalogConstants.TABLE_OUTPUT_FORMAT)
                ? tableProperties.remove(GlueCatalogConstants.TABLE_OUTPUT_FORMAT)
                : GlueCatalogOptions.OUTPUT_FORMAT.defaultValue();
    }

    /**
     * Get list of filtered columns which are partition columns.
     *
     * @param catalogTable {@link CatalogTable} instance.
     * @param columns List of all column in table.
     * @return List of column marked as partition key.
     */
    public static Collection<Column> getPartitionKeys(
            CatalogTable catalogTable, Collection<Column> columns) {
        Set<String> partitionKeys = new HashSet<>(catalogTable.getPartitionKeys());
        return columns.stream()
                .filter(column -> partitionKeys.contains(column.name()))
                .collect(Collectors.toList());
    }

    /**
     * check spec1 is subset of spec2.
     *
     * @param subsetProps Key/Value pair spec
     * @param props Key/Value pair spec
     * @return is spec1 is subset of spec2
     */
    public static boolean specSubset(Map<String, String> subsetProps, Map<String, String> props) {
        return subsetProps.entrySet().stream()
                .allMatch(e -> e.getValue().equals(props.get(e.getKey())));
    }

    public static String getDebugLog(final GlueResponse response) {
        return String.format(
                "Glue response : status = %s \n " + "Details = %s \nMetadataResponse = %s",
                response.sdkHttpResponse().isSuccessful(),
                response.sdkHttpResponse().toString(),
                response.responseMetadata());
    }

    /**
     * Derive {@link Schema} from Glue {@link Table}.
     *
     * @param glueTable Instance of {@link Table}
     * @return {@link Schema} of table.
     */
    public static Schema getSchemaFromGlueTable(Table glueTable) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (Column col : glueTable.storageDescriptor().columns()) {
            schemaBuilder.column(col.name(), col.type());
        }
        return schemaBuilder.build();
    }

    /**
     * Get column names from List of {@link Column}.
     *
     * @param columns List of {@link Column}.
     * @return Names of all Columns.
     */
    public static List<String> getColumnNames(final List<Column> columns) {
        return columns.stream().map(Column::name).collect(Collectors.toList());
    }

    /**
     * Glue Function class name.
     *
     * @param function Catalog Function.
     * @return function class name.
     */
    public static String getGlueFunctionClassName(CatalogFunction function) {
        if (function.getFunctionLanguage().equals(FunctionLanguage.JAVA)) {
            return GlueCatalogConstants.FLINK_JAVA_FUNCTION_PREFIX
                    + GlueCatalogConstants.DEFAULT_SEPARATOR
                    + function.getClassName();
        } else if (function.getFunctionLanguage().equals(FunctionLanguage.SCALA)) {
            return GlueCatalogConstants.FLINK_SCALA_FUNCTION_PREFIX
                    + GlueCatalogConstants.DEFAULT_SEPARATOR
                    + function.getClassName();
        } else if (function.getFunctionLanguage().equals(FunctionLanguage.PYTHON)) {
            return GlueCatalogConstants.FLINK_PYTHON_FUNCTION_PREFIX
                    + GlueCatalogConstants.DEFAULT_SEPARATOR
                    + function.getClassName();
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "GlueCatalog supports only creating: [%s]",
                            Arrays.stream(FunctionLanguage.values())
                                    .map(FunctionLanguage::name)
                                    .collect(Collectors.joining(GlueCatalogConstants.NEXT_LINE))));
        }
    }

    /**
     * Derive tableLocation.
     *
     * @param tablePath Fully Qualified Table Path.
     * @param database Instance of {@link CatalogDatabase}.
     * @return table location.
     */
    public static String getTableLocation(ObjectPath tablePath, CatalogDatabase database) {
        return database.getProperties().get(GlueCatalogConstants.LOCATION_URI)
                + GlueCatalogConstants.LOCATION_SEPARATOR
                + tablePath.getObjectName();
    }
}
