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

package org.apache.flink.table.catalog.glue;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.glue.model.BatchDeleteTableResponse;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.catalog.glue.GlueCatalogTest.WAREHOUSE_PATH;

/** Contains Utilities for Glue Catalog Tests. */
public class GlueCatalogTestUtils {

    public static final String DATABASE_DESCRPTION = "Test database";
    public static final String DATABASE_1 = "db1";
    public static final String DATABASE_2 = "db2";
    public static final String TABLE_1 = "t1";
    public static final String TABLE_2 = "t2";
    public static final String TABLE_3 = "t3";
    public static final String TABLE_4 = "t4";
    public static final String TABLE_5 = "t5";
    public static final String VIEW_1 = "v1";
    public static final String VIEW_2 = "v2";
    public static final String CATALOG_ID = "c1";
    public static final String COLUMN_1 = "col1";
    public static final String COLUMN_2 = "col2";
    public static final String STRING_DATATYPE = "string";
    public static final String COMMENT = "comment";
    public static final String EXPANDED_TEXT = "TEST EXPANDED_TEXT";
    public static final String ORIGINAL_TEXT = "TEST ORIGINAL_TEXT";
    public static final int SUCCESS_STATUS_CODE = 200;

    public static Map<String, String> getDatabaseParams() {
        return new HashMap<String, String>() {
            {
                put("key", "value");
                put("location-uri", WAREHOUSE_PATH);
            }
        };
    }

    public static Map<String, String> getTableParams() {
        return new HashMap<String, String>() {
            {
                put("k1", "v1");
            }
        };
    }

    public static Map<String, String> getPartitionSpecParams() {
        return new HashMap<String, String>() {
            {
                put("c1", "vp1");
                put("c2", "v2");
            }
        };
    }

    public static Map<String, String> getCatalogPartitionParams() {
        return new HashMap<String, String>() {
            {
                put("k1", "v1");
                put("kv2", "v2");
            }
        };
    }

    public static Map<String, String> getPartitionProperties() {
        Map<String, String> partitionProperties = getPartitionSpecParams();
        partitionProperties.put("location-uri", null);
        return partitionProperties;
    }

    public static SdkHttpResponse dummySuccessfulSdkHttpResponse() {
        return SdkHttpResponse.builder().statusCode(SUCCESS_STATUS_CODE).build();
    }

    public static CreateDatabaseResponse dummyCreateDatabaseResponse() {
        return (CreateDatabaseResponse)
                CreateDatabaseResponse.builder()
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static UpdateDatabaseResponse dummyUpdateDatabaseResponse() {
        return (UpdateDatabaseResponse)
                UpdateDatabaseResponse.builder()
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static GetDatabaseResponse dummyGetDatabaseResponse(
            String databaseName, String warehousePath, String description) {
        return (GetDatabaseResponse)
                GetDatabaseResponse.builder()
                        .database(
                                Database.builder()
                                        .name(databaseName)
                                        .parameters(getDatabaseParams())
                                        .locationUri(warehousePath)
                                        .description(description)
                                        .build())
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    /**
     * provides dummy list of Databases.
     *
     * @return Dummy list of database
     */
    public static GetDatabasesResponse dummyGetDatabasesResponse() {
        return (GetDatabasesResponse)
                GetDatabasesResponse.builder()
                        .databaseList(
                                Database.builder()
                                        .name(DATABASE_1)
                                        .catalogId(CATALOG_ID)
                                        .parameters(getDatabaseParams())
                                        .build(),
                                Database.builder()
                                        .name(DATABASE_2)
                                        .parameters(getDatabaseParams())
                                        .catalogId(CATALOG_ID)
                                        .build())
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static CreateTableResponse dummyCreateTableResponse() {
        return (CreateTableResponse)
                CreateTableResponse.builder()
                        .sdkHttpResponse(GlueCatalogTestUtils.dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static GetTableResponse dummyTableResponseWithTwoColumns(
            ObjectPath tablePath, String tableType) {
        return (GetTableResponse)
                GetTableResponse.builder()
                        .table(
                                Table.builder()
                                        .tableType(tableType)
                                        .catalogId(CATALOG_ID)
                                        .databaseName(tablePath.getDatabaseName())
                                        .name(tablePath.getObjectName())
                                        .parameters(getTableParams())
                                        .viewExpandedText(EXPANDED_TEXT)
                                        .viewOriginalText(ORIGINAL_TEXT)
                                        .storageDescriptor(
                                                StorageDescriptor.builder()
                                                        .parameters(getTableParams())
                                                        .columns(
                                                                Column.builder()
                                                                        .name(COLUMN_1)
                                                                        .type(STRING_DATATYPE)
                                                                        .build(),
                                                                Column.builder()
                                                                        .name(COLUMN_2)
                                                                        .type(STRING_DATATYPE)
                                                                        .build())
                                                        .build())
                                        .build())
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static GetTablesResponse dummyGetTablesResponseWithDifferentTableTypes(
            String databaseName) {
        return (GetTablesResponse)
                GetTablesResponse.builder()
                        .tableList(
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(TABLE_1)
                                        .tableType(CatalogBaseTable.TableKind.TABLE.name())
                                        .parameters(ImmutableMap.of("key1", "val1"))
                                        .build(),
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(TABLE_2)
                                        .tableType(CatalogBaseTable.TableKind.TABLE.name())
                                        .parameters(ImmutableMap.of("key2", "val2"))
                                        .build(),
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(TABLE_3)
                                        .tableType(CatalogBaseTable.TableKind.TABLE.name())
                                        .parameters(ImmutableMap.of("key3", "val3"))
                                        .build(),
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(TABLE_4)
                                        .tableType(CatalogBaseTable.TableKind.TABLE.name())
                                        .parameters(ImmutableMap.of("key4", "val4"))
                                        .build(),
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(TABLE_5)
                                        .tableType(CatalogBaseTable.TableKind.TABLE.name())
                                        .parameters(ImmutableMap.of("key5", "val5"))
                                        .build(),
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(VIEW_1)
                                        .tableType(CatalogBaseTable.TableKind.VIEW.name())
                                        .parameters(ImmutableMap.of("key1", "val1"))
                                        .build(),
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(VIEW_2)
                                        .tableType(CatalogBaseTable.TableKind.VIEW.name())
                                        .parameters(ImmutableMap.of("key3", "val3"))
                                        .build())
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static GetTablesResponse dummyGetTablesResponseContainingOnlyView(String databaseName) {
        return (GetTablesResponse)
                GetTablesResponse.builder()
                        .tableList(
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(VIEW_1)
                                        .tableType(CatalogBaseTable.TableKind.VIEW.name())
                                        .parameters(ImmutableMap.of("key1", "val1"))
                                        .build(),
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(VIEW_2)
                                        .tableType(CatalogBaseTable.TableKind.VIEW.name())
                                        .parameters(ImmutableMap.of("key3", "val3"))
                                        .build())
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static GetTablesResponse dummyGetTablesResponseContainingOnlyTable(String databaseName) {
        return (GetTablesResponse)
                GetTablesResponse.builder()
                        .tableList(
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(TABLE_1)
                                        .tableType(CatalogBaseTable.TableKind.TABLE.name())
                                        .parameters(ImmutableMap.of("key1", "val1"))
                                        .build(),
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(TABLE_2)
                                        .tableType(CatalogBaseTable.TableKind.TABLE.name())
                                        .parameters(ImmutableMap.of("key2", "val2"))
                                        .build(),
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(TABLE_3)
                                        .tableType(CatalogBaseTable.TableKind.TABLE.name())
                                        .parameters(ImmutableMap.of("key3", "val3"))
                                        .build(),
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(TABLE_4)
                                        .tableType(CatalogBaseTable.TableKind.TABLE.name())
                                        .parameters(ImmutableMap.of("key4", "val4"))
                                        .build(),
                                Table.builder()
                                        .databaseName(databaseName)
                                        .name(TABLE_5)
                                        .tableType(CatalogBaseTable.TableKind.TABLE.name())
                                        .parameters(ImmutableMap.of("key5", "val5"))
                                        .build())
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static GetPartitionResponse dummyGetPartitionResponse() {

        return (GetPartitionResponse)
                GetPartitionResponse.builder()
                        .partition(
                                Partition.builder()
                                        .databaseName(DATABASE_1)
                                        .tableName(TABLE_1)
                                        .parameters(getPartitionSpecParams())
                                        .storageDescriptor(
                                                StorageDescriptor.builder()
                                                        .parameters(getPartitionSpecParams())
                                                        .columns(
                                                                Column.builder()
                                                                        .type(STRING_DATATYPE)
                                                                        .name(COLUMN_1)
                                                                        .build())
                                                        .build())
                                        .catalogId(CATALOG_ID)
                                        .values("val1", "val2", "val3")
                                        .build())
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static GetTableResponse dummyGetTableResponseWithPartitionKeys(
            ObjectPath tablePath, String tableType) {

        return (GetTableResponse)
                GetTableResponse.builder()
                        .table(
                                Table.builder()
                                        .catalogId(CATALOG_ID)
                                        .partitionKeys(
                                                Column.builder()
                                                        .name(COLUMN_1)
                                                        .type(STRING_DATATYPE)
                                                        .build(),
                                                Column.builder()
                                                        .name(COLUMN_2)
                                                        .type(STRING_DATATYPE)
                                                        .build())
                                        .viewOriginalText("")
                                        .viewExpandedText("")
                                        .tableType(tableType)
                                        .databaseName(tablePath.getDatabaseName())
                                        .name(tablePath.getObjectName())
                                        .parameters(getTableParams())
                                        .storageDescriptor(
                                                StorageDescriptor.builder()
                                                        .parameters(getTableParams())
                                                        .columns(
                                                                Column.builder()
                                                                        .name(COLUMN_1)
                                                                        .type(STRING_DATATYPE)
                                                                        .build(),
                                                                Column.builder()
                                                                        .name(COLUMN_2)
                                                                        .type(STRING_DATATYPE)
                                                                        .build())
                                                        .build())
                                        .build())
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static CreatePartitionResponse dummyCreatePartitionResponse() {
        return (CreatePartitionResponse)
                CreatePartitionResponse.builder()
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static GetTablesResponse dummyGetEmptyTableList() {
        return (GetTablesResponse)
                GetTablesResponse.builder()
                        .tableList(new ArrayList<>())
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static DeleteDatabaseResponse dummyDeleteDatabaseResponse(String databaseName) {
        return (DeleteDatabaseResponse)
                DeleteDatabaseResponse.builder()
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }

    public static BatchDeleteTableResponse dummyBatchDeleteTable() {
        return (BatchDeleteTableResponse)
                BatchDeleteTableResponse.builder()
                        .sdkHttpResponse(dummySuccessfulSdkHttpResponse())
                        .build();
    }
}
