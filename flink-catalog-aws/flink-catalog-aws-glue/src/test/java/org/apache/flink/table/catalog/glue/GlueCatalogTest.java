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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.aws.config.AWSConfig;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.glue.operator.GlueDatabaseOperator;
import org.apache.flink.table.catalog.glue.operator.GlueFunctionOperator;
import org.apache.flink.table.catalog.glue.operator.GluePartitionOperator;
import org.apache.flink.table.catalog.glue.operator.GlueTableOperator;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.COMMENT;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.DATABASE_1;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.DATABASE_2;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.DATABASE_DESCRPTION;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.TABLE_1;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.TABLE_2;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.TABLE_3;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.TABLE_4;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.TABLE_5;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.VIEW_1;
import static org.apache.flink.table.catalog.glue.GlueCatalogTestUtils.VIEW_2;

class GlueCatalogTest {

    public static final String WAREHOUSE_PATH = "s3://bucket";
    private static final String CATALOG_NAME = "glue";
    private static DummyGlueClient glue;
    private static GlueCatalog glueCatalog;
    private static GlueTableOperator glueTableOperator;

    @BeforeAll
    static void setUp() {
        glue = new DummyGlueClient();
        ReadableConfig catalogConfig = new Configuration();
        String catalogPath = catalogConfig.get(GlueCatalogOptions.PATH);
        AWSConfig awsConfig = new AWSConfig(catalogConfig);
        GlueDatabaseOperator glueDatabaseOperator =
                new GlueDatabaseOperator(CATALOG_NAME, catalogPath, awsConfig, glue);
        glueTableOperator = new GlueTableOperator(CATALOG_NAME, catalogPath, awsConfig, glue);
        GluePartitionOperator gluePartitionOperator =
                new GluePartitionOperator(CATALOG_NAME, catalogPath, awsConfig, glue);
        GlueFunctionOperator glueFunctionOperator =
                new GlueFunctionOperator(CATALOG_NAME, catalogPath, awsConfig, glue);
        glueCatalog =
                new GlueCatalog(
                        CATALOG_NAME,
                        GlueCatalog.DEFAULT_DB,
                        glueDatabaseOperator,
                        glueTableOperator,
                        gluePartitionOperator,
                        glueFunctionOperator);
    }

    @Test
    void testCreateDatabase() throws DatabaseNotExistException {
        glue.setDatabaseName(DATABASE_1);
        glue.setDescription(DATABASE_DESCRPTION);
        glue.setWarehousePath(WAREHOUSE_PATH);
        CatalogDatabase catalogDatabase = glueCatalog.getDatabase(DATABASE_1);
        Assertions.assertNotNull(catalogDatabase);
        Assertions.assertNotNull(catalogDatabase.getProperties());
        Assertions.assertNotNull(catalogDatabase.getComment());
        Assertions.assertEquals(DATABASE_DESCRPTION, catalogDatabase.getComment());
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.createDatabase(CATALOG_NAME, catalogDatabase, true));
    }

    @Test
    void testAlterDatabase() throws DatabaseNotExistException {
        glue.setDatabaseName(DATABASE_1);
        glue.setWarehousePath(WAREHOUSE_PATH);
        glue.setDescription(DATABASE_DESCRPTION);
        CatalogDatabase catalogDatabase = glueCatalog.getDatabase(DATABASE_1);

        Assertions.assertNotNull(catalogDatabase);
        Assertions.assertNotNull(catalogDatabase.getProperties());
        Assertions.assertNotNull(catalogDatabase.getComment());
        Assertions.assertEquals(
                GlueCatalogTestUtils.DATABASE_DESCRPTION, catalogDatabase.getComment());

        Map<String, String> properties = catalogDatabase.getProperties();
        properties.put("newkey", "val");
        CatalogDatabase newCatalogDatabase = catalogDatabase.copy(properties);
        Assertions.assertDoesNotThrow(
                () -> glueCatalog.alterDatabase(DATABASE_1, newCatalogDatabase, false));
    }

    @Test
    void testDropDatabase() {
        glue.setDatabaseName(DATABASE_1);
        Assertions.assertDoesNotThrow(() -> glueCatalog.dropDatabase(DATABASE_2, true, false));
        Assertions.assertThrows(
                DatabaseNotExistException.class,
                () -> glueCatalog.dropDatabase(DATABASE_2, false, false));
    }

    @Test
    void testListDatabases() {
        List<String> expectedDatabasesList = Arrays.asList(DATABASE_1, DATABASE_2);
        Assertions.assertEquals(expectedDatabasesList, glueCatalog.listDatabases());
    }

    @Test
    void testGetDatabase() throws DatabaseNotExistException {
        glue.setDatabaseName(DATABASE_1);
        glue.setDescription(DATABASE_DESCRPTION);
        glue.setWarehousePath(WAREHOUSE_PATH);
        Assertions.assertTrue(glueCatalog.databaseExists(DATABASE_1));
        Assertions.assertFalse(glueCatalog.databaseExists(DATABASE_2));

        CatalogDatabase db = glueCatalog.getDatabase(DATABASE_1);
        Assertions.assertEquals("Test database", db.getComment());

        Assertions.assertEquals(GlueCatalogTestUtils.getDatabaseParams(), db.getProperties());
    }

    @Test
    void testDatabaseExists() {
        Assertions.assertTrue(glueCatalog.databaseExists(DATABASE_1));
        // false negative test case.
        Assertions.assertFalse(glueCatalog.databaseExists(DATABASE_2));
    }

    @Test
    public void testGetTable() throws TableNotExistException {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        GlueCatalogTestUtils.getTableParams();
        glue.setTablePath(tablePath);
        glue.setDescription(DATABASE_DESCRPTION);
        glue.setTableExists(true);
        glue.setTableType(CatalogBaseTable.TableKind.TABLE.name());
        CatalogBaseTable table =
                glueTableOperator.getCatalogBaseTableFromGlueTable(
                        glueTableOperator.getGlueTable(tablePath));

        Assertions.assertNotNull(table);
        Assertions.assertEquals(
                CatalogBaseTable.TableKind.TABLE.name(), table.getTableKind().name());
        Assertions.assertEquals(GlueCatalogTestUtils.getTableParams(), table.getOptions());
    }

    @Test
    public void testGetView() throws TableNotExistException {

        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        Map<String, String> tableParams = GlueCatalogTestUtils.getTableParams();
        GlueCatalogTestUtils.getTableParams();
        glue.setTablePath(tablePath);
        glue.setDescription(DATABASE_DESCRPTION);
        glue.setTableExists(true);
        glue.setTableType(CatalogBaseTable.TableKind.VIEW.name());
        CatalogBaseTable view =
                glueTableOperator.getCatalogBaseTableFromGlueTable(
                        glueTableOperator.getGlueTable(tablePath));

        Assertions.assertNotNull(view);
        Assertions.assertEquals(CatalogBaseTable.TableKind.VIEW.name(), view.getTableKind().name());
        Assertions.assertEquals(tableParams, view.getOptions());
    }

    @Test
    public void testTableExists() {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        GlueCatalogTestUtils.getTableParams();
        glue.setDatabaseName(DATABASE_1);
        glue.setWarehousePath(WAREHOUSE_PATH);
        glue.setTablePath(tablePath);
        glue.setDescription(DATABASE_DESCRPTION);
        glue.setTableExists(true);
        glue.setTableType(CatalogBaseTable.TableKind.TABLE.name());
        Assertions.assertDoesNotThrow(() -> glueCatalog.tableExists(tablePath));
        Assertions.assertTrue(glueCatalog.tableExists(tablePath));
        Assertions.assertFalse(glueCatalog.tableExists(new ObjectPath(DATABASE_1, TABLE_2)));
    }

    @Test
    public void testListTables() throws DatabaseNotExistException {

        glue.setDatabaseName(DATABASE_1);
        glue.setWarehousePath(WAREHOUSE_PATH);
        glue.setDescription(DATABASE_DESCRPTION);
        glue.setTableType(CatalogBaseTable.TableKind.TABLE.name());
        glue.setTableExists(true);

        Assertions.assertEquals(5, glueCatalog.listTables(DATABASE_1).size());
        Assertions.assertEquals(
                Lists.newArrayList(TABLE_1, TABLE_2, TABLE_3, TABLE_4, TABLE_5),
                glueCatalog.listTables(DATABASE_1));
    }

    @Test
    public void testListTablesWithCombinationOfDifferentTableKind()
            throws DatabaseNotExistException {

        glue.setDatabaseName(DATABASE_1);
        glue.setWarehousePath(WAREHOUSE_PATH);
        glue.setDescription(DATABASE_DESCRPTION);
        glue.setTableType(
                CatalogBaseTable.TableKind.TABLE.name() + CatalogBaseTable.TableKind.VIEW.name());
        glue.setTableExists(true);

        Assertions.assertEquals(7, glueCatalog.listTables(DATABASE_1).size());
        Assertions.assertEquals(
                Lists.newArrayList(TABLE_1, TABLE_2, TABLE_3, TABLE_4, TABLE_5, VIEW_1, VIEW_2),
                glueCatalog.listTables(DATABASE_1));
    }

    @Test
    public void testCreateTable()
            throws DatabaseNotExistException, TableNotExistException, TableAlreadyExistException {

        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);

        glue.setDatabaseName(DATABASE_1);
        glue.setWarehousePath(WAREHOUSE_PATH);
        glue.setDescription(DATABASE_DESCRPTION);
        glue.setTableExists(true);
        glue.setTableType(CatalogBaseTable.TableKind.TABLE.name());
        glue.setTablePath(tablePath);
        Map<String, String> tableParams = GlueCatalogTestUtils.getTableParams();

        CatalogBaseTable catalogTable =
                glueTableOperator.getCatalogBaseTableFromGlueTable(
                        glueTableOperator.getGlueTable(tablePath));

        glueCatalog.createTable(tablePath, catalogTable, true);
        CatalogBaseTable catalogBaseTable = glueCatalog.getTable(tablePath);

        Assertions.assertNotNull(catalogBaseTable);
        Assertions.assertEquals(tableParams, catalogBaseTable.getOptions());
        Assertions.assertEquals(
                CatalogTable.TableKind.TABLE.name(), catalogTable.getTableKind().name());
    }

    @Test
    public void testCreateView() throws TableNotExistException {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        glue.setDatabaseName(DATABASE_1);
        glue.setWarehousePath(WAREHOUSE_PATH);
        glue.setDescription(DATABASE_DESCRPTION);
        glue.setTableExists(true);
        glue.setTableType(CatalogBaseTable.TableKind.VIEW.name());
        glue.setTablePath(tablePath);
        Map<String, String> tableParams = GlueCatalogTestUtils.getTableParams();
        CatalogBaseTable catalogTable =
                glueTableOperator.getCatalogBaseTableFromGlueTable(
                        glueTableOperator.getGlueTable(tablePath));
        Assertions.assertDoesNotThrow(() -> glueCatalog.createTable(tablePath, catalogTable, true));
        CatalogBaseTable catalogBaseTable = glueCatalog.getTable(tablePath);
        Assertions.assertNotNull(catalogBaseTable);
        Assertions.assertEquals(tableParams, catalogBaseTable.getOptions());
        Assertions.assertEquals(
                CatalogTable.TableKind.VIEW.name(), catalogTable.getTableKind().name());
    }

    @Test
    public void testListView() throws DatabaseNotExistException {
        glue.setDatabaseName(DATABASE_1);
        glue.setWarehousePath(WAREHOUSE_PATH);
        glue.setDescription(DATABASE_DESCRPTION);
        glue.setTableExists(true);
        glue.setTableType(CatalogBaseTable.TableKind.VIEW.name());
        Assertions.assertNotSame(
                Lists.newArrayList(TABLE_1, TABLE_2, VIEW_1, VIEW_2),
                glueCatalog.listViews(DATABASE_1),
                "Should not contain any identifier of type table");
        Assertions.assertEquals(
                Lists.newArrayList(VIEW_1, VIEW_2), glueCatalog.listViews(DATABASE_1));
    }

    @Test
    public void testCreatePartition()
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    PartitionNotExistException {

        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);
        glue.setTableExists(true);
        glue.setPartitionedTable(true);
        glue.setTablePath(tablePath);
        glue.setDatabaseName(DATABASE_1);
        glue.setDescription(DATABASE_DESCRPTION);

        CatalogPartitionSpec partitionSpec =
                new CatalogPartitionSpec(GlueCatalogTestUtils.getPartitionSpecParams());
        CatalogPartition catalogPartition =
                new CatalogPartitionImpl(GlueCatalogTestUtils.getCatalogPartitionParams(), COMMENT);
        glueCatalog.createPartition(tablePath, partitionSpec, catalogPartition, true);
        CatalogPartition partition = glueCatalog.getPartition(tablePath, partitionSpec);

        Assertions.assertNotNull(partition);
        Assertions.assertEquals(
                GlueCatalogTestUtils.getPartitionProperties(), partition.getProperties());
    }

    @Test
    public void testGetPartition() throws PartitionNotExistException {

        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);

        glue.setTableExists(true);
        glue.setDatabaseName(DATABASE_1);
        glue.setTablePath(tablePath);
        glue.setTableType(CatalogBaseTable.TableKind.TABLE.name());

        CatalogPartitionSpec partitionSpec =
                new CatalogPartitionSpec(GlueCatalogTestUtils.getPartitionSpecParams());
        CatalogPartition catalogPartition =
                new CatalogPartitionImpl(GlueCatalogTestUtils.getCatalogPartitionParams(), COMMENT);
        Assertions.assertNotNull(catalogPartition);

        CatalogPartition partition = glueCatalog.getPartition(tablePath, partitionSpec);
        Assertions.assertNotNull(partition);
        Assertions.assertNull(partition.getComment());
        Assertions.assertEquals(
                GlueCatalogTestUtils.getPartitionProperties(), partition.getProperties());
    }

    @Test
    public void testPartitionExists() {
        ObjectPath tablePath = new ObjectPath(DATABASE_1, TABLE_1);

        glue.setTablePath(tablePath);
        glue.setDatabaseName(DATABASE_1);

        CatalogPartitionSpec partitionSpec =
                new CatalogPartitionSpec(GlueCatalogTestUtils.getPartitionSpecParams());
        boolean partitionExists = glueCatalog.partitionExists(tablePath, partitionSpec);
        Assertions.assertTrue(partitionExists);
    }
}
