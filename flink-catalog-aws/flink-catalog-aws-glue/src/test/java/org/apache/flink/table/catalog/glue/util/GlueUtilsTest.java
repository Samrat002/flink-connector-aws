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

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Test methods in GlueUtils . */
public class GlueUtilsTest {

    private static final String WAREHOUSE_PATH = "s3://bucket";

    @Test
    void testGetGlueConventionalName() {
        String name = "MyName";
        Assertions.assertEquals("myname", GlueUtils.getGlueConventionalName(name));

        String name1 = "Mtx@ndfv";
        Assertions.assertThrows(IllegalArgumentException.class, () -> GlueUtils.validate(name1));
    }

    @Test
    void testExtractDatabaseLocation() {
        Map<String, String> propertiesWithLocationUri =
                new HashMap() {
                    {
                        put(GlueCatalogConstants.LOCATION_URI, "s3://some-path/myDb/");
                        put("k1", "v1");
                    }
                };

        String location =
                GlueUtils.extractDatabaseLocation(propertiesWithLocationUri, "db1", WAREHOUSE_PATH);
        Assertions.assertEquals("s3://some-path/myDb/", location);

        String newLocation =
                GlueUtils.extractDatabaseLocation(propertiesWithLocationUri, "db1", WAREHOUSE_PATH);
        Assertions.assertNotEquals("s3://some-path/myDb/", newLocation);
        Assertions.assertEquals(
                WAREHOUSE_PATH + GlueCatalogConstants.LOCATION_SEPARATOR + "db1", newLocation);
    }

    @Test
    void testExtractTableLocation() {
        Map<String, String> propertiesWithLocationUri =
                new HashMap<String, String>() {
                    {
                        put(GlueCatalogConstants.LOCATION_URI, "s3://some-path/myDb/myTable/");
                        put("k1", "v1");
                    }
                };
        ObjectPath tablePath = new ObjectPath("db1", "t1");
        String location =
                GlueUtils.extractTableLocation(
                        propertiesWithLocationUri, tablePath, WAREHOUSE_PATH);
        Assertions.assertEquals("s3://some-path/myDb/myTable/", location);

        String newLocation =
                GlueUtils.extractTableLocation(
                        propertiesWithLocationUri, tablePath, WAREHOUSE_PATH);
        Assertions.assertNotEquals("s3://some-path/myDb/myTable", newLocation);
        Assertions.assertEquals(
                WAREHOUSE_PATH
                        + GlueCatalogConstants.LOCATION_SEPARATOR
                        + "db1"
                        + GlueCatalogConstants.LOCATION_SEPARATOR
                        + "t1",
                newLocation);
    }

    @Test
    void testGetCatalogDatabase() {}
}
