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

package org.apache.flink.table.catalog.glue.constants;

import org.apache.flink.table.catalog.glue.GlueCatalog;

import java.util.regex.Pattern;

/** Configs for catalog meta-objects in {@link GlueCatalog}. */
public class GlueCatalogConstants {

    public static final String COMMENT = "comment";
    public static final String DEFAULT_SEPARATOR = ":";
    public static final String LOCATION_SEPARATOR = "/";
    public static final String LOCATION_URI = "path";
    public static final String AND = "and";
    public static final String NEXT_LINE = "\n";
    public static final String SPACE = " ";

    public static final String TABLE_OWNER = "owner";
    public static final String TABLE_INPUT_FORMAT = "table.input.format";
    public static final String TABLE_OUTPUT_FORMAT = "table.output.format";

    public static final String FLINK_SCALA_FUNCTION_PREFIX = "flink:scala:";
    public static final String FLINK_PYTHON_FUNCTION_PREFIX = "flink:python:";
    public static final String FLINK_JAVA_FUNCTION_PREFIX = "flink:java:";

    public static final String FLINK_CATALOG = "FLINK_CATALOG";

    public static final Pattern GLUE_DB_PATTERN = Pattern.compile("^[a-z0-9_]{1,252}$");
    public static final String GLUE_EXCEPTION_MSG_IDENTIFIER = "GLUE EXCEPTION";
    public static final String TABLE_NOT_EXISTS_IDENTIFIER = "TABLE DOESN'T EXISTS";
    public static final String DEFAULT_PARTITION_NAME = "__GLUE_DEFAULT_PARTITION__";
}
