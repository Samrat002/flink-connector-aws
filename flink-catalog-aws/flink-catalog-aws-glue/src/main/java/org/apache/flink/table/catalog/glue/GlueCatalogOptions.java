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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.glue.constants.GlueCatalogConstants;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.catalog.glue.factory.GlueCatalogFactoryOptions.DEFAULT_DATABASE;

/** A collection of {@link ConfigOption} which is used in GlueCatalog. */
public class GlueCatalogOptions extends CommonCatalogOptions {

    public static final ConfigOption<String> INPUT_FORMAT =
            ConfigOptions.key(GlueCatalogConstants.TABLE_INPUT_FORMAT)
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> OUTPUT_FORMAT =
            ConfigOptions.key(GlueCatalogConstants.TABLE_OUTPUT_FORMAT)
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<Boolean> GLUE_CATALOG_SKIP_NAME_VALIDATION =
            ConfigOptions.key(AWSConfigConstants.GLUE_CATALOG_SKIP_NAME_VALIDATION)
                    .booleanType()
                    .defaultValue(AWSConfigConstants.GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT);

    public static final ConfigOption<String> GLUE_CATALOG_ENDPOINT =
            ConfigOptions.key(AWSConfigConstants.GLUE_CATALOG_ENDPOINT)
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> GLUE_CATALOG_ID =
            ConfigOptions.key(AWSConfigConstants.GLUE_CATALOG_ID).stringType().noDefaultValue();

    public static final ConfigOption<String> GLUE_ACCOUNT_ID =
            ConfigOptions.key(AWSConfigConstants.GLUE_ACCOUNT_ID).stringType().noDefaultValue();

    public static final ConfigOption<Boolean> GLUE_CATALOG_SKIP_ARCHIVE =
            ConfigOptions.key(AWSConfigConstants.GLUE_CATALOG_SKIP_ARCHIVE)
                    .booleanType()
                    .defaultValue(AWSConfigConstants.GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT);

    public static final ConfigOption<String> PATH =
            ConfigOptions.key("catalog-path").stringType().noDefaultValue();

    public static Set<ConfigOption<?>> getAllConfigOptions() {
        // list all config options declared above
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(INPUT_FORMAT);
        options.add(OUTPUT_FORMAT);
        options.add(GLUE_CATALOG_SKIP_NAME_VALIDATION);
        options.add(GLUE_CATALOG_ENDPOINT);
        options.add(GLUE_CATALOG_ID);
        options.add(GLUE_ACCOUNT_ID);
        options.add(GLUE_CATALOG_SKIP_ARCHIVE);
        options.add(DEFAULT_DATABASE);
        return options;
    }

    public static Set<ConfigOption<?>> getRequiredConfigOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        return options;
    }
}
