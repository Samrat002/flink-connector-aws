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

import software.amazon.awssdk.services.glue.GlueClient;

/**
 * Glue related operation. Important Note : * <a
 * href="https://aws.amazon.com/premiumsupport/knowledge-center/glue-crawler-internal-service-exception/">...</a>
 */
public abstract class GlueOperator {

    /**
     * Instance of AwsProperties which holds the configs related to configure glue and aws setup.
     */
    protected final AWSConfig awsConfig;

    /** http client for glue client. Current implementation for client is sync type. */
    protected final GlueClient glueClient;

    protected final String catalogName;

    public final String catalogPath;

    public GlueOperator(
            String catalogName, String catalogPath, AWSConfig awsConfig, GlueClient glueClient) {
        this.awsConfig = awsConfig;
        this.glueClient = glueClient;
        this.catalogPath = catalogPath;
        this.catalogName = catalogName;
    }

    public String getGlueCatalogId() {
        return awsConfig.getGlueCatalogId();
    }
}
