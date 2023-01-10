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

package org.apache.flink.connector.aws.util;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.aws.config.AWSConfig;

import software.amazon.awssdk.services.glue.GlueClient;

/** Default factories. */
public class AwsClientFactories {

    private AwsClientFactories() {}

    public static AwsClientFactory factory(AWSConfig awsConfig) {
        return new DefaultAwsClientFactory(awsConfig);
    }

    static class DefaultAwsClientFactory implements AwsClientFactory {

        /** instance that holds provides. */
        private AWSConfig awsConfig;

        DefaultAwsClientFactory(AWSConfig config) {
            awsConfig = config;
        }

        @Override
        public GlueClient glue() {
            return GlueClient.builder()
                    .applyMutation(awsConfig::applyHttpClientConfigurations)
                    .applyMutation(awsConfig::applyGlueEndpointConfigurations)
                    .build();
        }

        @Override
        public void initialize(ReadableConfig properties) {
            this.awsConfig = new AWSConfig(properties);
        }
    }
}
