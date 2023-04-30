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

package org.apache.flink.connector.aws.config;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;

import java.net.URI;
import java.time.Duration;

/** Aws Config for glue and other clients. */
public class AWSConfig {

    private final Long httpClientUrlConnectionConnectionTimeoutMs;

    private final Long httpClientUrlConnectionSocketTimeoutMs;

    private final Long httpClientApacheConnectionAcquisitionTimeoutMs;

    private final Long httpClientApacheConnectionMaxIdleTimeMs;

    private final Long httpClientApacheConnectionTimeToLiveMs;

    private final Long httpClientApacheConnectionTimeoutMs;

    private final Boolean httpClientApacheExpectContinueEnabled;

    private final Integer httpClientApacheMaxConnections;

    private final Long httpClientApacheSocketTimeoutMs;

    private final Boolean httpClientApacheTcpKeepAliveEnabled;

    private final Boolean httpClientApacheUseIdleConnectionReaperEnabled;

    private final String glueEndpoint;

    private final String glueCatalogId;

    private final Boolean glueCatalogSkipArchive;

    private final Boolean glueCatalogSkipNameValidation;

    /** http client. */
    private String httpClientType;

    public AWSConfig(ReadableConfig properties) {

        if (properties
                .getOptional(
                        ConfigOptions.key(AWSConfigConstants.HTTP_CLIENT_TYPE)
                                .stringType()
                                .noDefaultValue())
                .isPresent()) {
            this.httpClientType =
                    properties
                            .getOptional(
                                    ConfigOptions.key(AWSConfigConstants.HTTP_CLIENT_TYPE)
                                            .stringType()
                                            .noDefaultValue())
                            .get();
        }

        this.httpClientUrlConnectionConnectionTimeoutMs =
                properties.get(
                        ConfigOptions.key(
                                        AWSConfigConstants
                                                .HTTP_CLIENT_URLCONNECTION_CONNECTION_TIMEOUT_MS)
                                .longType()
                                .noDefaultValue());

        this.httpClientUrlConnectionSocketTimeoutMs =
                properties.get(
                        ConfigOptions.key(
                                        AWSConfigConstants
                                                .HTTP_CLIENT_URLCONNECTION_SOCKET_TIMEOUT_MS)
                                .longType()
                                .noDefaultValue());

        this.httpClientApacheConnectionAcquisitionTimeoutMs =
                properties.get(
                        ConfigOptions.key(
                                        AWSConfigConstants
                                                .HTTP_CLIENT_APACHE_CONNECTION_ACQUISITION_TIMEOUT_MS)
                                .longType()
                                .noDefaultValue());

        this.httpClientApacheConnectionMaxIdleTimeMs =
                properties.get(
                        ConfigOptions.key(
                                        AWSConfigConstants
                                                .HTTP_CLIENT_APACHE_CONNECTION_MAX_IDLE_TIME_MS)
                                .longType()
                                .noDefaultValue());

        this.httpClientApacheConnectionTimeToLiveMs =
                properties.get(
                        ConfigOptions.key(
                                        AWSConfigConstants
                                                .HTTP_CLIENT_APACHE_CONNECTION_TIME_TO_LIVE_MS)
                                .longType()
                                .noDefaultValue());

        this.httpClientApacheConnectionTimeoutMs =
                properties.get(
                        ConfigOptions.key(AWSConfigConstants.HTTP_CLIENT_CONNECTION_TIMEOUT_MS)
                                .longType()
                                .noDefaultValue());

        this.httpClientApacheExpectContinueEnabled =
                properties
                        .getOptional(
                                ConfigOptions.key(
                                                AWSConfigConstants
                                                        .HTTP_CLIENT_APACHE_EXPECT_CONTINUE_ENABLED)
                                        .booleanType()
                                        .noDefaultValue())
                        .orElse(false);
        this.httpClientApacheMaxConnections =
                properties
                        .getOptional(
                                ConfigOptions.key(
                                                AWSConfigConstants
                                                        .HTTP_CLIENT_APACHE_MAX_CONNECTIONS)
                                        .intType()
                                        .noDefaultValue())
                        .orElse(1);

        this.httpClientApacheSocketTimeoutMs =
                properties
                        .getOptional(
                                ConfigOptions.key(AWSConfigConstants.HTTP_CLIENT_SOCKET_TIMEOUT_MS)
                                        .longType()
                                        .noDefaultValue())
                        .orElse(0L);

        this.httpClientApacheTcpKeepAliveEnabled =
                properties.get(
                        ConfigOptions.key(
                                        AWSConfigConstants
                                                .HTTP_CLIENT_APACHE_TCP_KEEP_ALIVE_ENABLED)
                                .booleanType()
                                .defaultValue(true));

        this.httpClientApacheUseIdleConnectionReaperEnabled =
                properties.get(
                        ConfigOptions.key(
                                        AWSConfigConstants
                                                .HTTP_CLIENT_APACHE_USE_IDLE_CONNECTION_REAPER_ENABLED)
                                .booleanType()
                                .defaultValue(true));

        this.glueEndpoint =
                properties.get(
                        ConfigOptions.key(AWSConfigConstants.GLUE_CATALOG_ENDPOINT)
                                .stringType()
                                .noDefaultValue());
        this.glueCatalogId =
                properties.get(
                        ConfigOptions.key(AWSConfigConstants.GLUE_CATALOG_ID)
                                .stringType()
                                .noDefaultValue());

        this.glueCatalogSkipArchive =
                properties.get(
                        ConfigOptions.key(AWSConfigConstants.GLUE_CATALOG_SKIP_ARCHIVE)
                                .booleanType()
                                .noDefaultValue());

        this.glueCatalogSkipNameValidation =
                properties.get(
                        ConfigOptions.key(AWSConfigConstants.GLUE_CATALOG_SKIP_NAME_VALIDATION)
                                .booleanType()
                                .noDefaultValue());
    }

    /**
     * Configure the httpClient for a client according to the HttpClientType. The two supported
     * HttpClientTypes are urlconnection and apache
     *
     * <p>Sample usage:
     *
     * <pre>
     *     S3Client.builder().applyMutation(awsProperties::applyHttpClientConfigurations)
     * </pre>
     */
    public <T extends AwsSyncClientBuilder> void applyHttpClientConfigurations(T builder) {
        if (Strings.isNullOrEmpty(httpClientType)) {
            httpClientType = AWSConfigConstants.CLIENT_TYPE_URLCONNECTION;
        }
        switch (httpClientType) {
            case AWSConfigConstants.CLIENT_TYPE_URLCONNECTION:
                builder.httpClientBuilder(
                        UrlConnectionHttpClient.builder()
                                .applyMutation(this::configureUrlConnectionHttpClientBuilder));
                break;
            case AWSConfigConstants.CLIENT_TYPE_APACHE:
                builder.httpClientBuilder(
                        ApacheHttpClient.builder()
                                .applyMutation(this::configureApacheHttpClientBuilder));
                break;
            default:
                throw new IllegalArgumentException(
                        "Unrecognized HTTP client type " + httpClientType);
        }
    }

    @VisibleForTesting
    <T extends UrlConnectionHttpClient.Builder> void configureUrlConnectionHttpClientBuilder(
            T builder) {
        if (httpClientUrlConnectionConnectionTimeoutMs != null) {
            builder.connectionTimeout(
                    Duration.ofMillis(httpClientUrlConnectionConnectionTimeoutMs));
        }

        if (httpClientUrlConnectionSocketTimeoutMs != null) {
            builder.socketTimeout(Duration.ofMillis(httpClientUrlConnectionSocketTimeoutMs));
        }
    }

    @VisibleForTesting
    <T extends ApacheHttpClient.Builder> void configureApacheHttpClientBuilder(T builder) {
        if (httpClientApacheConnectionTimeoutMs != null) {
            builder.connectionTimeout(Duration.ofMillis(httpClientApacheConnectionTimeoutMs));
        }

        if (httpClientApacheSocketTimeoutMs != null) {
            builder.socketTimeout(Duration.ofMillis(httpClientApacheSocketTimeoutMs));
        }

        if (httpClientApacheConnectionAcquisitionTimeoutMs != null) {
            builder.connectionAcquisitionTimeout(
                    Duration.ofMillis(httpClientApacheConnectionAcquisitionTimeoutMs));
        }

        if (httpClientApacheConnectionMaxIdleTimeMs != null) {
            builder.connectionMaxIdleTime(
                    Duration.ofMillis(httpClientApacheConnectionMaxIdleTimeMs));
        }

        if (httpClientApacheConnectionTimeToLiveMs != null) {
            builder.connectionTimeToLive(Duration.ofMillis(httpClientApacheConnectionTimeToLiveMs));
        }

        if (httpClientApacheExpectContinueEnabled != null) {
            builder.expectContinueEnabled(httpClientApacheExpectContinueEnabled);
        }

        if (httpClientApacheMaxConnections != null) {
            builder.maxConnections(httpClientApacheMaxConnections);
        }

        if (httpClientApacheTcpKeepAliveEnabled != null) {
            builder.tcpKeepAlive(httpClientApacheTcpKeepAliveEnabled);
        }

        if (httpClientApacheUseIdleConnectionReaperEnabled != null) {
            builder.useIdleConnectionReaper(httpClientApacheUseIdleConnectionReaperEnabled);
        }
    }

    /**
     * Override the endpoint for a glue client.
     *
     * <p>Sample usage:
     *
     * <pre>
     *     GlueClient.builder().applyMutation(awsProperties::applyS3EndpointConfigurations)
     * </pre>
     */
    public <T extends GlueClientBuilder> void applyGlueEndpointConfigurations(T builder) {
        configureEndpoint(builder, glueEndpoint);
    }

    private <T extends SdkClientBuilder> void configureEndpoint(T builder, String endpoint) {
        if (endpoint != null) {
            builder.endpointOverride(URI.create(endpoint));
        }
    }

    /*
     * Getter for glue catalogId.
     */
    public String getGlueCatalogId() {
        return glueCatalogId;
    }
}
