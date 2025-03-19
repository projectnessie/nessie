/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.client.http.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.http.HttpClient;

public class TestHttpClients {
  @SuppressWarnings("deprecation")
  @ParameterizedTest
  @MethodSource
  public void clientByName(
      String clientName, String expectedClassName, boolean forceUrlConnClient) {
    try (HttpClient client =
        HttpClient.builder()
            .setBaseUri(URI.create("http://localhost:8080"))
            .setObjectMapper(new ObjectMapper())
            .setConnectionTimeoutMillis(15000)
            .setReadTimeoutMillis(15000)
            .setHttpClientName(clientName)
            .setForceUrlConnectionClient(forceUrlConnClient)
            .build()) {
      assertThat(client.getClass().getName()).isEqualTo(expectedClassName);
    }
  }

  @SuppressWarnings("resource")
  @Test
  public void unknownClient() {
    assertThatThrownBy(
            () ->
                HttpClient.builder()
                    .setBaseUri(URI.create("http://localhost:8080"))
                    .setObjectMapper(new ObjectMapper())
                    .setConnectionTimeoutMillis(15000)
                    .setReadTimeoutMillis(15000)
                    .setHttpClientName("blahblah")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No HTTP client factory for name 'blahblah' found");
  }

  static Stream<Arguments> clientByName() {

    if (JRE.currentJre().ordinal() < JRE.JAVA_11.ordinal()) {
      return Stream.of(
          arguments(
              "apachehttp", "org.projectnessie.client.http.impl.apache.ApacheHttpClient", false),
          arguments(
              "ApacheHttp", "org.projectnessie.client.http.impl.apache.ApacheHttpClient", false),
          arguments("http", "org.projectnessie.client.http.impl.jdk8.UrlConnectionClient", false),
          arguments("HTTP", "org.projectnessie.client.http.impl.jdk8.UrlConnectionClient", false),
          arguments(null, "org.projectnessie.client.http.impl.jdk8.UrlConnectionClient", false),
          arguments(
              "UrlConnection",
              "org.projectnessie.client.http.impl.jdk8.UrlConnectionClient",
              false),
          arguments(
              "urlconnection",
              "org.projectnessie.client.http.impl.jdk8.UrlConnectionClient",
              false),
          arguments(
              "urlconnection", "org.projectnessie.client.http.impl.jdk8.UrlConnectionClient", true),
          arguments(null, "org.projectnessie.client.http.impl.jdk8.UrlConnectionClient", true),
          arguments(
              "apachehttp", "org.projectnessie.client.http.impl.jdk8.UrlConnectionClient", true));
    }

    return Stream.of(
        arguments(
            "apachehttp", "org.projectnessie.client.http.impl.apache.ApacheHttpClient", false),
        arguments(
            "ApacheHttp", "org.projectnessie.client.http.impl.apache.ApacheHttpClient", false),
        arguments(
            "UrlConnection", "org.projectnessie.client.http.impl.jdk8.UrlConnectionClient", false),
        arguments(
            "urlconnection", "org.projectnessie.client.http.impl.jdk8.UrlConnectionClient", false),
        arguments(
            "urlconnection", "org.projectnessie.client.http.impl.jdk8.UrlConnectionClient", true),
        arguments(null, "org.projectnessie.client.http.impl.jdk8.UrlConnectionClient", true),
        arguments(null, "org.projectnessie.client.http.impl.jdk11.JavaHttpClient", false),
        arguments("http", "org.projectnessie.client.http.impl.jdk11.JavaHttpClient", false),
        arguments("HTTP", "org.projectnessie.client.http.impl.jdk11.JavaHttpClient", false),
        arguments("JavaHttp", "org.projectnessie.client.http.impl.jdk11.JavaHttpClient", false),
        arguments("javahttp", "org.projectnessie.client.http.impl.jdk11.JavaHttpClient", false));
  }
}
