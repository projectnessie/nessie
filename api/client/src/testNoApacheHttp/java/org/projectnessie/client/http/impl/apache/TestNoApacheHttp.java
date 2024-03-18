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
package org.projectnessie.client.http.impl.apache;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.http.HttpClient;

@Tag("NoApacheHttp")
public class TestNoApacheHttp {
  @Test
  public void checkNoApacheHttpOnClasspath() {
    assertThatThrownBy(
            () -> Class.forName("org.apache.hc.client5.http.impl.classic.CloseableHttpClient"))
        .isInstanceOf(ClassNotFoundException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"apachehttp", "ApacheHttp"})
  public void apacheHttpNotAvailable(String clientName) {
    assertThatThrownBy(
            () ->
                HttpClient.builder()
                    .setBaseUri(URI.create("http://localhost:8080"))
                    .setObjectMapper(new ObjectMapper())
                    .setConnectionTimeoutMillis(15000)
                    .setReadTimeoutMillis(15000)
                    .setHttpClientName(clientName)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No HTTP client factory for name '" + clientName + "' found");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "JavaHttp", "URLConnection", "HTTP"})
  public void defaultClientWorks(String clientName) {
    assertThatCode(
            () ->
                HttpClient.builder()
                    .setBaseUri(URI.create("http://localhost:8080"))
                    .setObjectMapper(new ObjectMapper())
                    .setConnectionTimeoutMillis(15000)
                    .setReadTimeoutMillis(15000)
                    .setHttpClientName(clientName.isEmpty() ? null : clientName)
                    .build()
                    .close())
        .doesNotThrowAnyException();
  }
}
