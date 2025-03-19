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
package org.projectnessie.client.http.impl.jdk11;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.JRE;
import org.projectnessie.client.http.BaseTestHttpClient;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;

public class TestJavaHttpClient extends BaseTestHttpClient {

  @BeforeAll
  public static void checkJava11() {
    assumeThat(JRE.currentJre()).matches(jre -> jre.ordinal() >= JRE.JAVA_11.ordinal());
  }

  @Override
  protected HttpClient createClient(URI baseUri, Consumer<HttpClient.Builder> customizer) {
    HttpClient.Builder b =
        HttpClient.builder()
            .setBaseUri(baseUri)
            .setObjectMapper(MAPPER)
            .setConnectionTimeoutMillis(15000)
            .setReadTimeoutMillis(15000)
            .setHttpClientName("JavaHttp");
    customizer.accept(b);
    return b.build();
  }

  @Override
  protected boolean supportsHttp2() {
    return true;
  }

  @Test
  void testCloseJava11Client() {
    HttpRuntimeConfig config = mock(HttpRuntimeConfig.class);
    when(config.getConnectionTimeoutMillis()).thenReturn(100);
    HttpClient client = new JavaHttpClient(config);
    client.close();
    verify(config).close();
  }
}
