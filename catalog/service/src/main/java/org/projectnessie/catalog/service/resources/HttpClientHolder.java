/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.service.resources;

import java.net.http.HttpClient;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class HttpClientHolder {

  private final HttpClient httpClient;
  private final Duration requestTimeout;

  @Inject
  public HttpClientHolder() {
    HttpClient.Builder clientBuilder =
        HttpClient.newBuilder()
            .connectTimeout(Duration.of(2, ChronoUnit.SECONDS))
            // Force HTTP 1.1, otherwise the response Content-Type gets lost
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL);

    // TODO SSL config
    //    if (config.getSslContext() != null) {
    //      clientBuilder.sslContext(config.getSslContext());
    //    }
    //    if (config.getSslParameters() != null) {
    //      clientBuilder.sslParameters(config.getSslParameters());
    //    }

    this.httpClient = clientBuilder.build();

    this.requestTimeout = Duration.of(30, ChronoUnit.SECONDS);
  }

  public HttpClient httpClient() {
    return httpClient;
  }

  public Duration requestTimeout() {
    return requestTimeout;
  }
}
