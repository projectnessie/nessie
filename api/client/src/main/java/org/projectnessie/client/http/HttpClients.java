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
package org.projectnessie.client.http;

import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class HttpClients {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpClients.class);

  private HttpClients() {}

  static HttpClient buildClient(boolean enableTracing, HttpClient.Builder clientBuilder) {
    if (enableTracing) {
      addTracing(clientBuilder);
    }
    clientBuilder.addResponseFilter(new NessieHttpResponseFilter());
    return clientBuilder.build();
  }

  private static void addTracing(HttpClient.Builder httpClient) {
    try {
      OpentelemetryTracing.addTracing(httpClient);
    } catch (NoClassDefFoundError e) {
      LOGGER.warn(
          "Failed to initialize tracing, the opentracing libraries are probably missing.", e);
    }
  }
}
