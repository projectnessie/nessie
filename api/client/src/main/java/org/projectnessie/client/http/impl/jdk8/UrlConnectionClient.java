/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.client.http.impl.jdk8;

import java.net.URI;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;

/**
 * Simple Http client to make REST calls.
 *
 * <p>Assumptions: - always send/receive JSON - set headers accordingly by default - very simple
 * interactions w/ API - no cookies - no caching of connections. Could be slow
 */
final class UrlConnectionClient implements HttpClient {

  public static final String UNSUPPORTED_CONFIG_MESSAGE =
      "Nessie's URLConnection client does not support the configuration options to specify SSL parameters. Switch to Java 11 instead.";

  private final HttpRuntimeConfig config;

  /**
   * Construct an HTTP client with a universal Accept header.
   *
   * @param config http client configuration
   */
  UrlConnectionClient(HttpRuntimeConfig config) {
    this.config = config;
    if (config.getSslParameters() != null) {
      throw new IllegalArgumentException(UNSUPPORTED_CONFIG_MESSAGE);
    }
  }

  @Override
  public HttpRequest newRequest(URI baseUri) {
    return new UrlConnectionRequest(config, baseUri);
  }

  @Override
  public URI getBaseUri() {
    return config.getBaseUri();
  }

  @Override
  public void close() {
    config.close();
  }
}
