/*
 * Copyright (C) 2020 Dremio
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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import javax.net.ssl.SSLContext;

/** Package-private HTTP configuration holder. */
final class HttpRuntimeConfig {
  private final URI baseUri;
  private final ObjectMapper mapper;
  private final int readTimeoutMillis;
  private final int connectionTimeoutMillis;
  private final SSLContext sslContext;
  private final List<RequestFilter> requestFilters;
  private final List<ResponseFilter> responseFilters;

  HttpRuntimeConfig(
      URI baseUri,
      ObjectMapper mapper,
      int readTimeoutMillis,
      int connectionTimeoutMillis,
      SSLContext sslContext,
      List<RequestFilter> requestFilters,
      List<ResponseFilter> responseFilters) {
    this.baseUri = Objects.requireNonNull(baseUri);
    if (!"http".equals(baseUri.getScheme()) && !"https".equals(baseUri.getScheme())) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot start http client. %s must be a valid http or https address", baseUri));
    }
    this.mapper = mapper;
    this.readTimeoutMillis = readTimeoutMillis;
    this.connectionTimeoutMillis = connectionTimeoutMillis;
    this.sslContext = sslContext;
    this.requestFilters = requestFilters;
    this.responseFilters = responseFilters;
  }

  URI getBaseUri() {
    return baseUri;
  }

  ObjectMapper getMapper() {
    return mapper;
  }

  int getReadTimeoutMillis() {
    return readTimeoutMillis;
  }

  int getConnectionTimeoutMillis() {
    return connectionTimeoutMillis;
  }

  SSLContext getSslContext() {
    return sslContext;
  }

  List<RequestFilter> getRequestFilters() {
    return requestFilters;
  }

  List<ResponseFilter> getResponseFilters() {
    return responseFilters;
  }
}
