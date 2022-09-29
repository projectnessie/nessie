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
package org.projectnessie.client.http.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.immutables.value.Value;
import org.projectnessie.client.http.RequestFilter;
import org.projectnessie.client.http.ResponseFilter;

/** Implementation specific HTTP configuration holder. */
@Value.Immutable
public interface HttpRuntimeConfig {

  static ImmutableHttpRuntimeConfig.Builder builder() {
    return ImmutableHttpRuntimeConfig.builder();
  }

  @Value.Check
  default void check() {
    URI baseUri = getBaseUri();
    if (!"http".equals(baseUri.getScheme()) && !"https".equals(baseUri.getScheme())) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot start http client. %s must be a valid http or https address", baseUri));
    }
  }

  URI getBaseUri();

  ObjectMapper getMapper();

  int getReadTimeoutMillis();

  int getConnectionTimeoutMillis();

  boolean isDisableCompression();

  SSLContext getSslContext();

  List<RequestFilter> getRequestFilters();

  List<ResponseFilter> getResponseFilters();
}
