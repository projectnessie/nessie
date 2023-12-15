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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NessieApiCompatibilityFilter implements RequestFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(NessieApiCompatibilityFilter.class);
  private static final String MIN_API_VERSION = "minSupportedApiVersion";
  private static final String MAX_API_VERSION = "maxSupportedApiVersion";
  private static final String ACTUAL_API_VERSION = "actualApiVersion";

  private HttpClient.Builder builder;
  private final int clientApiVersion;
  private final AtomicBoolean checkDone = new AtomicBoolean(false);

  NessieApiCompatibilityFilter(HttpClient.Builder builder, int clientApiVersion) {
    this.builder = builder.copy().clearRequestFilters().clearResponseFilters();
    this.clientApiVersion = clientApiVersion;
  }

  @Override
  public void filter(RequestContext context) {
    if (checkDone.compareAndSet(false, true)) {
      try (HttpClient httpClient = builder.build()) {
        check(clientApiVersion, httpClient);
      } finally {
        builder = null;
      }
    }
  }

  /**
   * Checks if the API version of the client is compatible with the server's.
   *
   * @param clientApiVersion the API version of the client
   * @param httpClient the underlying HTTP client.
   * @throws NessieApiCompatibilityException if the API version is not compatible.
   */
  static void check(int clientApiVersion, HttpClient httpClient)
      throws NessieApiCompatibilityException {
    JsonNode config;
    try {
      config = httpClient.newRequest().path("config").get().readEntity(JsonNode.class);
    } catch (HttpClientException e) {
      LOGGER.warn(
          "API compatibility check: failed to contact config endpoint, proceeding without check: {}",
          e.toString());
      return;
    } catch (Exception e) {
      LOGGER.warn(
          "API compatibility check: failed to contact config endpoint, proceeding without check",
          e);
      return;
    }
    int minServerApiVersion =
        config.hasNonNull(MIN_API_VERSION) ? config.get(MIN_API_VERSION).asInt() : 1;
    int maxServerApiVersion = config.get(MAX_API_VERSION).asInt();
    // Note: assuming v1 here will cause troubles for Nessie versions between [0.47.0,0.59.0[,
    // since v2 only accurately reports its actual version starting with 0.59.0.
    int actualServerApiVersion =
        config.hasNonNull(ACTUAL_API_VERSION) ? config.get(ACTUAL_API_VERSION).asInt() : 1;
    if (clientApiVersion < minServerApiVersion
        || clientApiVersion > maxServerApiVersion
        || clientApiVersion != actualServerApiVersion) {
      throw new NessieApiCompatibilityException(
          clientApiVersion, minServerApiVersion, maxServerApiVersion, actualServerApiVersion);
    }
  }
}
