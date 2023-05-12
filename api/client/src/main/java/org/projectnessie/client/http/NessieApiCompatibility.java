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

import org.projectnessie.client.rest.NessieServiceException;
import org.projectnessie.model.NessieConfiguration;

public class NessieApiCompatibility {

  /**
   * Checks if the API version of the client is compatible with the server's.
   *
   * @param clientApiVersion the API version of the client
   * @param httpClient the underlying HTTP client.
   * @throws NessieApiCompatibilityException if the API version is not compatible.
   */
  public static void check(int clientApiVersion, HttpClient httpClient)
      throws NessieApiCompatibilityException {
    NessieConfiguration config = fetchConfig(httpClient);
    int minServerApiVersion = config.getMinSupportedApiVersion();
    int maxServerApiVersion = config.getMaxSupportedApiVersion();
    if (clientApiVersion < minServerApiVersion || clientApiVersion > maxServerApiVersion) {
      throw new NessieApiCompatibilityException(
          clientApiVersion, minServerApiVersion, maxServerApiVersion);
    }
    int actualServerApiVersion = fetchActualServerApiVersion(httpClient, config);
    if (clientApiVersion != actualServerApiVersion) {
      throw new NessieApiCompatibilityException(
          clientApiVersion, minServerApiVersion, maxServerApiVersion, actualServerApiVersion);
    }
  }

  private static NessieConfiguration fetchConfig(HttpClient httpClient) {
    return httpClient.newRequest().path("config").get().readEntity(NessieConfiguration.class);
  }

  private static int fetchActualServerApiVersion(
      HttpClient httpClient, NessieConfiguration config) {
    try {
      httpClient
          .newRequest()
          .path("trees/tree/{branch}")
          .resolveTemplate("branch", config.getDefaultBranch())
          .get();
      return 1;
    } catch (NessieServiceException e) {
      // In theory, we could test if the status code is 404; but unfortunately on Jersey,
      // the 404 error arrives wrapped in a status code 500.
      return 2;
    }
  }
}
