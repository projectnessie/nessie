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

import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.NessieConfiguration;

public class NessieApiCompatibility {

  /**
   * Checks if the API version of the client is compatible with the server's.
   *
   * @param api the client API to check, can be either {@link NessieApiV1} or {@link NessieApiV2}
   *     currently.
   * @throws IllegalArgumentException if the API version is not compatible
   */
  public static void checkApiCompatibility(NessieApi api) {
    // First, check if the API version is supported by the server.
    int clientApiVersion = api instanceof NessieApiV2 ? 2 : 1;
    NessieConfiguration config = ((NessieApiV1) api).getConfig();
    int minServerApiVersion = config.getMinSupportedApiVersion();
    int maxServerApiVersion = config.getMaxSupportedApiVersion();
    String specVersion = config.getSpecVersion();
    if (minServerApiVersion > clientApiVersion || maxServerApiVersion < clientApiVersion) {
      throw new IllegalArgumentException(
          String.format(
              "API version %s is not supported by the server. "
                  + "The server supports API versions from %d to %d inclusive.",
              clientApiVersion, minServerApiVersion, maxServerApiVersion));
    }
    // Next, if the client is a V2 API, check that the URI prefix is indeed a V2 API root.
    // Spec version is only returned in V2+, so we can use that as a heuristic.
    // FIXME we need a better way to detect API vs URI prefix mismatches.
    int serverApiVersion = specVersion != null && !specVersion.isEmpty() ? 2 : 1;
    if (clientApiVersion != serverApiVersion) {
      throw new IllegalArgumentException(
          String.format(
              "Server supports API version %d but replied with API version %d. "
                  + "Is the client configured with the wrong URI prefix?",
              clientApiVersion, serverApiVersion));
    }
  }
}
