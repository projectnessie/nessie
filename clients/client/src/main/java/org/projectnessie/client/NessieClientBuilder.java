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
package org.projectnessie.client;

import java.net.URI;
import java.util.function.Function;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.auth.NessieAuthentication;

/**
 * {@link NessieApi} builder interface.
 *
 * @param <IMPL> concrete nessie-client builder type
 */
public interface NessieClientBuilder<IMPL extends NessieClientBuilder<IMPL>> {

  /**
   * Same semantics as {@link #fromConfig(Function)}, uses the system properties.
   *
   * @return {@code this}
   * @see #fromConfig(Function)
   */
  IMPL fromSystemProperties();

  /**
   * Configure this HttpClientBuilder instance using a configuration object and standard Nessie
   * configuration keys defined by the constants defined in {@link NessieConfigConstants}.
   * Non-{@code null} values returned by the {@code configuration}-function will override previously
   * configured values.
   *
   * <p>Calls {@link #withAuthenticationFromConfig(Function)}.
   *
   * @param configuration The function that returns a configuration value for a configuration key.
   * @return {@code this}
   * @see #fromSystemProperties()
   * @see #withAuthenticationFromConfig(Function)
   */
  IMPL fromConfig(Function<String, String> configuration);

  /**
   * Configure only authentication in this HttpClientBuilder instance using a configuration object
   * and standard Nessie configuration keys defined by the constants defined in {@link
   * NessieConfigConstants}.
   *
   * @param configuration The function that returns a configuration value for a configuration key.
   * @return {@code this}
   * @see #fromConfig(Function) called by {@link #fromConfig(Function)}
   */
  IMPL withAuthenticationFromConfig(Function<String, String> configuration);

  /**
   * Sets the {@link NessieAuthentication} instance to be used.
   *
   * @param authentication authentication for this client
   * @return {@code this}
   */
  IMPL withAuthentication(NessieAuthentication authentication);

  /**
   * Set the Nessie server URI. A server URI must be configured.
   *
   * @param uri server URI
   * @return {@code this}
   */
  IMPL withUri(URI uri);

  /**
   * Convenience method for {@link #withUri(URI)} taking a string.
   *
   * @param uri server URI
   * @return {@code this}
   */
  default IMPL withUri(String uri) {
    return withUri(URI.create(uri));
  }

  /**
   * Builds a new {@link NessieApi}.
   *
   * @return A new {@link NessieApi}.
   */
  <API extends NessieApi> API build(Class<API> apiContract);
}
