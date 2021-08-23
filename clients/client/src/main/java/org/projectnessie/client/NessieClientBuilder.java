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
import org.projectnessie.client.NessieClient.AuthType;

/**
 * {@link NessieClient} builder interface.
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
   * @param configuration The function that returns a configuration value for a configuration key.
   * @return {@code this}
   * @see #fromSystemProperties()
   */
  IMPL fromConfig(Function<String, String> configuration);

  /**
   * Set the authentication type. Default is {@link AuthType#NONE}.
   *
   * @param authType new auth-type
   * @return {@code this}
   */
  IMPL withAuthType(AuthType authType);

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
  IMPL withUri(String uri);

  /**
   * Set the repository owner + ID within the Nessie instance.
   *
   * <p>{@link NessieClient#getContentsApi()} and {@link NessieClient#getTreeApi()} are scoped to
   * the repository specified by this option.
   *
   * @param owner repository owner
   * @param repo repository ID
   * @return {@code this}
   */
  IMPL withRepoOwner(String owner, String repo);

  /**
   * Set the username for {@link AuthType#BASIC} authentication.
   *
   * @param username username
   * @return {@code this}
   */
  IMPL withUsername(String username);

  /**
   * Set the password for {@link AuthType#BASIC} authentication.
   *
   * @param password password
   * @return {@code this}
   */
  IMPL withPassword(String password);

  /**
   * Builds a new {@link NessieClient}.
   *
   * @return A new {@link NessieClient}.
   */
  NessieClient build();
}
