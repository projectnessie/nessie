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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_PASSWORD;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_TRACING;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_USERNAME;

import java.net.URI;
import java.util.function.Function;

import org.projectnessie.api.ConfigApi;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.http.HttpClientTimeoutException;

public interface NessieClient extends AutoCloseable {

  enum AuthType {
    AWS,
    BASIC,
    NONE
  }

  // Overridden to "remove 'throws Exception'"
  void close();

  TreeApi getTreeApi();

  ContentsApi getContentsApi();

  ConfigApi getConfigApi();

  /**
   * Create a new {@link Builder} to configure a new {@link NessieClient}.
   * Currently, the {@link Builder} is only capable of building a {@link NessieClient} for HTTP,
   * but that may change.
   */
  static Builder builder() {
    return new Builder();
  }

  /**
   * Builder to configure a new {@link NessieClient}. Currently, the {@link Builder} is only
   * capable of building a {@link NessieClient} for HTTP, but that may change.
   */
  class Builder {
    private AuthType authType = AuthType.NONE;
    private URI uri;
    private String username;
    private String password;
    private boolean tracing;
    private int readTimeout = 10;

    /**
     * Same semantics as {@link #fromConfig(Function)}, uses the system properties.
     * @return {@code this}
     * @see #fromConfig(Function)
     */
    public Builder fromSystemProperties() {
      return fromConfig(System::getProperty);
    }

    /**
     * Configure this builder instance using a configuration object and standard Nessie
     * configuration keys defined by the constants defined in {@link NessieConfigConstants}.
     * Non-{@code null} values returned by the {@code configuration}-function will override
     * previously configured values.
     * @param configuration The function that returns a configuration value for a configuration key.
     * @return {@code this}
     * @see #fromSystemProperties()
     */
    public Builder fromConfig(Function<String, String> configuration) {
      String uri = configuration.apply(CONF_NESSIE_URI);
      if (uri != null) {
        this.uri = URI.create(uri);
      }
      String username = configuration.apply(CONF_NESSIE_USERNAME);
      if (username != null) {
        this.username = username;
      }
      String password = configuration.apply(CONF_NESSIE_PASSWORD);
      if (password != null) {
        this.password = password;
      }
      String authType = configuration.apply(CONF_NESSIE_AUTH_TYPE);
      if (authType != null) {
        this.authType = AuthType.valueOf(authType);
      }
      String tracing = configuration.apply(CONF_NESSIE_TRACING);
      if (tracing != null) {
        this.tracing = Boolean.parseBoolean(tracing);
      }
      return this;
    }

    /**
     * Set the authentication type. Default is {@link AuthType#NONE}.
     * @param authType new auth-type
     * @return {@code this}
     */
    public Builder withAuthType(AuthType authType) {
      this.authType = authType;
      return this;
    }

    /**
     * Set the Nessie server URI. A server URI must be configured.
     * @param uri server URI
     * @return {@code this}
     */
    public Builder withUri(URI uri) {
      this.uri = uri;
      return this;
    }

    /**
     * Convenience method for {@link #withUri(URI)} taking a string.
     * @param uri server URI
     * @return {@code this}
     */
    public Builder withUri(String uri) {
      return withUri(URI.create(uri));
    }

    /**
     * Set the username for {@link AuthType#BASIC} authentication.
     * @param username username
     * @return {@code this}
     */
    public Builder withUsername(String username) {
      this.username = username;
      return this;
    }

    /**
     * Set the password for {@link AuthType#BASIC} authentication.
     * @param password password
     * @return {@code this}
     */
    public Builder withPassword(String password) {
      this.password = password;
      return this;
    }

    /**
     * Whether to enable adding the HTTP headers of an active OpenTracing span to all
     * Nessie requests. If enabled, the OpenTracing dependencies must be present at runtime.
     * @param tracing {@code true} to enable passing HTTP headers for active tracing spans.
     * @return {@code this}
     */
    public Builder withTracing(boolean tracing) {
      this.tracing = tracing;
      return this;
    }

    /**
     * Set the read timeout in seconds for this client. Timeout will throw {@link HttpClientTimeoutException}. If unset defaults to 10s
     * @param readTimeout number of seconds to wait for a response from server.
     * @return {@code this}
     */
    public Builder withReadTimeout(int readTimeout) {
      this.readTimeout = readTimeout;
      return this;
    }

    /**
     * Build a new {@link NessieClient}.
     * @return new {@link NessieClient}
     */
    public NessieClient build() {
      return new NessieHttpClient(authType, uri, username, password, tracing, readTimeout);
    }
  }
}
