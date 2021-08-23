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
import org.projectnessie.api.ConfigApi;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.http.HttpClientBuilder;

public interface NessieClient extends AutoCloseable {

  /**
   * Authentication types.
   *
   * @deprecated Replace with either direct usage of {@link
   *     NessieClientBuilder#withAuthentication(NessieAuthentication)} or via properties via {@link
   *     NessieClientBuilder#fromConfig(Function)}.
   */
  @Deprecated
  enum AuthType {
    AWS,
    BASIC,
    NONE
  }

  // Overridden to "remove 'throws Exception'"
  void close();

  String getOwner();

  String getRepo();

  URI getUri();

  /**
   * Tree-API scoped to the repository returned by {@link #getRepo()} for this {@link NessieClient}.
   */
  TreeApi getTreeApi();

  /**
   * Contents-API scoped to the repository returned by {@link #getRepo()} for this {@link
   * NessieClient}.
   */
  ContentsApi getContentsApi();

  ConfigApi getConfigApi();

  /**
   * Create a new {@link Builder} to configure a new {@link NessieClient}. Currently, the {@link
   * Builder} is only capable of building a {@link NessieClient} for HTTP, but that may change.
   *
   * @deprecated this method will be removed, use {@link HttpClientBuilder
   *     HttpClientBuilder.builder()} instead.
   */
  @Deprecated // TODO (forRemoval = true) - remove once Iceberg uses a Nessie version > 0.9.0
  static Builder builder() {
    return new Builder();
  }

  /**
   * Builder to configure a new {@link NessieClient}. Currently, the {@link Builder} is only capable
   * of building a {@link NessieClient} for HTTP, but that may change.
   *
   * @deprecated this inner class will be removed, replace with {@link NessieClientBuilder}.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated // TODO (forRemoval = true) - remove once Iceberg uses a Nessie version > 0.9.0
  class Builder extends HttpClientBuilder {
    private AuthType authType;
    private String username;
    private String password;

    @Override
    @Deprecated
    public Builder fromSystemProperties() {
      return (Builder) super.fromSystemProperties();
    }

    @Override
    @Deprecated
    public Builder fromConfig(Function<String, String> configuration) {
      return (Builder) super.fromConfig(configuration);
    }

    /**
     * Set the authentication type. Default is {@link AuthType#NONE}.
     *
     * @param authType new auth-type
     * @return {@code this}
     * @deprecated Use {@link NessieClientBuilder#withAuthentication(NessieAuthentication)} instead
     */
    @Deprecated
    public Builder withAuthType(AuthType authType) {
      this.authType = authType;
      return this;
    }

    @Override
    @Deprecated
    public Builder withUri(URI uri) {
      return (Builder) super.withUri(uri);
    }

    @Override
    @Deprecated
    public Builder withUri(String uri) {
      return (Builder) super.withUri(uri);
    }

    @Override
    @Deprecated
    public Builder withRepoOwner(String owner, String repo) {
      return (Builder) super.withRepoOwner(owner, repo);
    }

    /**
     * Set the username for {@link AuthType#BASIC} authentication.
     *
     * @param username username
     * @return {@code this}
     * @deprecated Use {@link NessieClientBuilder#withAuthentication(NessieAuthentication)} instead
     */
    @Deprecated
    public Builder withUsername(String username) {
      this.username = username;
      return this;
    }

    /**
     * Set the password for {@link AuthType#BASIC} authentication.
     *
     * @param password password
     * @return {@code this}
     * @deprecated Use {@link NessieClientBuilder#withAuthentication(NessieAuthentication)} instead
     */
    @Deprecated
    public Builder withPassword(String password) {
      this.password = password;
      return this;
    }

    @Override
    @Deprecated
    public Builder withTracing(boolean tracing) {
      return (Builder) super.withTracing(tracing);
    }

    @Override
    @Deprecated
    public Builder withReadTimeout(int readTimeoutMillis) {
      return (Builder) super.withReadTimeout(readTimeoutMillis);
    }

    @Override
    @Deprecated
    public Builder withConnectionTimeout(int connectionTimeoutMillis) {
      return (Builder) super.withConnectionTimeout(connectionTimeoutMillis);
    }

    @Override
    @Deprecated
    public NessieClient build() {
      AuthType auth =
          authType != null
              ? authType
              : (username != null && password != null) ? AuthType.BASIC : null;
      if (auth != null) {
        withAuthenticationFromConfig(
            s -> {
              switch (s) {
                case NessieConfigConstants.CONF_NESSIE_AUTH_TYPE:
                  return auth.name();
                case NessieConfigConstants.CONF_NESSIE_USERNAME:
                  return username;
                case NessieConfigConstants.CONF_NESSIE_PASSWORD:
                  return password;
                case NessieConfigConstants.CONF_NESSIE_AWS_REGION:
                  return null;
                default:
                  throw new IllegalArgumentException("Unexpected parameter key " + s);
              }
            });
      }

      return super.build();
    }
  }
}
