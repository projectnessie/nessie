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
import org.projectnessie.client.http.HttpClientBuilder;

public interface NessieClient extends AutoCloseable {

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

    @Override
    @Deprecated
    public Builder withAuthType(AuthType authType) {
      return (Builder) super.withAuthType(authType);
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

    @Override
    @Deprecated
    public Builder withUsername(String username) {
      return (Builder) super.withUsername(username);
    }

    @Override
    @Deprecated
    public Builder withPassword(String password) {
      return (Builder) super.withPassword(password);
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
      return super.build();
    }
  }
}
