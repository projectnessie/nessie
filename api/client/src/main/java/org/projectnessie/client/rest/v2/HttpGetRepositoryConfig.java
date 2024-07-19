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
package org.projectnessie.client.rest.v2;

import static java.util.Objects.requireNonNull;

import java.util.HashSet;
import java.util.Set;
import org.projectnessie.client.api.GetRepositoryConfigBuilder;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.model.RepositoryConfigResponse;

final class HttpGetRepositoryConfig implements GetRepositoryConfigBuilder {
  private final HttpClient client;

  private final Set<RepositoryConfig.Type> types = new HashSet<>();

  HttpGetRepositoryConfig(HttpClient client) {
    this.client = client;
  }

  @Override
  public GetRepositoryConfigBuilder type(RepositoryConfig.Type type) {
    this.types.add(requireNonNull(type, "repository config type is null"));
    return this;
  }

  @Override
  public RepositoryConfigResponse get() {
    if (types.isEmpty()) {
      throw new IllegalStateException("repository config types to retrieve must be set");
    }

    HttpRequest req = client.newRequest().path("config/repository");
    types.stream().map(RepositoryConfig.Type::name).forEach(t -> req.queryParam("type", t));
    return req.get().readEntity(RepositoryConfigResponse.class);
  }
}
