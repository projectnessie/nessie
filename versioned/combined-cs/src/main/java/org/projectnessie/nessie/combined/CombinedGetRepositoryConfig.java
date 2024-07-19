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
package org.projectnessie.nessie.combined;

import static java.util.Objects.requireNonNull;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.projectnessie.api.v2.ConfigApi;
import org.projectnessie.client.api.GetRepositoryConfigBuilder;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.model.RepositoryConfigResponse;

final class CombinedGetRepositoryConfig implements GetRepositoryConfigBuilder {
  private final ConfigApi configApi;

  private final Set<RepositoryConfig.Type> types = new HashSet<>();

  CombinedGetRepositoryConfig(ConfigApi configApi) {
    this.configApi = configApi;
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

    try {
      return configApi.getRepositoryConfig(
          types.stream().map(RepositoryConfig.Type::name).collect(Collectors.toList()));
    } catch (RuntimeException e) {
      throw CombinedClientImpl.maybeWrapException(e);
    }
  }
}
