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
package org.projectnessie.services.impl;

import org.projectnessie.model.ImmutableNessieConfiguration;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.spi.ConfigService;
import org.projectnessie.versioned.RepositoryInformation;
import org.projectnessie.versioned.VersionStore;

public class ConfigApiImpl implements ConfigService {

  private final VersionStore store;
  private final ServerConfig config;
  private final int actualApiVersion;

  public ConfigApiImpl(ServerConfig config, VersionStore store, int actualApiVersion) {
    this.store = store;
    this.config = config;
    this.actualApiVersion = actualApiVersion;
  }

  @Override
  public NessieConfiguration getConfig() {
    RepositoryInformation info = store.getRepositoryInformation();
    String defaultBranch = info.getDefaultBranch();
    if (defaultBranch == null) {
      defaultBranch = this.config.getDefaultBranch();
    }
    return ImmutableNessieConfiguration.builder()
        .from(NessieConfiguration.getBuiltInConfig())
        .defaultBranch(this.config.getDefaultBranch())
        .actualApiVersion(actualApiVersion)
        .noAncestorHash(info.getNoAncestorHash())
        .repositoryCreationTimestamp(info.getRepositoryCreationTimestamp())
        .oldestPossibleCommitTimestamp(info.getOldestPossibleCommitTimestamp())
        .additionalProperties(info.getAdditionalProperties())
        .build();
  }
}
