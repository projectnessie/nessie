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
package org.projectnessie.server.config;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.server.providers.DynamoVersionStoreFactory;
import org.projectnessie.server.providers.InMemoryVersionStoreFactory;
import org.projectnessie.server.providers.JGitVersionStoreFactory;
import org.projectnessie.server.providers.VersionStoreFactory;

import io.quarkus.arc.config.ConfigProperties;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Version store configuration.
 */
@ConfigProperties(prefix = "nessie.version.store")
public interface VersionStoreConfig {

  @RegisterForReflection
  public enum VersionStoreType {
    DYNAMO(DynamoVersionStoreFactory.class),
    INMEMORY(InMemoryVersionStoreFactory.class),
    JGIT(JGitVersionStoreFactory.class);

    private final Class<? extends VersionStoreFactory> factoryClass;
    private VersionStoreType(Class<? extends VersionStoreFactory> factoryClass) {
      this.factoryClass = factoryClass;
    }

    public Class<? extends VersionStoreFactory> getFactoryClass() {
      return factoryClass;
    }
  }

  @ConfigProperty(name = "type", defaultValue = "INMEMORY")
  VersionStoreType getVersionStoreType();
}
