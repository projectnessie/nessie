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

import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.server.config.converters.BackendType;
import org.projectnessie.server.config.converters.JGitStoreType;
import org.projectnessie.server.config.converters.VersionStoreType;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.dynamodb.DynamoStoreConfig;

import io.quarkus.arc.config.ConfigProperties;

/**
 * config object and subobjects. Each interface below populates 1 level of config entries in the config hierarchy.
 */
@Singleton
public class ApplicationConfig {

  private final BackendsConfig backendsConfig;
  private final VersionStoreConfig versionStoreConfig;
  private final VersionStoreJGitConfig versionStoreJGitConfig;
  private final VersionStoreDynamoConfig versionStoreDynamoConfig;

  /**
   * inject all configs form config providers.
   */
  @Inject
  public ApplicationConfig(BackendsConfig backendsConfig,
                           VersionStoreConfig versionStoreConfig,
                           VersionStoreJGitConfig versionStoreJGitConfig,
                           VersionStoreDynamoConfig versionStoreDynamoConfig) {

    this.backendsConfig = backendsConfig;
    this.versionStoreConfig = versionStoreConfig;
    this.versionStoreJGitConfig = versionStoreJGitConfig;
    this.versionStoreDynamoConfig = versionStoreDynamoConfig;
  }


  public BackendsConfig getBackendsConfig() {
    return backendsConfig;
  }

  public VersionStoreConfig getVersionStoreConfig() {
    return versionStoreConfig;
  }

  public VersionStoreJGitConfig getVersionStoreJGitConfig() {
    return versionStoreJGitConfig;
  }

  public VersionStoreDynamoConfig getVersionStoreDynamoConfig() {
    return versionStoreDynamoConfig;
  }

  @ConfigProperties(prefix = "nessie.server")
  public interface ServerConfigImpl extends ServerConfig {

    @ConfigProperty(name = "default-branch", defaultValue = "main")
    @Override
    String getDefaultBranch();

    @ConfigProperty(name = "send-stacktrace-to-client", defaultValue = "main")
    @Override
    boolean shouldSendstackTraceToAPIClient();

  }

  @ConfigProperties(prefix = "nessie.backends")
  public interface BackendsConfig {

    @ConfigProperty(name = "type", defaultValue = "INMEMORY")
    BackendType getBackendType();
  }

  @ConfigProperties(prefix = "nessie.version.store")
  public interface VersionStoreConfig {

    @ConfigProperty(name = "type", defaultValue = "INMEMORY")
    VersionStoreType getVersionStoreType();
  }


  @ConfigProperties(prefix = "nessie.version.store.jgit")
  public interface VersionStoreJGitConfig {

    @ConfigProperty(name = "type", defaultValue = "INMEMORY")
    JGitStoreType getJgitStoreType();

    @ConfigProperty(name = "directory")
    Optional<String> getJgitDirectory();
  }

  @ConfigProperties(prefix = "nessie.version.store.dynamo")
  public interface VersionStoreDynamoConfig {

    @ConfigProperty(name = "initialize", defaultValue = "false")
    boolean isDynamoInitialize();

    @ConfigProperty(defaultValue = DynamoStoreConfig.TABLE_PREFIX)
    String getTablePrefix();

    @ConfigProperty(name = "tracing", defaultValue = "true")
    boolean enableTracing();
  }
}
