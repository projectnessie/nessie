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

package com.dremio.nessie.server.providers;

import java.util.Optional;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.MetricRegistry.Type;

import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.backend.dynamodb.DynamoDbBackend;
import com.dremio.nessie.backend.simple.InMemory;
import com.dremio.nessie.server.config.ApplicationConfig;
import com.dremio.nessie.server.config.converters.BackendType;
import com.dremio.nessie.server.config.converters.JGitStoreType;

import io.smallrye.metrics.MetricRegistries;

/**
 * Factory to generate backend based on server config.
 */
@Singleton
public class BackendProducer {

  private final ApplicationConfig config;

  @Inject
  public BackendProducer(ApplicationConfig config) {
    this.config = config;
  }

  @ConfigProperty(name = "quarkus.dynamodb.aws.region")
  String region;
  @ConfigProperty(name = "quarkus.dynamodb.endpoint-override")
  Optional<String> endpoint;


  /**
   * produce a backend based on config.
   */
  @Singleton
  @Produces
  public Backend producer() {
    MetricRegistry registry = MetricRegistries.get(Type.APPLICATION);

    if (!config.getVersionStoreJGitConfig().getJgitStoreType().equals(JGitStoreType.DYNAMO)) {
      return null;
    }
    if (config.getBackendsConfig().getBackendType().equals(BackendType.INMEMORY)) {
      return new InMemory.BackendFactory().create();
    }
    return new DynamoDbBackend.BackendFactory().create(region, endpoint.orElse(null), registry);
  }

}
