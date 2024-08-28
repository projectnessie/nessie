/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.quarkus.providers.storage;

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.CASSANDRA;

import com.datastax.oss.quarkus.runtime.api.session.QuarkusCqlSession;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.quarkus.config.QuarkusCassandraConfig;
import org.projectnessie.quarkus.providers.versionstore.StoreType;
import org.projectnessie.versioned.storage.cassandra.CassandraBackendConfig;
import org.projectnessie.versioned.storage.cassandra.CassandraBackendFactory;
import org.projectnessie.versioned.storage.common.persist.Backend;

@SuppressWarnings("deprecation")
@StoreType(CASSANDRA)
@Dependent
public class CassandraBackendBuilder implements BackendBuilder {

  @Inject CompletionStage<QuarkusCqlSession> client;

  @Inject
  @ConfigProperty(name = "quarkus.cassandra.keyspace")
  String keyspace;

  @Inject QuarkusCassandraConfig config;

  @Override
  public Backend buildBackend() {
    CassandraBackendFactory factory = new CassandraBackendFactory();
    try {
      CassandraBackendConfig c =
          CassandraBackendConfig.builder()
              .client(client.toCompletableFuture().get())
              .keyspace(keyspace)
              .ddlTimeout(config.ddlTimeout())
              .dmlTimeout(config.dmlTimeout())
              .build();
      return factory.buildBackend(c);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
