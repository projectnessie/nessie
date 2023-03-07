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
package org.projectnessie.quarkus.providers;

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.CASSANDRA;

import javax.enterprise.context.Dependent;
import org.projectnessie.versioned.storage.common.persist.Backend;

@StoreType(CASSANDRA)
@Dependent
public class CassandraBackendBuilder implements BackendBuilder {

  // @Inject CompletionStage<QuarkusCqlSession> client;

  //  @Inject
  //  @ConfigProperty(name = "quarkus.cassandra.keyspace")
  //  String keyspace;

  @Override
  public Backend buildBackend() {
    throw new UnsupportedOperationException(
        "The CASSANDRA version store type is currently unsupported, because there is no Quarkus 3 "
            + "compatible version of com.datastax.oss.quarkus:cassandra-quarkus-client.");
    // TODO   CassandraBackendFactory factory = new CassandraBackendFactory();
    //    try {
    //      CassandraBackendConfig c =
    //          CassandraBackendConfig.builder()
    //              .client(client.toCompletableFuture().get())
    //              .keyspace(keyspace)
    //              .build();
    //      return factory.buildBackend(c);
    //    } catch (InterruptedException | ExecutionException e) {
    //      throw new RuntimeException(e);
    //    }
  }
}
