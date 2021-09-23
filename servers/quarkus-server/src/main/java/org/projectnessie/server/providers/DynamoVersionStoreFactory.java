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
package org.projectnessie.server.providers;

import static org.projectnessie.server.config.VersionStoreConfig.VersionStoreType.DYNAMO;

import java.util.Optional;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.dynamodb.DynamoDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.dynamodb.DynamoDatabaseClient;
import org.projectnessie.versioned.persist.dynamodb.ImmutableDynamoClientConfig;
import org.projectnessie.versioned.persist.store.PersistVersionStore;

/** DynamoDB version store factory. */
@StoreType(DYNAMO)
@Dependent
public class DynamoVersionStoreFactory implements VersionStoreFactory {
  private final String region;
  private final Optional<String> endpoint;

  /** Creates a factory for dynamodb version stores. */
  @Inject
  public DynamoVersionStoreFactory(
      @ConfigProperty(name = "quarkus.dynamodb.aws.region") String region,
      @ConfigProperty(name = "quarkus.dynamodb.endpoint-override") Optional<String> endpoint) {
    this.region = region;
    this.endpoint = endpoint;
  }

  @Override
  public <VALUE, METADATA, VALUE_TYPE extends Enum<VALUE_TYPE>>
      VersionStore<VALUE, METADATA, VALUE_TYPE> newStore(
          StoreWorker<VALUE, METADATA, VALUE_TYPE> worker, ServerConfig serverConfig) {

    DatabaseAdapter databaseAdapter =
        new DynamoDatabaseAdapterFactory()
            .newBuilder()
            .configure(
                c -> {
                  DynamoDatabaseClient client = new DynamoDatabaseClient();
                  ImmutableDynamoClientConfig.Builder config =
                      ImmutableDynamoClientConfig.builder().region(region);
                  endpoint.ifPresent(config::endpointURI);
                  client.configure(config.build());
                  client.initialize();
                  return c.withConnectionProvider(client);
                })
            .build();

    databaseAdapter.initializeRepo(serverConfig.getDefaultBranch());

    return new PersistVersionStore<>(databaseAdapter, worker);
  }
}
