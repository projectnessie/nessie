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
package org.projectnessie.quarkus.cli;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.mongodb.ImmutableMongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.LocalMongoResource;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseClient;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;

/**
 * A JUnit5 extension that sets up the execution environment for Nessie CLI tests.
 *
 * <p>MongoDB storage is used.
 *
 * <p>A {@link DatabaseAdapter} instance is created and injected into tests for manipulating the
 * test Nessie repository
 *
 * <p>The test Nessie repository is erased and re-created for each test case.
 */
public class NessieCliTestExtension
    implements BeforeAllCallback, BeforeEachCallback, ParameterResolver {

  private static final Namespace NAMESPACE = Namespace.create(NessieCliTestExtension.class);
  private static final String MONGO_KEY = "mongo";
  private static final String ADAPTER_KEY = "adapter";

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    LocalMongoResource mongo = getOrCreateMongo(context);

    // Quarkus runtime will pick up relevant values from java system properties.
    System.setProperty("quarkus.mongodb.connection-string", mongo.getConnectionString());
    System.setProperty("quarkus.mongodb.database", mongo.getDatabaseName());
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    // Use one DatabaseAdapter instance for all tests (adapters are stateless)
    DatabaseAdapter adapter = getOrCreateAdapter(context);
    // ... but reset the repo for each test.
    adapter.eraseRepo();
    adapter.initializeRepo("main");
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    return parameterContext.getParameter().getType().isAssignableFrom(DatabaseAdapter.class);
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    return getOrCreateAdapter(context);
  }

  private DatabaseAdapter getOrCreateAdapter(ExtensionContext context) {
    return context
        .getRoot()
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(ADAPTER_KEY, key -> createAdapter(context), DatabaseAdapter.class);
  }

  private LocalMongoResource getOrCreateMongo(ExtensionContext context) {
    // Maintain one MongoDB instance for all tests (store it in the root context).
    return context
        .getRoot()
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(MONGO_KEY, key -> new LocalMongoResource(), LocalMongoResource.class);
  }

  private DatabaseAdapter createAdapter(ExtensionContext context) {

    LocalMongoResource mongo = getOrCreateMongo(context);

    MongoDatabaseClient client = new MongoDatabaseClient();
    client.configure(
        ImmutableMongoClientConfig.builder()
            .connectionString(mongo.getConnectionString())
            .databaseName(mongo.getDatabaseName())
            .build());
    client.initialize();

    return new MongoDatabaseAdapterFactory()
        .newBuilder()
        .withConfig(
            ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder()
                .repositoryId(BaseConfigProfile.TEST_REPO_ID)
                .build())
        .withConnector(client)
        .build();
  }
}
