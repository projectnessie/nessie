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

import com.mongodb.client.MongoClient;
import io.quarkus.arc.Arc;
import io.quarkus.mongodb.runtime.MongoClientBeanUtil;
import io.quarkus.mongodb.runtime.MongoClients;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.versioned.persist.mongodb.MongoClientConfig;
import org.projectnessie.versioned.persist.mongodb.MongoDatabaseClient;

/** CDI bean for {@link MongoDatabaseClient}. */
@Singleton
public class QuarkusMongoDatabaseClient extends MongoDatabaseClient {
  @Inject
  public QuarkusMongoDatabaseClient(
      @ConfigProperty(name = "quarkus.mongodb.database") String databaseName) {
    MongoClients mongoClients = Arc.container().instance(MongoClients.class).get();
    MongoClient mongoClient =
        mongoClients.createMongoClient(MongoClientBeanUtil.DEFAULT_MONGOCLIENT_NAME);

    configure(MongoClientConfig.of(mongoClient).withDatabaseName(databaseName));
    initialize();
  }
}
