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
package org.projectnessie.versioned.storage.mongodb;

import static com.mongodb.client.model.Filters.in;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.ID_REPO_PATH;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.TABLE_OBJS;
import static org.projectnessie.versioned.storage.mongodb.MongoDBConstants.TABLE_REFS;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import jakarta.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;

public class MongoDBBackend implements Backend {

  private final MongoDBBackendConfig config;
  private final MongoClient client;
  private final boolean closeClient;
  private MongoCollection<Document> refs;
  private MongoCollection<Document> objs;

  public MongoDBBackend(@Nonnull MongoDBBackendConfig config, boolean closeClient) {
    this.config = config;
    this.client = config.client();
    this.closeClient = closeClient;
  }

  @Nonnull
  MongoCollection<Document> refs() {
    return refs;
  }

  @Nonnull
  MongoCollection<Document> objs() {
    return objs;
  }

  private synchronized void initialize() {
    if (refs == null) {
      String databaseName = config.databaseName();
      MongoDatabase database =
          client.getDatabase(Objects.requireNonNull(databaseName, "Database name must be set"));

      refs = database.getCollection(TABLE_REFS);
      objs = database.getCollection(TABLE_OBJS);
    }
  }

  @Override
  @Nonnull
  public PersistFactory createFactory() {
    initialize();
    return new MongoDBPersistFactory(this);
  }

  @Override
  public synchronized void close() {
    if (closeClient) {
      client.close();
    }
  }

  @Override
  public Optional<String> setupSchema() {
    initialize();
    return Optional.of("database name: " + config.databaseName());
  }

  @Override
  public void eraseRepositories(Set<String> repositoryIds) {
    if (repositoryIds == null || repositoryIds.isEmpty()) {
      return;
    }

    Bson repoIdFilter = in(ID_REPO_PATH, repositoryIds);
    Stream.of(refs(), objs()).forEach(coll -> coll.deleteMany(repoIdFilter));
  }
}
