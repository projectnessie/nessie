/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.spanner;

import static java.util.Arrays.asList;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.CREATE_TABLE_OBJS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.CREATE_TABLE_REFS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.ERASE_OBJS;
import static org.projectnessie.versioned.storage.spanner.SpannerConstants.ERASE_REFS;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;

final class SpannerBackend implements Backend {

  private final Spanner spanner;
  private final boolean closeSpanner;
  private final DatabaseId databaseId;
  private final DatabaseClient client;

  SpannerBackend(
      @Nonnull @jakarta.annotation.Nonnull Spanner spanner,
      boolean closeSpanner,
      @Nonnull @jakarta.annotation.Nonnull DatabaseId databaseId) {
    this.spanner = spanner;
    this.closeSpanner = closeSpanner;
    this.databaseId = databaseId;
    this.client = spanner.getDatabaseClient(databaseId);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  DatabaseClient client() {
    return client;
  }

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public PersistFactory createFactory() {
    return new SpannerPersistFactory(this);
  }

  @Override
  public void close() {
    if (closeSpanner) {
      spanner.close();
    }
  }

  @Override
  public void setupSchema() {
    DatabaseAdminClient adminClient = spanner.getDatabaseAdminClient();
    Database database;
    List<String> ddl = asList(CREATE_TABLE_REFS, CREATE_TABLE_OBJS);
    try {
      database =
          adminClient.getDatabase(
              databaseId.getInstanceId().getInstance(), databaseId.getDatabase());
      database.updateDdl(ddl, "update_ddl").get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(
          "Failed to check/setup schema in Spanner database " + databaseId, e);
    }
  }

  @Override
  public String configInfo() {
    return "Spanner(databaseId=" + databaseId + ")";
  }

  @Override
  public void eraseRepositories(Set<String> repositoryIds) {
    if (repositoryIds == null || repositoryIds.isEmpty()) {
      return;
    }

    List<String> repoIds = new ArrayList<>(repositoryIds);

    DatabaseClient c = client();

    c.readWriteTransaction()
        .run(
            tx ->
                tx.executeUpdate(
                    Statement.newBuilder(ERASE_REFS)
                        .bind("RepoIDs")
                        .toStringArray(repoIds)
                        .build()));
    c.readWriteTransaction()
        .run(
            tx ->
                tx.executeUpdate(
                    Statement.newBuilder(ERASE_OBJS)
                        .bind("RepoIDs")
                        .toStringArray(repoIds)
                        .build()));
  }
}
