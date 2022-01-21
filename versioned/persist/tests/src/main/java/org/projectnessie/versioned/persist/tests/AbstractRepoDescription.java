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
package org.projectnessie.versioned.persist.tests;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.RepoDescription;

/** Verifies handling of repo-description in the database-adapters. */
public abstract class AbstractRepoDescription {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractRepoDescription(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  @Test
  void emptyIsDefault() throws Exception {
    assertThat(databaseAdapter.fetchRepositoryDescription()).isEqualTo(RepoDescription.DEFAULT);
    databaseAdapter.updateRepositoryDescription(
        d -> {
          assertThat(d).isEqualTo(RepoDescription.DEFAULT);
          return null;
        });
  }

  @Test
  void updates() throws Exception {
    RepoDescription update1 =
        RepoDescription.builder()
            .repoVersion(42)
            .putProperties("a", "b")
            .putProperties("c", "d")
            .build();
    RepoDescription update2 =
        RepoDescription.builder()
            .repoVersion(666)
            .putProperties("a", "e")
            .putProperties("c", "f")
            .build();

    databaseAdapter.updateRepositoryDescription(
        d -> {
          assertThat(d).isEqualTo(RepoDescription.DEFAULT);
          return update1;
        });
    assertThat(databaseAdapter.fetchRepositoryDescription()).isEqualTo(update1);

    databaseAdapter.updateRepositoryDescription(
        d -> {
          assertThat(d).isEqualTo(update1);
          return update2;
        });
    assertThat(databaseAdapter.fetchRepositoryDescription()).isEqualTo(update2);
  }
}
