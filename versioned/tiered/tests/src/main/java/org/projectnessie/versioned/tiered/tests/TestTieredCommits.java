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
package org.projectnessie.versioned.tiered.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.StringSerializer.TestEnum;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapter;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfiguration;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterFactory.Builder;
import org.projectnessie.versioned.tiered.impl.TieredVersionStore;

public class TestTieredCommits {
  protected DatabaseAdapter databaseAdapter;
  protected TieredVersionStore<String, String, String, TestEnum> versionStore;

  @BeforeEach
  public void loadDatabaseAdapter() throws Exception {
    DatabaseAdapterFactory factory =
        ServiceLoader.load(DatabaseAdapterFactory.class).iterator().next();

    Builder builder = factory.newBuilder();

    configureDatabaseAdapter(builder.getConfig());

    StoreWorker<String, String, String, TestEnum> storeWorker =
        StoreWorker.of(
            StringSerializer.getInstanceNoSpy(),
            StringSerializer.getInstanceNoSpy(),
            StringSerializer.getInstanceNoSpy(),
            (value, state) -> state + '|' + value.substring(value.indexOf('|') + 1));

    databaseAdapter = builder.build();
    databaseAdapter.initializeRepo();

    versionStore = new TieredVersionStore<>(databaseAdapter, storeWorker);
  }

  protected void configureDatabaseAdapter(DatabaseAdapterConfiguration config) throws Exception {}

  @AfterEach
  void closeDatabaseAdapter() throws Exception {
    databaseAdapter.close();
    postCloseDatabaseAdapter();
  }

  protected void postCloseDatabaseAdapter() {}

  @Test
  void createBranch() throws Exception {
    BranchName branch = BranchName.of("main");
    BranchName create = BranchName.of("createBranch");

    Hash mainHash = databaseAdapter.toHash(branch);

    assertThatThrownBy(() -> databaseAdapter.toHash(create))
        .isInstanceOf(ReferenceNotFoundException.class);

    Hash createHash = databaseAdapter.create(create, Optional.empty(), Optional.empty());
    assertThat(createHash).isEqualTo(mainHash);

    assertThatThrownBy(() -> databaseAdapter.create(create, Optional.empty(), Optional.empty()))
        .isInstanceOf(ReferenceAlreadyExistsException.class);

    assertThatThrownBy(
            () -> databaseAdapter.delete(create, Optional.of(Hash.of("dead00004242fee18eef"))))
        .isInstanceOf(ReferenceConflictException.class);

    databaseAdapter.delete(create, Optional.of(createHash));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 19, 20, 21, 39, 40, 41, 49, 50, 51, 255, 256, 257, 1000})
  // Note: 1000 commits is quite the max that in-JVM H2 database can handle
  void manyCommits(int numCommits) throws Exception {
    BranchName branch = BranchName.of("manyCommits-" + numCommits);

    Hash head = databaseAdapter.create(branch, Optional.empty(), Optional.empty());

    for (int i = 0; i < numCommits; i++) {
      String newState = "state for #" + i + " of " + numCommits;
      String value = newState + "|value for #" + i + " of " + numCommits;
      List<Operation<String, String>> ops =
          Collections.singletonList(
              Put.of(
                  Key.of("many", "commits", Integer.toString(numCommits)),
                  value,
                  newState,
                  i > 0 ? "state for #" + (i - 1) + " of " + numCommits : null));
      versionStore.commit(branch, Optional.empty(), "commit #" + i + " of " + numCommits, ops);
    }

    try (Stream<WithHash<String>> s =
        versionStore.getCommits(branch, Optional.empty(), Optional.of(head))) {
      assertThat(s.count()).isEqualTo(numCommits);
    }

    databaseAdapter.delete(branch, Optional.empty());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3, 5})
  void commit(int tablesPerCommit) throws Exception {
    BranchName branch = BranchName.of("main");

    ArrayList<Key> keys = new ArrayList<>(tablesPerCommit);
    List<Operation<String, String>> operations = new ArrayList<>(tablesPerCommit);
    for (int i = 0; i < tablesPerCommit; i++) {
      Key key = Key.of("my", "table", "num" + i);
      keys.add(key);

      operations.add(Put.of(key, "0|initial commit contents", "0", null));
    }

    Hash head = versionStore.commit(branch, Optional.empty(), "initial commit meta", operations);
    for (int commitNum = 0; commitNum < 3; commitNum++) {
      List<Optional<String>> contents = versionStore.getValues(branch, keys);

      operations = new ArrayList<>(tablesPerCommit);
      for (int i = 0; i < tablesPerCommit; i++) {
        String value = contents.get(i).orElseThrow(RuntimeException::new);
        String currentState = value.split("\\|")[0];
        operations.add(
            Put.of(
                keys.get(i),
                "commit value",
                Integer.toString(Integer.parseInt(currentState) + 1),
                currentState));
      }

      Hash newHead = versionStore.commit(branch, Optional.empty(), "commit meta data", operations);
      assertThat(newHead).isNotEqualTo(head);
      head = newHead;
    }
  }
}
