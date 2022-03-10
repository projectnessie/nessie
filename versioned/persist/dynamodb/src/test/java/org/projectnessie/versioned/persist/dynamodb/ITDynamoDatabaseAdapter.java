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
package org.projectnessie.versioned.persist.dynamodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.randomHash;
import static org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext.NON_TRANSACTIONAL_OPERATION_CONTEXT;

import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry;
import org.projectnessie.versioned.persist.tests.AbstractDatabaseAdapterTest;
import org.projectnessie.versioned.persist.tests.LongerCommitTimeouts;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;

@NessieExternalDatabase(LocalDynamoTestConnectionProviderSource.class)
public class ITDynamoDatabaseAdapter extends AbstractDatabaseAdapterTest
    implements LongerCommitTimeouts {

  protected DynamoDatabaseAdapter implDatabaseAdapter() {
    return (DynamoDatabaseAdapter) databaseAdapter;
  }

  static Stream<Arguments> cleanUpCasBatch() {
    return Stream.of(
        Arguments.of(0, 0),
        Arguments.of(0, 1),
        Arguments.of(1, 0),
        Arguments.of(10, 13),
        Arguments.of(25, 0),
        Arguments.of(0, 25),
        Arguments.of(20, 20),
        Arguments.of(30, 30));
  }

  @ParameterizedTest
  @MethodSource("cleanUpCasBatch")
  public void cleanUpCasBatch(int numCommits, int numKeyLists) throws Exception {
    Hash globalId = randomHash();
    Set<Hash> branchCommits = new HashSet<>();
    Set<Hash> newKeyLists = new HashSet<>();
    Hash refLogId = randomHash();

    GlobalStateLogEntry globalStateLogEntry =
        GlobalStateLogEntry.newBuilder().setId(globalId.asBytes()).build();
    RefLogEntry refLogEntry = RefLogEntry.newBuilder().setRefLogId(refLogId.asBytes()).build();

    NonTransactionalOperationContext ctx = NON_TRANSACTIONAL_OPERATION_CONTEXT;
    implDatabaseAdapter().doWriteRefLog(ctx, refLogEntry);
    implDatabaseAdapter().doWriteGlobalCommit(ctx, globalStateLogEntry);
    for (int i = 0; i < numCommits; i++) {
      Hash commitId = randomHash();
      CommitLogEntry commit =
          CommitLogEntry.of(
              0L,
              commitId,
              0,
              Collections.emptyList(),
              ByteString.EMPTY,
              Collections.emptyList(),
              Collections.emptyList(),
              0,
              KeyList.of(Collections.emptyList()),
              Collections.emptyList());
      implDatabaseAdapter().doWriteIndividualCommit(ctx, commit);
      branchCommits.add(commitId);
    }
    for (int i = 0; i < numKeyLists; i++) {
      Hash keyListId = randomHash();
      KeyListEntity keyListEntity =
          KeyListEntity.of(keyListId, KeyList.of(Collections.emptyList()));
      implDatabaseAdapter().doWriteKeyListEntities(ctx, Collections.singletonList(keyListEntity));
      newKeyLists.add(keyListId);
    }

    assertThat(implDatabaseAdapter().doFetchFromGlobalLog(ctx, globalId)).isNotNull();
    assertThat(implDatabaseAdapter().doFetchFromRefLog(ctx, refLogId)).isNotNull();
    assertThat(branchCommits)
        .map(id -> implDatabaseAdapter().doFetchFromCommitLog(ctx, id))
        .allMatch(Objects::nonNull);
    assertThat(newKeyLists)
        .map(
            id ->
                implDatabaseAdapter()
                    .doFetchKeyLists(ctx, Collections.singletonList(id))
                    .collect(Collectors.toList()))
        .allMatch(l -> l.size() == 1)
        .extracting(l -> l.get(0))
        .allMatch(Objects::nonNull);

    implDatabaseAdapter().doCleanUpCommitCas(ctx, globalId, branchCommits, newKeyLists, refLogId);

    assertThat(implDatabaseAdapter().doFetchFromGlobalLog(ctx, globalId)).isNull();
    assertThat(implDatabaseAdapter().doFetchFromRefLog(ctx, refLogId)).isNull();
    assertThat(branchCommits)
        .map(id -> implDatabaseAdapter().doFetchFromCommitLog(ctx, id))
        .allMatch(Objects::isNull);
    assertThat(newKeyLists)
        .map(
            id ->
                implDatabaseAdapter()
                    .doFetchKeyLists(ctx, Collections.singletonList(id))
                    .collect(Collectors.toList()))
        .allMatch(l -> l.size() == 1)
        .extracting(l -> l.get(0))
        .allMatch(Objects::isNull);
  }
}
