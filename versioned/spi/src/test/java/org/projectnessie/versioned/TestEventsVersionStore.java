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
package org.projectnessie.versioned;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.projectnessie.model.IdentifiedContentKey.identifiedContentKeyFromContent;
import static org.projectnessie.versioned.ContentResult.contentResult;
import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.VersionStore.CommitValidator;
import org.projectnessie.versioned.VersionStore.MergeOp;
import org.projectnessie.versioned.VersionStore.TransplantOp;
import org.projectnessie.versioned.paging.PaginationIterator;

@ExtendWith(MockitoExtension.class)
class TestEventsVersionStore {

  @Mock VersionStore delegate;
  @Mock Consumer<Result> sink;

  @Mock MetadataRewriter<CommitMeta> metadataRewriter;
  @Mock PaginationIterator<ReferenceInfo<CommitMeta>> iteratorInfos;
  @Mock PaginationIterator<Commit> iteratorCommits;
  @Mock PaginationIterator<KeyEntry> iteratorKeyEntries;
  @Mock PaginationIterator<Diff> iteratorDiffs;

  BranchName branch1 = BranchName.of("branch1");
  BranchName branch2 = BranchName.of("branch2");
  Hash hash1 = Hash.of("1234");
  Hash hash2 = Hash.of("5678");
  CommitMeta commitMeta = CommitMeta.fromMessage("irrelevant");
  ContentKey key1 = ContentKey.of("foo.bar.table1");
  ContentKey key2 = ContentKey.of("foo.bar.table2");
  IcebergTable table1 = IcebergTable.of("somewhere", 42, 42, 42, 42, "table1");
  IcebergTable table2 = IcebergTable.of("somewhere", 42, 42, 42, 42, "table2");
  List<Operation> operations = Collections.singletonList(Put.of(key1, table1));
  CommitValidator validator = x -> {};
  BiConsumer<ContentKey, String> addedContents = (k, v) -> {};

  @Test
  void testCommitSuccess() throws Exception {
    CommitResult expectedResult =
        CommitResult.builder()
            .targetBranch(branch1)
            .commit(
                ImmutableCommit.builder()
                    .hash(hash1)
                    .commitMeta(commitMeta)
                    .operations(operations)
                    .build())
            .build();
    when(delegate.commit(
            branch1, Optional.of(hash1), commitMeta, operations, validator, addedContents))
        .thenReturn(expectedResult);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    CommitResult actualResult =
        versionStore.commit(
            branch1, Optional.of(hash1), commitMeta, operations, validator, addedContents);
    assertThat(actualResult).isEqualTo(expectedResult);
    verify(delegate)
        .commit(
            eq(branch1),
            eq(Optional.of(hash1)),
            eq(commitMeta),
            eq(operations),
            eq(validator),
            eq(addedContents));
    verify(sink).accept(eq(expectedResult));
    verifyNoMoreInteractions(delegate, sink);
  }

  @ParameterizedTest
  @ValueSource(classes = {ReferenceNotFoundException.class, ReferenceConflictException.class})
  void testCommitFailure(Class<? extends VersionStoreException> e) throws Exception {
    when(delegate.commit(
            branch1, Optional.of(hash1), commitMeta, operations, validator, addedContents))
        .thenAnswer(
            invocation -> {
              throw e.getConstructor(String.class).newInstance("irrelevant");
            });
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    assertThatThrownBy(
            () ->
                versionStore.commit(
                    branch1, Optional.of(hash1), commitMeta, operations, validator, addedContents))
        .isInstanceOf(e);
    verify(delegate)
        .commit(
            eq(branch1),
            eq(Optional.of(hash1)),
            eq(commitMeta),
            eq(operations),
            eq(validator),
            eq(addedContents));
    verifyNoMoreInteractions(delegate, sink);
  }

  @Test
  void testTransplantDryRun() throws Exception {
    boolean dryRun = true;
    TransplantResult expectedResult =
        TransplantResult.builder()
            .sourceRef(branch1)
            .targetBranch(branch2)
            .effectiveTargetHash(hash1)
            .resultantTargetHash(hash2)
            .build();
    when(delegate.transplant(
            TransplantOp.builder()
                .fromRef(branch1)
                .toBranch(branch2)
                .expectedHash(Optional.of(hash1))
                .addSequenceToTransplant(hash1, hash2)
                .updateCommitMetadata(metadataRewriter)
                .dryRun(dryRun)
                .build()))
        .thenReturn(expectedResult);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    TransplantResult result =
        versionStore.transplant(
            TransplantOp.builder()
                .fromRef(branch1)
                .toBranch(branch2)
                .expectedHash(Optional.of(hash1))
                .addSequenceToTransplant(hash1, hash2)
                .updateCommitMetadata(metadataRewriter)
                .dryRun(dryRun)
                .build());
    assertThat(result).isEqualTo(expectedResult);
    verify(delegate)
        .transplant(
            eq(
                TransplantOp.builder()
                    .fromRef(branch1)
                    .toBranch(branch2)
                    .expectedHash(Optional.of(hash1))
                    .addSequenceToTransplant(hash1, hash2)
                    .updateCommitMetadata(metadataRewriter)
                    .dryRun(dryRun)
                    .build()));
    verifyNoMoreInteractions(delegate);
    verifyNoInteractions(sink);
  }

  @Test
  void testTransplantSuccessful() throws Exception {
    boolean dryRun = false;
    TransplantResult expectedResult =
        TransplantResult.builder()
            .sourceRef(branch1)
            .targetBranch(branch2)
            .effectiveTargetHash(hash1)
            .resultantTargetHash(hash2)
            .wasApplied(true)
            .build();
    when(delegate.transplant(
            TransplantOp.builder()
                .fromRef(branch1)
                .toBranch(branch2)
                .expectedHash(Optional.of(hash1))
                .addSequenceToTransplant(hash1, hash2)
                .updateCommitMetadata(metadataRewriter)
                .dryRun(dryRun)
                .build()))
        .thenReturn(expectedResult);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    TransplantResult result =
        versionStore.transplant(
            TransplantOp.builder()
                .fromRef(branch1)
                .toBranch(branch2)
                .expectedHash(Optional.of(hash1))
                .addSequenceToTransplant(hash1, hash2)
                .updateCommitMetadata(metadataRewriter)
                .dryRun(dryRun)
                .build());
    assertThat(result).isEqualTo(expectedResult);
    verify(delegate)
        .transplant(
            eq(
                TransplantOp.builder()
                    .fromRef(branch1)
                    .toBranch(branch2)
                    .expectedHash(Optional.of(hash1))
                    .addSequenceToTransplant(hash1, hash2)
                    .updateCommitMetadata(metadataRewriter)
                    .dryRun(dryRun)
                    .build()));
    verify(sink).accept(expectedResult);
    verifyNoMoreInteractions(delegate, sink);
  }

  @ParameterizedTest
  @ValueSource(classes = {ReferenceNotFoundException.class, ReferenceConflictException.class})
  void testTransplantFailure(Class<? extends VersionStoreException> e) throws Exception {
    when(delegate.transplant(
            TransplantOp.builder()
                .fromRef(branch1)
                .toBranch(branch2)
                .expectedHash(Optional.of(hash1))
                .addSequenceToTransplant(hash1, hash2)
                .updateCommitMetadata(metadataRewriter)
                .build()))
        .thenAnswer(
            invocation -> {
              throw e.getConstructor(String.class).newInstance("irrelevant");
            });
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    assertThatThrownBy(
            () ->
                versionStore.transplant(
                    TransplantOp.builder()
                        .fromRef(branch1)
                        .toBranch(branch2)
                        .expectedHash(Optional.of(hash1))
                        .addSequenceToTransplant(hash1, hash2)
                        .updateCommitMetadata(metadataRewriter)
                        .build()))
        .isInstanceOf(e);
    verify(delegate)
        .transplant(
            eq(
                TransplantOp.builder()
                    .fromRef(branch1)
                    .toBranch(branch2)
                    .expectedHash(Optional.of(hash1))
                    .addSequenceToTransplant(hash1, hash2)
                    .updateCommitMetadata(metadataRewriter)
                    .build()));
    verifyNoMoreInteractions(delegate, sink);
  }

  @Test
  void testMergeDryRun() throws Exception {
    boolean dryRun = true;
    MergeResult expectedResult =
        MergeResult.builder()
            .sourceRef(branch1)
            .sourceHash(hash1)
            .targetBranch(branch2)
            .effectiveTargetHash(hash1)
            .resultantTargetHash(hash2)
            .build();
    when(delegate.merge(
            MergeOp.builder()
                .fromRef(branch1)
                .fromHash(hash1)
                .toBranch(branch2)
                .expectedHash(Optional.of(hash2))
                .dryRun(dryRun)
                .build()))
        .thenReturn(expectedResult);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    MergeResult result =
        versionStore.merge(
            MergeOp.builder()
                .fromRef(branch1)
                .fromHash(hash1)
                .toBranch(branch2)
                .expectedHash(Optional.of(hash2))
                .dryRun(dryRun)
                .build());
    assertThat(result).isEqualTo(expectedResult);
    verify(delegate)
        .merge(
            eq(
                MergeOp.builder()
                    .fromRef(branch1)
                    .fromHash(hash1)
                    .toBranch(branch2)
                    .expectedHash(Optional.of(hash2))
                    .dryRun(dryRun)
                    .build()));
    verifyNoMoreInteractions(delegate);
    verifyNoInteractions(sink);
  }

  @Test
  void testMergeSuccessful() throws Exception {
    boolean dryRun = false;
    MergeResult expectedResult =
        MergeResult.builder()
            .sourceRef(branch1)
            .sourceHash(hash1)
            .targetBranch(branch2)
            .effectiveTargetHash(hash1)
            .resultantTargetHash(hash2)
            .wasApplied(true)
            .build();
    when(delegate.merge(
            MergeOp.builder()
                .fromRef(branch1)
                .fromHash(hash1)
                .toBranch(branch2)
                .expectedHash(Optional.of(hash2))
                .dryRun(dryRun)
                .build()))
        .thenReturn(expectedResult);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    MergeResult result =
        versionStore.merge(
            MergeOp.builder()
                .fromRef(branch1)
                .fromHash(hash1)
                .toBranch(branch2)
                .expectedHash(Optional.of(hash2))
                .dryRun(dryRun)
                .build());
    assertThat(result).isEqualTo(expectedResult);
    verify(delegate)
        .merge(
            eq(
                MergeOp.builder()
                    .fromRef(branch1)
                    .fromHash(hash1)
                    .toBranch(branch2)
                    .expectedHash(Optional.of(hash2))
                    .dryRun(dryRun)
                    .build()));
    verify(sink).accept(expectedResult);
    verifyNoMoreInteractions(delegate, sink);
  }

  @ParameterizedTest
  @ValueSource(classes = {ReferenceNotFoundException.class, ReferenceConflictException.class})
  void testMergeFailure(Class<? extends VersionStoreException> e) throws Exception {
    when(delegate.merge(
            MergeOp.builder()
                .fromRef(branch1)
                .fromHash(hash1)
                .toBranch(branch2)
                .expectedHash(Optional.of(hash2))
                .build()))
        .thenAnswer(
            invocation -> {
              throw e.getConstructor(String.class).newInstance("irrelevant");
            });
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    assertThatThrownBy(
            () ->
                versionStore.merge(
                    MergeOp.builder()
                        .fromRef(branch1)
                        .fromHash(hash1)
                        .toBranch(branch2)
                        .expectedHash(Optional.of(hash2))
                        .build()))
        .isInstanceOf(e);
    verify(delegate)
        .merge(
            eq(
                MergeOp.builder()
                    .fromRef(branch1)
                    .fromHash(hash1)
                    .toBranch(branch2)
                    .expectedHash(Optional.of(hash2))
                    .build()));
    verifyNoMoreInteractions(delegate, sink);
  }

  @Test
  void testAssignSuccess() throws Exception {
    ReferenceAssignedResult expectedResult =
        ImmutableReferenceAssignedResult.builder()
            .namedRef(branch1)
            .previousHash(hash1)
            .currentHash(hash2)
            .build();
    when(delegate.assign(branch1, hash1, hash2)).thenReturn(expectedResult);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    ReferenceAssignedResult actualResult = versionStore.assign(branch1, hash1, hash2);
    assertThat(actualResult).isEqualTo(expectedResult);
    verify(delegate).assign(branch1, hash1, hash2);
    verify(sink).accept(expectedResult);
    verifyNoMoreInteractions(delegate, sink);
  }

  @ParameterizedTest
  @ValueSource(classes = {ReferenceNotFoundException.class, ReferenceConflictException.class})
  void testAssignFailure(Class<? extends VersionStoreException> e) throws Exception {
    when(delegate.assign(branch1, hash1, hash2))
        .thenAnswer(
            invocation -> {
              throw e.getConstructor(String.class).newInstance("irrelevant");
            });
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    assertThatThrownBy(() -> versionStore.assign(branch1, hash1, hash2)).isInstanceOf(e);
    verify(delegate).assign(branch1, hash1, hash2);
    verifyNoMoreInteractions(delegate, sink);
  }

  @Test
  void testCreateSuccess() throws Exception {
    ReferenceCreatedResult expectedResult =
        ImmutableReferenceCreatedResult.builder().namedRef(branch1).hash(hash2).build();
    when(delegate.create(branch1, Optional.of(hash1))).thenReturn(expectedResult);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    ReferenceCreatedResult actualResult = versionStore.create(branch1, Optional.of(hash1));
    assertThat(actualResult).isEqualTo(expectedResult);
    verify(delegate).create(branch1, Optional.of(hash1));
    verify(sink).accept(expectedResult);
    verifyNoMoreInteractions(delegate, sink);
  }

  @ParameterizedTest
  @ValueSource(classes = {ReferenceAlreadyExistsException.class, ReferenceConflictException.class})
  void testCreateFailure(Class<? extends VersionStoreException> e) throws Exception {
    when(delegate.create(branch1, Optional.of(hash1)))
        .thenAnswer(
            invocation -> {
              throw e.getConstructor(String.class).newInstance("irrelevant");
            });
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    assertThatThrownBy(() -> versionStore.create(branch1, Optional.of(hash1))).isInstanceOf(e);
    verify(delegate).create(branch1, Optional.of(hash1));
    verifyNoMoreInteractions(delegate, sink);
  }

  @Test
  void testDeleteSuccess() throws Exception {
    ReferenceDeletedResult expectedResult =
        ImmutableReferenceDeletedResult.builder().namedRef(branch1).hash(hash2).build();
    when(delegate.delete(branch1, hash1)).thenReturn(expectedResult);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    ReferenceDeletedResult actualResult = versionStore.delete(branch1, hash1);
    assertThat(actualResult).isEqualTo(expectedResult);
    verify(delegate).delete(branch1, hash1);
    verify(sink).accept(expectedResult);
    verifyNoMoreInteractions(delegate, sink);
  }

  @ParameterizedTest
  @ValueSource(classes = {ReferenceNotFoundException.class, ReferenceConflictException.class})
  void testDeleteFailure(Class<? extends VersionStoreException> e) throws Exception {
    when(delegate.delete(branch1, hash1))
        .thenAnswer(
            invocation -> {
              throw e.getConstructor(String.class).newInstance("irrelevant");
            });
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    assertThatThrownBy(() -> versionStore.delete(branch1, hash1)).isInstanceOf(e);
    verify(delegate).delete(branch1, hash1);
    verifyNoMoreInteractions(delegate, sink);
  }

  @Test
  void testHashOnReferenceSuccess() throws ReferenceNotFoundException {
    when(delegate.hashOnReference(branch1, Optional.of(hash1), emptyList())).thenReturn(hash1);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    Hash actualHash = versionStore.hashOnReference(branch1, Optional.of(hash1), emptyList());
    assertThat(actualHash).isEqualTo(hash1);
    verify(delegate).hashOnReference(eq(branch1), eq(Optional.of(hash1)), eq(emptyList()));
    verifyNoMoreInteractions(delegate);
    verifyNoInteractions(sink);
  }

  @Test
  void testNoAncestor() throws ReferenceNotFoundException {
    when(delegate.noAncestorHash()).thenReturn(hash1);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    Hash actual = versionStore.noAncestorHash();
    assertThat(actual).isEqualTo(hash1);
    verify(delegate).noAncestorHash();
    verifyNoMoreInteractions(delegate);
    verifyNoInteractions(sink);
  }

  @Test
  void testGetNamedRef() throws Exception {
    String ref = "refs/heads/master";
    GetNamedRefsParams params = GetNamedRefsParams.builder().build();
    ReferenceInfo<CommitMeta> expected = ReferenceInfo.of(hash1, branch1);
    when(delegate.getNamedRef(ref, params)).thenReturn(expected);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    ReferenceInfo<CommitMeta> result = versionStore.getNamedRef(ref, params);
    assertThat(result).isSameAs(expected);
    verifyNoMoreInteractions(delegate);
    verifyNoInteractions(sink);
  }

  @Test
  void testGetNamedRefs() throws Exception {
    GetNamedRefsParams params = GetNamedRefsParams.builder().build();
    when(delegate.getNamedRefs(params, "token1")).thenReturn(iteratorInfos);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    PaginationIterator<ReferenceInfo<CommitMeta>> result =
        versionStore.getNamedRefs(params, "token1");
    assertThat(result).isSameAs(iteratorInfos);
    verifyNoMoreInteractions(delegate);
    verifyNoInteractions(sink);
  }

  @Test
  void testGetCommits() throws Exception {
    boolean fetchAdditionalInfo = true;
    when(delegate.getCommits(branch1, fetchAdditionalInfo)).thenReturn(iteratorCommits);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    PaginationIterator<Commit> result = versionStore.getCommits(branch1, fetchAdditionalInfo);
    assertThat(result).isSameAs(iteratorCommits);
    verifyNoMoreInteractions(delegate);
    verifyNoInteractions(sink);
  }

  @Test
  void testGetKeys() throws Exception {
    when(delegate.getKeys(branch1, "token1", false, NO_KEY_RESTRICTIONS))
        .thenReturn(iteratorKeyEntries);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    PaginationIterator<KeyEntry> result =
        versionStore.getKeys(branch1, "token1", false, NO_KEY_RESTRICTIONS);
    assertThat(result).isSameAs(iteratorKeyEntries);
    verifyNoMoreInteractions(delegate);
    verifyNoInteractions(sink);
  }

  @Test
  void testGetValue() throws Exception {
    ContentResult contentResult =
        contentResult(identifiedContentKeyFromContent(key1, table1, x -> null), table1, null);
    when(delegate.getValue(branch1, key1, false)).thenReturn(contentResult);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    ContentResult result = versionStore.getValue(branch1, key1, false);
    assertThat(result).isEqualTo(contentResult);
    verifyNoMoreInteractions(delegate);
    verifyNoInteractions(sink);
  }

  @Test
  void testGetValues() throws Exception {
    ContentResult contentResult1 =
        contentResult(identifiedContentKeyFromContent(key1, table1, x -> null), table1, null);
    ContentResult contentResult2 =
        contentResult(identifiedContentKeyFromContent(key2, table2, x -> null), table2, null);
    Map<ContentKey, ContentResult> expected =
        ImmutableMap.of(key1, contentResult1, key2, contentResult2);
    when(delegate.getValues(branch1, Arrays.asList(key1, key2), false)).thenReturn(expected);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    Map<ContentKey, ContentResult> result =
        versionStore.getValues(branch1, Arrays.asList(key1, key2), false);
    assertThat(result).isEqualTo(expected);
    verifyNoMoreInteractions(delegate);
    verifyNoInteractions(sink);
  }

  @Test
  void testGetDiffs() throws Exception {
    when(delegate.getDiffs(hash1, hash2, "token1", NO_KEY_RESTRICTIONS)).thenReturn(iteratorDiffs);
    EventsVersionStore versionStore = new EventsVersionStore(delegate, sink);
    PaginationIterator<Diff> result =
        versionStore.getDiffs(hash1, hash2, "token1", NO_KEY_RESTRICTIONS);
    assertThat(result).isSameAs(iteratorDiffs);
    verifyNoMoreInteractions(delegate);
    verifyNoInteractions(sink);
  }
}
