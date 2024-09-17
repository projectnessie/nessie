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
package org.projectnessie.versioned.storage.versionstore;

import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.AUTHOR;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.COMMITTER;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.CONTENT_DISCRIMINATOR;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.MAIN_UNIVERSE;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.UDF;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.ImmutableCommit;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.testworker.OnRefOnly;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestContentMapping {
  @NessiePersist protected static Persist persist;

  @InjectSoftAssertions protected SoftAssertions soft;

  static Stream<Content> contentSamples() {
    return Stream.of(
        IcebergTable.of("/dev/null", 42, 43, 44, 45),
        IcebergView.of("/dev/null", 42, 43),
        UDF.udf("udf-meta", "42", "666"),
        Namespace.of("foo", "bar"),
        OnRefOnly.newOnRef("value"),
        ImmutableDeltaLakeTable.builder()
            .lastCheckpoint("last")
            .addCheckpointLocationHistory("check")
            .addMetadataLocationHistory("meta")
            .build());
  }

  @ParameterizedTest
  @MethodSource("contentSamples")
  public void assignContentId(Content content) {
    soft.assertThat(content.getId()).isNull();
    String newId = UUID.randomUUID().toString();
    Content withId = content.withId(newId);
    soft.assertThat(withId.getId()).isEqualTo(newId);
  }

  @ParameterizedTest
  @MethodSource("contentSamples")
  public void storeFetchContent(Content content) throws Exception {
    ContentMapping contentMapping = new ContentMapping(persist);

    String newId = UUID.randomUUID().toString();
    content = content.withId(newId);

    int payload = payloadForContent(content);
    ContentValueObj value = contentMapping.buildContent(content, payload);
    ObjId id = value.id();
    persist.storeObj(value);

    soft.assertThat(id).isNotNull();

    Content obj = contentMapping.fetchContent(id);
    soft.assertThat(obj).extracting(Content::getId).isEqualTo(content.getId()).isEqualTo(newId);

    soft.assertThat(obj).isEqualTo(content);
  }

  @ParameterizedTest
  @MethodSource("contentSamples")
  public void sameContentOnMultipleKeys(Content contentWithoutId) throws Exception {
    ContentMapping contentMapping = new ContentMapping(persist);

    String newId = UUID.randomUUID().toString();
    Content content = contentWithoutId.withId(newId);

    int payload = payloadForContent(content);
    ContentValueObj value = contentMapping.buildContent(content, payload);
    ObjId id = value.id();
    persist.storeObj(value);

    List<ContentKey> dupKeys =
        IntStream.rangeClosed(1, 3)
            .mapToObj(i -> ContentKey.of("dupContent" + i))
            .collect(Collectors.toList());

    StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
    dupKeys.forEach(k -> index.add(indexElement(keyToStoreKey(k), commitOp(Action.ADD, 1, id))));

    Map<ContentKey, Content> contents = contentMapping.fetchContents(index, dupKeys);
    soft.assertThat(contents)
        .containsAllEntriesOf(
            dupKeys.stream().collect(Collectors.toMap(Function.identity(), e -> content)));
  }

  @Test
  public void commitsWithoutAdditionalInfo() throws Exception {
    ContentMapping contentMapping = new ContentMapping(persist);

    ObjId id = randomObjId();
    ObjId secondary = randomObjId();

    IcebergTable table = IcebergTable.of("/dev/null", 42, 43, 44, 45, UUID.randomUUID().toString());
    ObjId tableId = contentMapping.buildContent(table, 1).id();

    StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
    index.add(
        indexElement(
            key(MAIN_UNIVERSE, "foo", CONTENT_DISCRIMINATOR), commitOp(Action.ADD, 1, tableId)));
    index.add(
        indexElement(
            key(MAIN_UNIVERSE, "bar", CONTENT_DISCRIMINATOR), commitOp(Action.REMOVE, 1, tableId)));

    CommitMeta referenceCommitMeta =
        ImmutableCommitMeta.builder()
            .message("commit message")
            .author("author")
            .committer("committer")
            .hash(id.toString())
            .addParentCommitHashes(EMPTY_OBJ_ID.toString(), secondary.toString())
            .build();

    CommitObj commitObj =
        CommitObj.commitBuilder()
            .id(id)
            .created(42L)
            .headers(newCommitHeaders().add(AUTHOR, "author").add(COMMITTER, "committer").build())
            .addTail(EMPTY_OBJ_ID)
            .addSecondaryParents(secondary)
            .incrementalIndex(index.serialize())
            .message("commit message")
            .seq(42L)
            .build();
    Commit commit =
        Commit.builder()
            .hash(objIdToHash(id))
            .parentHash(objIdToHash(EMPTY_OBJ_ID))
            .commitMeta(referenceCommitMeta)
            .build();

    Commit c = contentMapping.commitObjToCommit(false, commitObj);
    soft.assertThat(c).isEqualTo(commit);

    c = contentMapping.commitObjToCommit(false, commitObj, EMPTY_OBJ_ID);
    c =
        ImmutableCommit.builder()
            .from(commit)
            .hash(TypeMapping.objIdToHash(EMPTY_OBJ_ID))
            .commitMeta(c.getCommitMeta().toBuilder().hash(EMPTY_OBJ_ID.toString()).build())
            .build();
    commit =
        ImmutableCommit.builder()
            .from(commit)
            .hash(objIdToHash(EMPTY_OBJ_ID))
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .from(commit.getCommitMeta())
                    .hash(EMPTY_OBJ_ID.toString())
                    .build())
            .build();
    soft.assertThat(c).isEqualTo(commit);
  }

  @Test
  public void commitsWithAdditionalInfo() throws Exception {
    ContentMapping contentMapping = new ContentMapping(persist);

    ObjId id = randomObjId();
    ObjId secondary = randomObjId();

    IcebergTable table = IcebergTable.of("/dev/null", 42, 43, 44, 45, UUID.randomUUID().toString());
    ContentValueObj tableObj = contentMapping.buildContent(table, 1);
    ObjId tableId = tableObj.id();
    persist.storeObj(tableObj);

    StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
    index.add(
        indexElement(
            key(MAIN_UNIVERSE, "foo", CONTENT_DISCRIMINATOR), commitOp(Action.ADD, 1, tableId)));
    index.add(
        indexElement(
            key(MAIN_UNIVERSE, "bar", CONTENT_DISCRIMINATOR), commitOp(Action.REMOVE, 1, tableId)));

    CommitMeta referenceCommitMeta =
        ImmutableCommitMeta.builder()
            .message("commit message")
            .author("author")
            .committer("committer")
            .hash(id.toString())
            .addParentCommitHashes(EMPTY_OBJ_ID.toString(), secondary.toString())
            .build();

    CommitObj commitObj =
        CommitObj.commitBuilder()
            .id(id)
            .created(42L)
            .headers(newCommitHeaders().add(AUTHOR, "author").add(COMMITTER, "committer").build())
            .addTail(EMPTY_OBJ_ID)
            .addSecondaryParents(secondary)
            .incrementalIndex(index.serialize())
            .message("commit message")
            .seq(42L)
            .build();
    Commit commit =
        Commit.builder()
            .hash(objIdToHash(id))
            .parentHash(objIdToHash(EMPTY_OBJ_ID))
            .commitMeta(referenceCommitMeta)
            .addOperations(
                Delete.of(ContentKey.of("bar")),
                Put.of(
                    ContentKey.of("foo"),
                    DefaultStoreWorker.instance()
                        .valueFromStore(tableObj.payload(), tableObj.data())))
            .build();

    Commit c = contentMapping.commitObjToCommit(true, commitObj);
    soft.assertThat(c).isEqualTo(commit);

    c = contentMapping.commitObjToCommit(true, commitObj, EMPTY_OBJ_ID);
    c =
        ImmutableCommit.builder()
            .from(commit)
            .hash(TypeMapping.objIdToHash(EMPTY_OBJ_ID))
            .commitMeta(c.getCommitMeta().toBuilder().hash(EMPTY_OBJ_ID.toString()).build())
            .build();
    commit =
        ImmutableCommit.builder()
            .from(commit)
            .hash(objIdToHash(EMPTY_OBJ_ID))
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .from(commit.getCommitMeta())
                    .hash(EMPTY_OBJ_ID.toString())
                    .build())
            .build();
    soft.assertThat(c).isEqualTo(commit);
  }
}
