/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.common.persist;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.emptyImmutableIndex;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.REF;
import static org.projectnessie.versioned.storage.common.persist.Hashes.contentValueHash;
import static org.projectnessie.versioned.storage.common.persist.Hashes.hashAsObjId;
import static org.projectnessie.versioned.storage.common.persist.Hashes.hashCommitHeaders;
import static org.projectnessie.versioned.storage.common.persist.Hashes.indexHash;
import static org.projectnessie.versioned.storage.common.persist.Hashes.indexSegmentsHash;
import static org.projectnessie.versioned.storage.common.persist.Hashes.newHasher;
import static org.projectnessie.versioned.storage.common.persist.Hashes.refHash;
import static org.projectnessie.versioned.storage.common.persist.Hashes.stringDataHash;
import static org.projectnessie.versioned.storage.common.persist.Hashes.tagHash;
import static org.projectnessie.versioned.storage.common.persist.Hashes.uniqueIdHash;
import static org.projectnessie.versioned.storage.commontests.AbstractBasePersistTests.allObjectTypeSamples;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.logic.ConflictHandler;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj;
import org.projectnessie.versioned.storage.common.objtypes.RefObj;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.objtypes.TagObj;
import org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj;

@SuppressWarnings("UnstableApiUsage")
@ExtendWith(SoftAssertionsExtension.class)
public class TestObjIdHasher {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void nullValuesAndEmptyCollections() {
    Hasher h = mock(Hasher.class);
    ObjIdHasher hasher = new ObjIdHasherImpl(h);
    hasher
        .hash((Long) null)
        .hash((Integer) null)
        .hash((Boolean) null)
        .hash((String) null)
        .hash((ByteBuffer) null)
        .hash((Hashable) null)
        .hash((Enum<?>) null)
        .hash((Double) null)
        .hash((Float) null)
        .hash((byte[]) null)
        .hash((UUID) null)
        .hash((ObjId) null)
        .hashCollection(null)
        .hashIntCollection(null)
        .hashLongCollection(null)
        .hashStringToStringMap(null)
        .hashUuidCollection(null)
        .hashCollection(emptyList())
        .hashIntCollection(emptyList())
        .hashLongCollection(emptyList())
        .hashStringToStringMap(emptyMap())
        .hashUuidCollection(emptyList());
    verifyNoInteractions(h);
  }

  @Test
  public void verifyPrimitive() {
    Hasher h = mock(Hasher.class);
    ObjIdHasher hasher = new ObjIdHasherImpl(h);
    hasher.hash(true).hash(42).hash('c').hash(666L).hash(1.2d).hash(2.3f);
    verify(h).putBoolean(true);
    verify(h).putInt(42);
    verify(h).putChar('c');
    verify(h).putLong(666L);
    verify(h).putDouble(1.2d);
    verify(h).putFloat(2.3f);
    verifyNoMoreInteractions(h);
  }

  @Test
  public void verifyBoxed() {
    Hasher h = mock(Hasher.class);
    ObjIdHasher hasher = new ObjIdHasherImpl(h);
    hasher
        .hash(Boolean.TRUE)
        .hash(Integer.valueOf(42))
        .hash(Long.valueOf(666L))
        .hash(Double.valueOf(1.2d))
        .hash(Float.valueOf(2.3f))
        .hash("foo")
        .hash(new byte[] {1, 2});
    verify(h).putBoolean(true);
    verify(h).putInt(42);
    verify(h).putLong(666L);
    verify(h).putDouble(1.2d);
    verify(h).putFloat(2.3f);
    verify(h).putString("foo", UTF_8);
    verify(h).putBytes(new byte[] {1, 2});
    verifyNoMoreInteractions(h);
  }

  @Test
  public void verifyEnum() {
    Hasher h = mock(Hasher.class);
    ObjIdHasher hasher = new ObjIdHasherImpl(h);
    hasher.hash(REF);
    verify(h).putString(REF.name(), UTF_8);
    verifyNoMoreInteractions(h);
  }

  @Test
  public void verifyUuid() {
    Hasher h = mock(Hasher.class);
    ObjIdHasher hasher = new ObjIdHasherImpl(h);
    hasher.hash(new UUID(123L, 456L));
    verify(h).putLong(123L);
    verify(h).putLong(456L);
    verifyNoMoreInteractions(h);
  }

  @Test
  public void verifyHashable() {
    Hasher h = mock(Hasher.class);
    ObjIdHasher hasher = new ObjIdHasherImpl(h);
    hasher.hash(idHasher -> idHasher.hash(42L).hash(1));
    verify(h).putLong(42L);
    verify(h).putInt(1);
    verifyNoMoreInteractions(h);
  }

  @Test
  public void verifyHashableCollection() {
    Hasher h = mock(Hasher.class);
    ObjIdHasher hasher = new ObjIdHasherImpl(h);
    List<Hashable> coll =
        asList(
            idHasher -> idHasher.hash(1),
            idHasher -> idHasher.hash(2),
            idHasher -> idHasher.hash(3));
    hasher.hashCollection(coll);
    verify(h).putInt(1);
    verify(h).putInt(2);
    verify(h).putInt(3);
    verifyNoMoreInteractions(h);
  }

  @Test
  public void verifyUuidCollection() {
    Hasher h = mock(Hasher.class);
    ObjIdHasher hasher = new ObjIdHasherImpl(h);
    hasher.hashUuidCollection(asList(new UUID(1L, 2L), new UUID(3L, 4L)));
    verify(h).putLong(1L);
    verify(h).putLong(2L);
    verify(h).putLong(3L);
    verify(h).putLong(4L);
    verifyNoMoreInteractions(h);
  }

  @Test
  public void verifyStringToStringMap() {
    Hasher h = mock(Hasher.class);
    ObjIdHasher hasher = new ObjIdHasherImpl(h);
    Map<String, String> map = new LinkedHashMap<>();
    map.put("a", "b");
    map.put("c", "d");
    map.put("e", "f");
    hasher.hashStringToStringMap(map);
    verify(h).putString("a", UTF_8);
    verify(h).putString("b", UTF_8);
    verify(h).putString("c", UTF_8);
    verify(h).putString("d", UTF_8);
    verify(h).putString("e", UTF_8);
    verify(h).putString("f", UTF_8);
    verifyNoMoreInteractions(h);
  }

  @Test
  public void verifyLongCollection() {
    Hasher h = mock(Hasher.class);
    ObjIdHasher hasher = new ObjIdHasherImpl(h);
    hasher.hashLongCollection(asList(1L, 2L));
    verify(h).putLong(1L);
    verify(h).putLong(2L);
    verifyNoMoreInteractions(h);
  }

  @Test
  public void verifyIntCollection() {
    Hasher h = mock(Hasher.class);
    ObjIdHasher hasher = new ObjIdHasherImpl(h);
    hasher.hashIntCollection(asList(1, 2));
    verify(h).putInt(1);
    verify(h).putInt(2);
    verifyNoMoreInteractions(h);
  }

  @Test
  public void verifyGenerate() {
    Hasher h = mock(Hasher.class);
    ObjIdHasher hasher = new ObjIdHasherImpl(h);
    when(h.hash()).thenReturn(HashCode.fromBytes(new byte[] {1, 2, 3, 4}));
    ObjId id = hasher.generate();
    verify(h).hash();
    verifyNoMoreInteractions(h);
    soft.assertThat(id.asByteArray()).containsExactly(1, 2, 3, 4);
  }

  /**
   * Verify that the {@link ObjId} generation yields the same results compared to the state before
   * the change that introduced {@link ObjIdHasher}. This test and the {@link Hashes} class may
   * eventually go away.
   */
  @ParameterizedTest
  @MethodSource("standardObjTypes")
  public void objIdHasherProducesSameResult(Obj obj) throws Exception {
    StandardObjType type = (StandardObjType) obj.type();
    ObjId generatedId = obj.id();
    ObjId previousCodeId = null;
    switch (type) {
      case COMMIT:
        CommitObj commitObj = (CommitObj) obj;

        CreateCommit.Builder commit =
            CreateCommit.newCommitBuilder()
                .parentCommitId(commitObj.directParent())
                .message(commitObj.message())
                .headers(commitObj.headers());

        Hasher hasher =
            newHasher()
                .putString(COMMIT.name(), UTF_8)
                .putBytes(commitObj.directParent().asByteBuffer())
                .putString(commitObj.message(), UTF_8);
        hashCommitHeaders(hasher, commitObj.headers());

        StoreIndex<CommitOp> index = indexesLogic(null).incrementalIndexFromCommit(commitObj);
        for (StoreIndexElement<CommitOp> el : index) {
          CommitOp op = el.content();
          if (op.action() == CommitOp.Action.REMOVE) {
            UUID contentId = op.contentId();
            hasher.putInt(2).putInt(op.payload()).putString(el.key().rawString(), UTF_8);
            if (contentId != null) {
              hasher
                  .putLong(contentId.getMostSignificantBits())
                  .putLong(contentId.getLeastSignificantBits());
            }
            commit.addRemoves(
                CreateCommit.Remove.commitRemove(
                    el.key(), op.payload(), ObjId.EMPTY_OBJ_ID, op.contentId()));
          }
        }
        for (StoreIndexElement<CommitOp> el : index) {
          CommitOp op = el.content();
          if (op.action() == CommitOp.Action.ADD) {
            UUID contentId = op.contentId();
            hasher
                .putInt(1)
                .putString(el.key().rawString(), UTF_8)
                .putInt(op.payload())
                .putBytes(requireNonNull(op.value()).asByteBuffer());
            if (contentId != null) {
              hasher
                  .putLong(contentId.getMostSignificantBits())
                  .putLong(contentId.getLeastSignificantBits());
            }
            commit.addAdds(
                CreateCommit.Add.commitAdd(
                    el.key(), op.payload(), requireNonNull(op.value()), null, op.contentId()));
          }
        }
        previousCodeId = hashAsObjId(hasher);

        Persist persist = mock(Persist.class);
        when(persist.config()).thenReturn(StoreConfig.Adjustable.empty());
        when(persist.fetchTypedObj(commitObj.directParent(), COMMIT, CommitObj.class))
            .thenReturn(
                commitBuilder()
                    .id(commitObj.directParent())
                    .created(0L)
                    .seq(1L)
                    .headers(EMPTY_COMMIT_HEADERS)
                    .message("")
                    .incrementalIndex(emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize())
                    .build());
        generatedId =
            commitLogic(persist)
                .buildCommitObj(
                    commit.build(),
                    c -> ConflictHandler.ConflictResolution.CONFLICT,
                    (k, v) -> {},
                    (add, key, id) -> null,
                    (add, key, id) -> null)
                .id();

        break;
      case REF:
        RefObj refObj = (RefObj) obj;
        previousCodeId = refHash(refObj.name(), refObj.initialPointer(), refObj.createdAtMicros());
        break;
      case INDEX:
        IndexObj indexObj = (IndexObj) obj;
        previousCodeId = indexHash(indexObj.index());
        break;
      case INDEX_SEGMENTS:
        IndexSegmentsObj indexSegmentsObj = (IndexSegmentsObj) obj;
        previousCodeId = indexSegmentsHash(indexSegmentsObj.stripes());
        break;
      case TAG:
        TagObj tagObj = (TagObj) obj;
        previousCodeId = tagHash(tagObj.message(), tagObj.headers(), tagObj.signature());
        break;
      case STRING:
        StringObj stringObj = (StringObj) obj;
        previousCodeId =
            stringDataHash(
                stringObj.contentType(),
                stringObj.compression(),
                stringObj.filename(),
                stringObj.predecessors(),
                stringObj.text());
        break;
      case UNIQUE:
        UniqueIdObj uniqueIdObj = (UniqueIdObj) obj;
        previousCodeId = uniqueIdHash(uniqueIdObj.space(), uniqueIdObj.value());
        break;
      case VALUE:
        ContentValueObj contentValueObj = (ContentValueObj) obj;
        previousCodeId =
            contentValueHash(
                contentValueObj.contentId(), contentValueObj.payload(), contentValueObj.data());
        break;
      default:
        // ignore
        break;
    }

    soft.assertThat(generatedId).isEqualTo(previousCodeId);
  }

  static Stream<Obj> standardObjTypes() {
    return Stream.concat(
        allObjectTypeSamples().filter(o -> o.type() instanceof StandardObjType), moreCommitObjs());
  }

  private static Stream<Obj> moreCommitObjs() {
    return Stream.of(
        // CommitObj
        );
  }
}
