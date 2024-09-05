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
package org.projectnessie.versioned.storage.serialize;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.emptyImmutableIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;
import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.common.objtypes.TagObj.tag;
import static org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj.uniqueId;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.Reference.PreviousPointer.previousPointer;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeObjId;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeObjIds;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeReference;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObjId;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObjIds;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializePreviousPointers;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeReference;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.immutables.value.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;
import org.projectnessie.versioned.storage.common.objtypes.JsonObj;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.common.proto.StorageTypes;
import org.projectnessie.versioned.storage.commontests.objtypes.AnotherTestObj;
import org.projectnessie.versioned.storage.commontests.objtypes.SimpleTestObj;
import org.projectnessie.versioned.storage.commontests.objtypes.VersionedTestObj;

@ExtendWith(SoftAssertionsExtension.class)
public class TestProtoSerialization {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource("references")
  void references(Reference reference) {
    byte[] serialized = serializeReference(reference);
    Reference deserialized = deserializeReference(serialized);
    byte[] reserialized = serializeReference(deserialized);
    soft.assertThat(deserialized).isEqualTo(reference);
    soft.assertThat(serialized).isEqualTo(reserialized);
  }

  @ParameterizedTest
  @MethodSource("previousPointers")
  void previousPointers(List<Reference.PreviousPointer> previousPointers) {
    byte[] serialized = serializePreviousPointers(previousPointers);
    List<Reference.PreviousPointer> deserialized = deserializePreviousPointers(serialized);
    soft.assertThat(deserialized).containsExactlyElementsOf(previousPointers);

    Reference ref = reference("foo", randomObjId(), false, 42L, null, previousPointers);
    byte[] serializedRef = serializeReference(ref);
    Reference deserializedRef = deserializeReference(serializedRef);
    soft.assertThat(deserializedRef.previousPointers()).containsExactlyElementsOf(previousPointers);
  }

  @ParameterizedTest
  @MethodSource("objs")
  void objs(Obj obj) throws Exception {
    byte[] serialized = serializeObj(obj, Integer.MAX_VALUE, Integer.MAX_VALUE, true);
    Obj deserialized = deserializeObj(obj.id(), 0L, serialized, null);
    Obj deserializedByteBuffer = deserializeObj(obj.id(), 0L, ByteBuffer.wrap(serialized), null);
    byte[] reserialized = serializeObj(deserialized, Integer.MAX_VALUE, Integer.MAX_VALUE, true);
    soft.assertThat(deserialized).isEqualTo(obj).isEqualTo(deserializedByteBuffer);
    Obj deserialized2 = deserializeObj(obj.id(), 0L, reserialized, null);
    soft.assertThat(deserialized2).isEqualTo(obj);
    soft.assertThat(serialized).isEqualTo(reserialized);

    if (obj instanceof UpdateableObj) {
      soft.assertThat(StorageTypes.ObjProto.parseFrom(serialized).getCustom().getVersionToken())
          .isNotNull();

      byte[] serializedWithoutVersionToken =
          serializeObj(obj, Integer.MAX_VALUE, Integer.MAX_VALUE, false);

      soft.assertThat(
              StorageTypes.ObjProto.parseFrom(serializedWithoutVersionToken)
                  .getCustom()
                  .getVersionToken())
          .isEmpty();

      UpdateableObj deserializedWithoutVersionToken =
          (UpdateableObj)
              deserializeObj(obj.id(), 420L, serializedWithoutVersionToken, "my-foo-bar-baz");
      soft.assertThat(deserializedWithoutVersionToken.versionToken())
          .isEqualTo("my-foo-bar-baz")
          .isNotEqualTo(((UpdateableObj) obj).versionToken());
    }

    if (obj instanceof IndexObj || obj instanceof CommitObj) {
      soft.assertThatThrownBy(() -> serializeObj(obj, 0, 0, true))
          .isInstanceOf(ObjTooLargeException.class);
    }
  }

  @Test
  public void nullInputs() throws Exception {
    soft.assertThat(serializeReference(null)).isNull();
    soft.assertThat(deserializeReference(null)).isNull();

    soft.assertThat(serializeObjId(null)).isNull();
    soft.assertThat(deserializeObjId(null)).isNull();

    soft.assertThatCode(() -> serializeObjIds(null, id -> {})).doesNotThrowAnyException();
    soft.assertThatCode(() -> deserializeObjIds(null, id -> {})).doesNotThrowAnyException();

    soft.assertThat(deserializePreviousPointers(null)).isNotNull().isEmpty();
    soft.assertThat(serializePreviousPointers(null)).isNull();

    soft.assertThat(serializeObj(null, Integer.MAX_VALUE, Integer.MAX_VALUE, true)).isNull();
    soft.assertThat(deserializeObj(randomObjId(), 420L, (byte[]) null, null)).isNull();
    soft.assertThat(deserializeObj(randomObjId(), 420L, (ByteBuffer) null, null)).isNull();
  }

  static Stream<List<Reference.PreviousPointer>> previousPointers() {
    return Stream.of(
        emptyList(),
        singletonList(previousPointer(randomObjId(), 42L)),
        asList(previousPointer(randomObjId(), 42L), previousPointer(randomObjId(), 101L)),
        asList(
            previousPointer(randomObjId(), 42L),
            previousPointer(randomObjId(), 101L),
            previousPointer(EMPTY_OBJ_ID, 99L)));
  }

  static Stream<Reference> references() {
    return Stream.of(
        reference("a", EMPTY_OBJ_ID, false, 0L, null),
        reference("b", randomObjId(), false, 0L, null),
        reference("c", randomObjId(), true, 0L, null),
        reference("d", EMPTY_OBJ_ID, false, 42L, null),
        reference("e", randomObjId(), false, 42L, null),
        reference("f", randomObjId(), true, 42L, null),
        reference("g", EMPTY_OBJ_ID, false, 0L, randomObjId()),
        reference("h", randomObjId(), false, 0L, randomObjId()),
        reference("i", randomObjId(), true, 0L, randomObjId()),
        reference("j", EMPTY_OBJ_ID, false, 42L, randomObjId()),
        reference("k", randomObjId(), false, 42L, randomObjId()),
        reference("l", randomObjId(), true, 42L, randomObjId()));
  }

  static Stream<Obj> objs() {
    return Stream.of(
        ref(randomObjId(), 420L, "hello", randomObjId(), 42L, randomObjId()),
        CommitObj.commitBuilder()
            .id(randomObjId())
            .seq(1L)
            .created(42L)
            .referenced(1234L)
            .message("msg")
            .headers(EMPTY_COMMIT_HEADERS)
            .incrementalIndex(emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize())
            .build(),
        CommitObj.commitBuilder()
            .id(randomObjId())
            .seq(2L)
            .created(43L)
            .message("with headers and more")
            // Note: can only use one header name here, because commit-headers rely on HashMap,
            // which has a non-deterministic iteration order.
            .headers(newCommitHeaders().add("foo", "bar").add("foo", OffsetDateTime.now()).build())
            .addTail(randomObjId())
            .addTail(randomObjId())
            .addSecondaryParents(randomObjId())
            .addReferenceIndexStripes(indexStripe(key("a"), key("b"), randomObjId()))
            .addReferenceIndexStripes(indexStripe(key("c"), key("d"), randomObjId()))
            .referenceIndex(randomObjId())
            .incrementalIndex(emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize())
            .build(),
        tag(
            randomObjId(),
            420L,
            "tab-msg",
            newCommitHeaders().add("Foo", "bar").build(),
            ByteString.copyFrom(new byte[1])),
        contentValue(randomObjId(), 420L, "cid", 0, ByteString.copyFrom(new byte[1])),
        stringData(
            randomObjId(),
            420L,
            "foo",
            Compression.NONE,
            "foo",
            emptyList(),
            ByteString.copyFrom(new byte[1])),
        indexSegments(randomObjId(), 420L, emptyList()),
        indexSegments(
            randomObjId(),
            420L,
            asList(
                indexStripe(key("a"), key("b"), randomObjId()),
                indexStripe(key("c"), key("d"), randomObjId()))),
        index(randomObjId(), 420L, emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize()),
        JsonObj.json(
            randomObjId(),
            420L,
            ImmutableJsonTestModel.builder()
                .parent(randomObjId())
                .text("foo")
                .number(42)
                .map(Map.of("foo", "bar"))
                .list(List.of("foo", "bar"))
                .instant(Instant.now())
                .optional(Optional.of("foo"))
                .build()),
        uniqueId("space", UUID.randomUUID()),
        SimpleTestObj.builder().id(randomObjId()).build(),
        AnotherTestObj.builder()
            .id(randomObjId())
            .parent(randomObjId())
            .text("foo".repeat(4000))
            .number(42.42d)
            .map(Map.of("k1", "v1".repeat(4000), "k2", "v2".repeat(4000)))
            .list(List.of("a", "b", "c"))
            .optional("optional")
            .instant(Instant.ofEpochMilli(1234567890L))
            .build(),
        VersionedTestObj.builder().id(randomObjId()).someValue("foo").versionToken("1").build());
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableJsonTestModel.class)
  @JsonDeserialize(as = ImmutableJsonTestModel.class)
  public interface JsonTestModel {
    ObjId parent();

    String text();

    Number number();

    Map<String, String> map();

    List<String> list();

    Instant instant();

    Optional<String> optional();
  }
}
