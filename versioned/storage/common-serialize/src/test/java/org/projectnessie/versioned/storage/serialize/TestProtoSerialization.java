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
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;
import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.common.objtypes.TagObj.tag;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.Reference.PreviousPointer.previousPointer;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializePreviousPointers;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.deserializeReference;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObj;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializePreviousPointers;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeReference;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.immutables.value.Value;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.JsonObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;

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
    byte[] serialized = serializeObj(obj, Integer.MAX_VALUE, Integer.MAX_VALUE);
    Obj deserialized = deserializeObj(obj.id(), serialized);
    Obj deserializedByteBuffer = deserializeObj(obj.id(), ByteBuffer.wrap(serialized));
    byte[] reserialized = serializeObj(deserialized, Integer.MAX_VALUE, Integer.MAX_VALUE);
    soft.assertThat(deserialized).isEqualTo(obj).isEqualTo(deserializedByteBuffer);
    soft.assertThat(serialized).isEqualTo(reserialized);
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
        ref(randomObjId(), "hello", randomObjId(), 42L, randomObjId()),
        CommitObj.commitBuilder()
            .id(randomObjId())
            .seq(1L)
            .created(42L)
            .message("msg")
            .headers(EMPTY_COMMIT_HEADERS)
            .incrementalIndex(emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize())
            .build(),
        tag(
            randomObjId(),
            "tab-msg",
            newCommitHeaders().add("Foo", "bar").build(),
            ByteString.copyFrom(new byte[1])),
        contentValue(randomObjId(), "cid", 0, ByteString.copyFrom(new byte[1])),
        stringData(
            randomObjId(),
            "foo",
            Compression.NONE,
            "foo",
            emptyList(),
            ByteString.copyFrom(new byte[1])),
        indexSegments(randomObjId(), emptyList()),
        index(randomObjId(), emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize()),
        JsonObj.json(
            randomObjId(),
            ImmutableJsonTestModel.builder()
                .parent(randomObjId())
                .text("foo")
                .number(42)
                .map(Map.of("foo", "bar"))
                .list(List.of("foo", "bar"))
                .instant(Instant.now())
                .optional(Optional.of("foo"))
                .build()));
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
