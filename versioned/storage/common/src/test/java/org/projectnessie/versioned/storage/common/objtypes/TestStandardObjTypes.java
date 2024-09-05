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
package org.projectnessie.versioned.storage.common.objtypes;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.nessie.relocated.protobuf.ByteString.copyFromUtf8;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.emptyImmutableIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.REMOVE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;
import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.INDEX;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.INDEX_SEGMENTS;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.REF;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.STRING;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.TAG;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.UNIQUE;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.VALUE;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.common.objtypes.TagObj.tag;
import static org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj.uniqueId;
import static org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj.uuidToBytes;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@ExtendWith(SoftAssertionsExtension.class)
public class TestStandardObjTypes {
  @InjectSoftAssertions protected SoftAssertions soft;

  static Stream<Arguments> standardObjTypes() {
    return Stream.of(
        arguments(ref(randomObjId(), 420L, "hello", randomObjId(), 42L, randomObjId()), REF),
        arguments(
            CommitObj.commitBuilder()
                .id(randomObjId())
                .seq(1L)
                .created(42L)
                .message("msg")
                .headers(EMPTY_COMMIT_HEADERS)
                .incrementalIndex(emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize())
                .build(),
            COMMIT),
        arguments(
            tag(
                randomObjId(),
                420L,
                "tab-msg",
                newCommitHeaders().add("Foo", "bar").build(),
                ByteString.copyFrom(new byte[1])),
            TAG),
        arguments(
            contentValue(randomObjId(), 420L, "cid", 0, ByteString.copyFrom(new byte[1])), VALUE),
        arguments(
            stringData(
                randomObjId(),
                420L,
                "foo",
                Compression.NONE,
                "foo",
                emptyList(),
                ByteString.copyFrom(new byte[1])),
            STRING),
        arguments(indexSegments(randomObjId(), 420L, emptyList()), INDEX_SEGMENTS),
        arguments(
            index(randomObjId(), 420L, emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize()),
            INDEX),
        arguments(uniqueId(randomObjId(), 420L, "space", uuidToBytes(UUID.randomUUID())), UNIQUE));
  }

  @ParameterizedTest
  @MethodSource("standardObjTypes")
  public void standardObjTypes(Obj obj, ObjType type) {
    soft.assertThat(obj).extracting(Obj::type).isSameAs(type);
  }

  @Test
  public void indexIds() {
    StoreIndex<CommitOp> index0 = emptyImmutableIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> index1 = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> index2 = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> index3 = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> index4 = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> index5 = newStoreIndex(COMMIT_OP_SERIALIZER);

    index1.add(indexElement(key("foo"), commitOp(ADD, 0, objIdFromString("1234"))));
    index2.add(indexElement(key("foo"), commitOp(REMOVE, 0, objIdFromString("1234"))));
    index3.add(indexElement(key("foo"), commitOp(ADD, 1, objIdFromString("1234"))));
    index4.add(indexElement(key("foo"), commitOp(ADD, 0, objIdFromString("123456"))));
    index5.add(indexElement(key("foo"), commitOp(ADD, 0, objIdFromString("1234"))));
    index5.add(indexElement(key("bar"), commitOp(ADD, 0, objIdFromString("1234"))));

    List<IndexObj> objs =
        asList(
            index(index0.serialize()),
            index(index1.serialize()),
            index(index2.serialize()),
            index(index3.serialize()),
            index(index4.serialize()),
            index(index5.serialize()));

    // Generated IDs must be unique
    soft.assertThat(objs.stream().map(Obj::id).collect(Collectors.toSet())).hasSize(objs.size());
  }

  @Test
  public void indexSegmentIds() {
    List<IndexSegmentsObj> objs =
        asList(
            indexSegments(emptyList()),
            indexSegments(
                singletonList(indexStripe(keyFromString(""), keyFromString("a"), EMPTY_OBJ_ID))),
            indexSegments(
                asList(
                    indexStripe(keyFromString(""), keyFromString("b"), EMPTY_OBJ_ID),
                    indexStripe(keyFromString("b"), keyFromString("c"), EMPTY_OBJ_ID))),
            indexSegments(
                asList(
                    indexStripe(keyFromString("a"), keyFromString("b"), EMPTY_OBJ_ID),
                    indexStripe(keyFromString("c"), keyFromString("d"), EMPTY_OBJ_ID))));

    // Generated IDs must be unique
    soft.assertThat(objs.stream().map(Obj::id).collect(Collectors.toSet())).hasSize(objs.size());
  }

  @Test
  public void refIds() {
    List<RefObj> objs =
        asList(
            ref("r1", objIdFromString("1234"), 0L, randomObjId()),
            ref("r2", objIdFromString("1234"), 42L, null),
            ref("r3", objIdFromString("123456"), 0L, randomObjId()),
            ref("r4", objIdFromString("123456"), 42L, randomObjId()));

    // Generated IDs must be unique
    soft.assertThat(objs.stream().map(Obj::id).collect(Collectors.toSet())).hasSize(objs.size());
  }

  @Test
  public void tagIds() {
    List<TagObj> objs =
        asList(
            tag("tag-message", newCommitHeaders().add("Foo", "bar").build(), copyFromUtf8("foo")),
            tag(null, null, copyFromUtf8("hello world")),
            tag(null, null, copyFromUtf8("foo")));

    // Generated IDs must be unique
    soft.assertThat(objs.stream().map(Obj::id).collect(Collectors.toSet())).hasSize(objs.size());
  }

  @Test
  public void contentValueIds() {
    List<ContentValueObj> objs =
        asList(
            contentValue("cid", 0, copyFromUtf8("hello world")),
            contentValue("xyz", 0, copyFromUtf8("hello world")),
            contentValue("xyz", 42, copyFromUtf8("hello world")),
            contentValue("xyz", 42, copyFromUtf8("foo bar")));

    // Generated IDs must be unique
    soft.assertThat(objs.stream().map(Obj::id).collect(Collectors.toSet())).hasSize(objs.size());
  }

  @Test
  public void stringIds() {
    List<StringObj> objs =
        asList(
            stringData(
                "text/plain",
                Compression.NONE,
                null,
                asList(objIdFromString("1234"), objIdFromString("5678")),
                copyFromUtf8("hello world")),
            stringData(
                "text/plain",
                Compression.NONE,
                "file.name",
                asList(objIdFromString("1234"), objIdFromString("5678")),
                copyFromUtf8("hello world")),
            stringData(
                "text/html",
                Compression.NONE,
                "file.name",
                asList(objIdFromString("1234"), objIdFromString("5678")),
                copyFromUtf8("hello world")),
            stringData(
                "text/html",
                Compression.GZIP,
                "file.name",
                asList(objIdFromString("1234"), objIdFromString("5678")),
                copyFromUtf8("hello world")),
            stringData(
                "text/html",
                Compression.GZIP,
                "file.name.other",
                asList(objIdFromString("1234"), objIdFromString("5678")),
                copyFromUtf8("hello world")),
            stringData(
                "text/html",
                Compression.GZIP,
                "file.name.other",
                singletonList(objIdFromString("1234")),
                copyFromUtf8("hello world")),
            stringData(
                "text/html",
                Compression.GZIP,
                "file.name.other",
                asList(objIdFromString("1234"), objIdFromString("8888")),
                copyFromUtf8("hello foo")));

    // Generated IDs must be unique
    soft.assertThat(objs.stream().map(Obj::id).collect(Collectors.toSet())).hasSize(objs.size());
  }
}
