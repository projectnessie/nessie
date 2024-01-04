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
package org.projectnessie.versioned.storage.commontests;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.nessie.relocated.protobuf.ByteString.copyFromUtf8;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_INCREMENTAL_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_SERIALIZED_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_REPOSITORY_ID;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.INCREMENTAL_ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.REMOVE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.objtypes.Compression.NONE;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;
import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.objtypes.JsonObj.json;
import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.INDEX;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.INDEX_SEGMENTS;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.REF;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.STRING;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.TAG;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.VALUE;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.common.objtypes.TagObj.tag;
import static org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj.uniqueId;
import static org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj.uuidToBytes;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.BooleanArrayAssert;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.objtypes.JsonObj;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.objtypes.TagObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.ImmutableReference;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.ObjTypes;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.commontests.objtypes.AnotherTestObj;
import org.projectnessie.versioned.storage.commontests.objtypes.ImmutableJsonTestBean;
import org.projectnessie.versioned.storage.commontests.objtypes.JsonTestBean;
import org.projectnessie.versioned.storage.commontests.objtypes.SimpleTestObj;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

/** Basic {@link Persist} tests to be run by every implementation. */
@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class AbstractBasePersistTests {
  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  @Test
  public void singleReferenceCreateMarkDeletedPurge() throws Exception {
    ObjId pointer = randomObjId();
    ObjId otherId = objIdFromString("88776655");
    String name = "some-reference-name";
    long created = 12345L;
    ObjId extended = randomObjId();
    Reference create = reference(name, pointer, false, created, extended);
    Reference deleted = create.withDeleted(true);

    soft.assertThat(persist.addReference(create)).isEqualTo(create);
    soft.assertThatThrownBy(() -> persist.addReference(create))
        .isInstanceOf(RefAlreadyExistsException.class);
    soft.assertThat(persist.fetchReference(name)).isEqualTo(create);
    soft.assertThat(persist.fetchReferences(new String[] {name}))
        .hasSize(1)
        .containsExactly(create);
    soft.assertThat(persist.markReferenceAsDeleted(create)).isEqualTo(deleted);
    soft.assertThat(persist.fetchReference(name)).isEqualTo(deleted);
    soft.assertThat(persist.fetchReferences(new String[] {name}))
        .hasSize(1)
        .containsExactly(deleted);
    soft.assertThatThrownBy(() -> persist.markReferenceAsDeleted(create))
        .isInstanceOf(RefConditionFailedException.class);
    soft.assertThatThrownBy(
            () -> persist.markReferenceAsDeleted(create.forNewPointer(otherId, persist.config())))
        .isInstanceOf(RefConditionFailedException.class);
    soft.assertThatThrownBy(
            () ->
                persist.markReferenceAsDeleted(
                    create.forNewPointer(otherId, persist.config()).withDeleted(true)))
        .isInstanceOf(RefConditionFailedException.class);
    soft.assertThatThrownBy(() -> persist.markReferenceAsDeleted(deleted))
        .isInstanceOf(RefConditionFailedException.class);
    soft.assertThatThrownBy(() -> persist.addReference(create))
        .isInstanceOf(RefAlreadyExistsException.class);

    soft.assertThatThrownBy(() -> persist.purgeReference(reference(name, otherId, false, 0L, null)))
        .isInstanceOf(RefConditionFailedException.class);
    soft.assertThatCode(() -> persist.purgeReference(deleted)).doesNotThrowAnyException();
    soft.assertThatThrownBy(() -> persist.purgeReference(deleted))
        .isInstanceOf(RefNotFoundException.class);
    soft.assertThatThrownBy(() -> persist.markReferenceAsDeleted(create))
        .isInstanceOf(RefNotFoundException.class);
    soft.assertThat(persist.fetchReference(name)).isNull();
    soft.assertThat(persist.fetchReferences(new String[] {name})).hasSize(1).containsOnlyNulls();
  }

  @Test
  public void updateReference() throws Exception {
    ObjId initialPointer = objIdFromString("0000");
    ObjId pointer1 = objIdFromString("0001");
    ObjId pointer2 = objIdFromString("0002");
    ObjId pointer3 = objIdFromString("0003");

    long created = 12345L;
    ObjId extended = randomObjId();

    Reference create = reference("some-reference-name", initialPointer, false, created, extended);
    Reference assigned1 = create.forNewPointer(pointer1, persist.config());
    Reference assigned2 = create.forNewPointer(pointer2, persist.config());
    Reference deleted = assigned2.withDeleted(true);

    soft.assertThat(persist.addReference(create)).isEqualTo(create);
    soft.assertThat(persist.fetchReference("some-reference-name")).isEqualTo(create);

    // Wrong current pointer
    soft.assertThatThrownBy(() -> persist.updateReferencePointer(assigned1, initialPointer))
        .isInstanceOf(RefConditionFailedException.class);
    soft.assertThat(persist.fetchReference("some-reference-name")).isEqualTo(create);

    // Wrong created timestamp
    soft.assertThatThrownBy(
            () ->
                persist.updateReferencePointer(
                    reference(
                        create.name(),
                        create.pointer(),
                        create.deleted(),
                        create.createdAtMicros() + 1,
                        create.extendedInfoObj()),
                    pointer1))
        .isInstanceOf(RefConditionFailedException.class);
    soft.assertThat(persist.fetchReference("some-reference-name")).isEqualTo(create);

    // Wrong created extended info obj
    soft.assertThatThrownBy(
            () ->
                persist.updateReferencePointer(
                    reference(
                        create.name(),
                        create.pointer(),
                        create.deleted(),
                        create.createdAtMicros(),
                        randomObjId()),
                    pointer1))
        .isInstanceOf(RefConditionFailedException.class);
    soft.assertThat(persist.fetchReference("some-reference-name")).isEqualTo(create);

    // Correct current pointer
    Reference updated1 = persist.updateReferencePointer(create, pointer1);
    soft.assertThat(updated1)
        .isEqualTo(
            ImmutableReference.builder()
                .from(assigned1)
                .previousPointers(updated1.previousPointers())
                .build())
        .extracting(Reference::previousPointers, list(Reference.PreviousPointer.class))
        .extracting(Reference.PreviousPointer::pointer)
        .containsExactly(create.pointer());
    soft.assertThat(persist.fetchReference("some-reference-name")).isEqualTo(updated1);

    // Wrong current pointer
    soft.assertThatThrownBy(() -> persist.updateReferencePointer(assigned2, initialPointer))
        .isInstanceOf(RefConditionFailedException.class);
    soft.assertThat(persist.fetchReference("some-reference-name")).isEqualTo(updated1);

    // Wrong created timestamp
    soft.assertThatThrownBy(
            () ->
                persist.updateReferencePointer(
                    reference(
                        updated1.name(),
                        updated1.pointer(),
                        updated1.deleted(),
                        updated1.createdAtMicros() + 1,
                        updated1.extendedInfoObj(),
                        updated1.previousPointers()),
                    pointer2))
        .isInstanceOf(RefConditionFailedException.class);
    soft.assertThat(persist.fetchReference("some-reference-name")).isEqualTo(updated1);

    // Wrong created extended info obj
    soft.assertThatThrownBy(
            () ->
                persist.updateReferencePointer(
                    reference(
                        updated1.name(),
                        updated1.pointer(),
                        updated1.deleted(),
                        updated1.createdAtMicros(),
                        randomObjId(),
                        updated1.previousPointers()),
                    pointer2))
        .isInstanceOf(RefConditionFailedException.class);
    soft.assertThat(persist.fetchReference("some-reference-name")).isEqualTo(updated1);

    // Correct current pointer
    Reference updated2 = persist.updateReferencePointer(updated1, pointer2);
    soft.assertThat(updated2)
        .isEqualTo(
            ImmutableReference.builder()
                .from(assigned2)
                .previousPointers(updated2.previousPointers())
                .build())
        .extracting(Reference::previousPointers, list(Reference.PreviousPointer.class))
        .extracting(Reference.PreviousPointer::pointer)
        .containsExactly(updated1.pointer(), create.pointer());
    soft.assertThat(persist.fetchReference("some-reference-name")).isEqualTo(updated2);

    // "Bump" from current pointer to current pointer (no update)
    Reference updated2b = persist.updateReferencePointer(updated2, updated2.pointer());
    soft.assertThat(updated2b).isEqualTo(updated2);
    soft.assertThat(persist.fetchReference("some-reference-name")).isEqualTo(updated2b);

    // Delete it (must not update)
    Reference deletedRef = persist.markReferenceAsDeleted(updated2b);
    soft.assertThat(deletedRef)
        .isEqualTo(
            ImmutableReference.builder()
                .from(deleted)
                .previousPointers(deletedRef.previousPointers())
                .build())
        .extracting(Reference::previousPointers, list(Reference.PreviousPointer.class))
        .extracting(Reference.PreviousPointer::pointer)
        .containsExactly(updated1.pointer(), create.pointer());
    soft.assertThatThrownBy(() -> persist.updateReferencePointer(deleted, pointer3))
        .isInstanceOf(RefConditionFailedException.class);
    soft.assertThat(persist.fetchReference("some-reference-name")).isEqualTo(deletedRef);

    // Some other name - must not create a reference for it
    soft.assertThatThrownBy(
            () ->
                persist.updateReferencePointer(
                    reference("other-reference-name", initialPointer, false, created, extended),
                    pointer1))
        .isInstanceOf(RefNotFoundException.class);
    soft.assertThat(persist.fetchReference("other-reference-name")).isNull();
  }

  @Test
  public void fetchManyReferences() throws Exception {
    List<Reference> references =
        IntStream.range(0, 305)
            .mapToObj(
                i ->
                    reference(
                        "ref-" + i,
                        randomObjId(),
                        false,
                        ThreadLocalRandom.current().nextLong(1L, Long.MAX_VALUE),
                        randomObjId()))
            .collect(Collectors.toList());
    for (Reference reference : references) {
      persist.addReference(reference);
    }

    soft.assertThat(
            persist.fetchReferences(
                references.stream().map(Reference::name).toArray(String[]::new)))
        .containsExactlyElementsOf(references);
  }

  @Test
  public void fetchManyReferencesEmpty() throws Exception {
    List<Reference> references =
        asList(
            reference("foo", randomObjId(), false, 1L, randomObjId()),
            reference("bar", randomObjId(), false, 2L, randomObjId()),
            reference("baz", randomObjId(), false, 3L, null));
    for (Reference reference : references) {
      persist.addReference(reference);
    }

    soft.assertThat(
            persist.fetchReferences(
                references.stream().map(Reference::name).toArray(String[]::new)))
        .containsExactlyElementsOf(references);

    soft.assertThat(
            persist.fetchReferences(
                new String[] {null, "foo", "not there", "bar", "non-existing", "baz", null}))
        .containsExactly(
            null, references.get(0), null, references.get(1), null, references.get(2), null);

    soft.assertThat(persist.fetchReferences(new String[205])).hasSize(205).containsOnlyNulls();
  }

  public static Stream<Obj> allObjectTypeSamples() {
    String nonAscii = "äöüß^€éèêµ";
    byte[] someFooBar = "Some foo bar baz".getBytes(UTF_8);
    ByteString fooBar = ByteString.copyFrom(someFooBar);
    StoreIndex<CommitOp> emptyIndex = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
    index.add(indexElement(key("foo", "bar"), commitOp(ADD, 42, objIdFromString("1234"))));
    index.add(indexElement(key(nonAscii), commitOp(ADD, 3, objIdFromString("4567"))));
    index.add(indexElement(key("boo", nonAscii), commitOp(REMOVE, 4, objIdFromString("cafe"))));
    index.add(
        indexElement(key("moo", "woof"), commitOp(INCREMENTAL_ADD, 2, objIdFromString("8888"))));

    return Stream.of(
        // 1
        contentValue(randomObjId(), randomContentId(), 1, fooBar),
        contentValue(randomObjId(), randomContentId(), 127, fooBar),
        contentValue(randomObjId(), randomContentId(), 11, fooBar),
        contentValue(randomObjId(), randomContentId(), 33, fooBar),
        // 5
        contentValue(randomObjId(), randomContentId(), 42, fooBar),
        indexSegments(
            randomObjId(), singletonList(indexStripe(key("xyy"), key("xzz"), randomObjId()))),
        indexSegments(
            randomObjId(),
            asList(
                indexStripe(key(nonAscii), key(nonAscii), randomObjId()),
                indexStripe(key("moo", "woof"), key("zoo", "woof"), randomObjId()))),
        index(randomObjId(), emptyIndex.serialize()),
        index(randomObjId(), index.serialize()),
        // 10
        tag(randomObjId(), "tag-message", newCommitHeaders().add("Foo", "Bar").build(), fooBar),
        tag(randomObjId(), null, null, ByteString.EMPTY),
        commitBuilder()
            .id(randomObjId())
            .created(123L)
            .headers(
                newCommitHeaders().add("Foo", "bar").add("Foo", "baz").add("meep", "moo").build())
            .message("hello world")
            .referenceIndex(objIdFromString("1234567890123456"))
            .addTail(objIdFromString("1234567890000000"))
            .addTail(objIdFromString("aaaaaaaaaaaaaaaa"))
            .addTail(objIdFromString("abababababababab"))
            .addTail(objIdFromString("deadbeefcafebabe"))
            .addTail(objIdFromString("0000000000000000"))
            .addSecondaryParents(objIdFromString("1234567cc8900000"))
            .addSecondaryParents(objIdFromString("aaaaaccaaaaaaaaa"))
            .addSecondaryParents(objIdFromString("abaccbababababab"))
            .addSecondaryParents(objIdFromString("dcceadbeefcafeba"))
            .addSecondaryParents(objIdFromString("cc00000000000000"))
            .addReferenceIndexStripes(indexStripe(key("abc"), key("def"), randomObjId()))
            .addReferenceIndexStripes(indexStripe(key("def"), key("ghi"), randomObjId()))
            .addReferenceIndexStripes(indexStripe(key("ghi"), key("jkl"), randomObjId()))
            .incrementalIndex(index.serialize())
            .commitType(CommitType.INTERNAL)
            .seq(42L)
            .build(),
        commitBuilder()
            .id(randomObjId())
            .created(123L)
            .headers(EMPTY_COMMIT_HEADERS)
            .message("")
            .incrementalIndex(emptyIndex.serialize())
            .incompleteIndex(true)
            .seq(42L)
            .build(),
        stringData(randomObjId(), "text/plain", NONE, null, emptyList(), ByteString.EMPTY),
        // 15
        stringData(
            randomObjId(),
            "text/markdown",
            Compression.GZIP,
            "filename",
            asList(
                objIdFromString("1234567890000000"),
                objIdFromString("aaaaaaaaaaaaaaaa"),
                objIdFromString("abababababababab"),
                objIdFromString("deadbeefcafebabe"),
                objIdFromString("0000000000000000")),
            copyFromUtf8("This is not a markdown")),
        ref(randomObjId(), "foo", randomObjId(), 123L, null),
        ref(randomObjId(), "bar", randomObjId(), 456L, randomObjId()),
        uniqueId(randomObjId(), "space", uuidToBytes(UUID.randomUUID())),
        // custom object types
        SimpleTestObj.builder()
            .id(randomObjId())
            .parent(randomObjId())
            .text("foo".repeat(4000))
            .number(42.42d)
            .map(Map.of("k1", "v1".repeat(4000), "k2", "v2".repeat(4000)))
            .list(List.of("a", "b", "c"))
            .optional("optional")
            .instant(Instant.ofEpochMilli(1234567890L))
            .build(),
        SimpleTestObj.builder()
            .id(randomObjId())
            .parent(randomObjId())
            .text("foo")
            .number(42.42d)
            .map(Map.of("k1", "v1", "k2", "v2"))
            .list(List.of("a", "b", "c"))
            .optional("optional")
            .instant(Instant.ofEpochMilli(1234567890L))
            .build(),
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
        AnotherTestObj.builder()
            .id(randomObjId())
            .parent(randomObjId())
            .text("foo")
            .number(42.42d)
            .map(Map.of("k1", "v1", "k2", "v2"))
            .list(List.of("a", "b", "c"))
            .optional("optional")
            .instant(Instant.ofEpochMilli(1234567890L))
            .build(),
        AnotherTestObj.builder().id(randomObjId()).build(),
        // JSON objects
        // scalar types
        json(randomObjId(), "text"),
        json(randomObjId(), 123),
        json(randomObjId(), 123.456d),
        json(randomObjId(), true),
        json(randomObjId(), String.class, "text"),
        json(randomObjId(), Integer.class, 123),
        json(randomObjId(), Number.class, 123),
        json(randomObjId(), Double.class, 123.456d),
        json(randomObjId(), Boolean.class, true),
        // arrays / maps
        json(randomObjId(), List.of("a", "b", "c")),
        json(randomObjId(), List.class, List.of("a", "b", "c")),
        json(randomObjId(), "java.util.List<java.lang.String>", List.of("a", "b", "c")),
        json(randomObjId(), ImmutableList.class, List.of("a", "b", "c")),
        json(randomObjId(), Map.of("k1", "v1", "k2", "v2")),
        json(randomObjId(), Map.class, Map.of("k1", "v1", "k2", "v2")),
        json(
            randomObjId(),
            "java.util.Map<java.lang.String,java.lang.String>",
            Map.of("k1", "v1", "k2", "v2")),
        json(randomObjId(), ImmutableMap.class, Map.of("k1", "v1", "k2", "v2")),
        // objects
        json(
            randomObjId(),
            ImmutableJsonTestBean.builder()
                .parent(randomObjId())
                .text("foo")
                .number(42.42d)
                .map(Map.of("k1", "v1", "k2", "v2"))
                .list(List.of("a", "b", "c"))
                .optional("optional")
                .instant(Instant.ofEpochMilli(1234567890L))
                .build()),
        json(
            randomObjId(),
            JsonTestBean.class,
            ImmutableJsonTestBean.builder()
                .parent(randomObjId())
                .text("foo")
                .number(42.42d)
                .map(Map.of("k1", "v1", "k2", "v2"))
                .list(List.of("a", "b", "c"))
                .optional("optional")
                .instant(Instant.ofEpochMilli(1234567890L))
                .build()),
        // large objects
        json(
            randomObjId(),
            ImmutableJsonTestBean.builder()
                .parent(randomObjId())
                .text("foo".repeat(4000))
                .number(42.42d)
                .map(Map.of("k1", "v1".repeat(4000), "k2", "v2".repeat(4000)))
                .list(List.of("a", "b", "c"))
                .optional("optional")
                .instant(Instant.ofEpochMilli(1234567890L))
                .build()),
        // lists and maps of objects
        json(
            randomObjId(),
            "java.util.List<org.projectnessie.versioned.storage.commontests.objtypes.JsonTestBean>",
            List.of(
                ImmutableJsonTestBean.builder()
                    .parent(randomObjId())
                    .text("foo")
                    .number(42.42d)
                    .map(Map.of("k1", "v1", "k2", "v2"))
                    .list(List.of("a", "b", "c"))
                    .optional("optional")
                    .instant(Instant.ofEpochMilli(1234567890L))
                    .build(),
                ImmutableJsonTestBean.builder()
                    .parent(randomObjId())
                    .text("bar")
                    .number(43.43d)
                    .map(Map.of("k2", "v2", "k3", "v3"))
                    .list(List.of("d", "e", "f"))
                    .optional(Optional.empty())
                    .instant(null)
                    .build())),
        json(
            randomObjId(),
            "java.util.Map<java.lang.String,org.projectnessie.versioned.storage.commontests.objtypes.JsonTestBean>",
            Map.of(
                "foo",
                ImmutableJsonTestBean.builder()
                    .parent(randomObjId())
                    .text("foo")
                    .number(42.42d)
                    .map(Map.of("k1", "v1", "k2", "v2"))
                    .list(List.of("a", "b", "c"))
                    .optional("optional")
                    .instant(Instant.ofEpochMilli(1234567890L))
                    .build(),
                "bar",
                ImmutableJsonTestBean.builder()
                    .parent(randomObjId())
                    .text("bar")
                    .number(43.43d)
                    .map(Map.of("k2", "v2", "k3", "v3"))
                    .list(List.of("d", "e", "f"))
                    .optional(Optional.empty())
                    .instant(null)
                    .build())),
        // empty objects / null
        json(randomObjId(), ImmutableJsonTestBean.builder().build()),
        json(randomObjId(), JsonTestBean.class, ImmutableJsonTestBean.builder().build()),
        json(randomObjId(), String.class, null),
        json(randomObjId(), List.class, null),
        json(randomObjId(), Map.class, null),
        json(randomObjId(), JsonTestBean.class, null),
        json(randomObjId(), "java.util.List<java.lang.String>", null));
  }

  static StandardObjType typeDifferentThan(ObjType type) {
    if (type instanceof StandardObjType) {
      switch (((StandardObjType) type)) {
        case COMMIT:
          return VALUE;
        case VALUE:
          return COMMIT;
        case INDEX_SEGMENTS:
          return TAG;
        case INDEX:
          return REF;
        case REF:
          return INDEX;
        case TAG:
          return STRING;
        case STRING:
          return INDEX_SEGMENTS;
        case UNIQUE:
          return REF;
        default:
          // fall through
      }
    }
    if (type.equals(SimpleTestObj.TYPE)) {
      return COMMIT;
    }
    if (type.equals(AnotherTestObj.TYPE)) {
      return VALUE;
    }
    if (type.equals(JsonObj.TYPE)) {
      return INDEX;
    }
    throw new IllegalArgumentException(type.name());
  }

  @ParameterizedTest
  @MethodSource("allObjectTypeSamples")
  public void singleObjectCreateDelete(Obj obj) throws Exception {
    soft.assertThatThrownBy(() -> persist.fetchObj(obj.id()))
        .isInstanceOf(ObjNotFoundException.class);
    soft.assertThatThrownBy(() -> persist.fetchObjType(obj.id()))
        .isInstanceOf(ObjNotFoundException.class);
    soft.assertThatThrownBy(
            () -> persist.fetchTypedObj(obj.id(), obj.type(), obj.type().targetClass()))
        .isInstanceOf(ObjNotFoundException.class);
    soft.assertThatThrownBy(() -> persist.fetchObjs(new ObjId[] {obj.id()}))
        .isInstanceOf(ObjNotFoundException.class);

    soft.assertThat(persist.storeObj(obj)).isTrue();

    soft.assertThat(persist.fetchObj(obj.id())).isEqualTo(obj);
    soft.assertThat(persist.fetchObjType(obj.id())).isEqualTo(obj.type());
    soft.assertThat(persist.fetchTypedObj(obj.id(), obj.type(), obj.type().targetClass()))
        .isEqualTo(obj);
    StandardObjType otherType = typeDifferentThan(obj.type());
    soft.assertThatThrownBy(
            () -> persist.fetchTypedObj(obj.id(), otherType, otherType.targetClass()))
        .isInstanceOf(ObjNotFoundException.class);
    soft.assertThat(persist.fetchObjs(new ObjId[] {obj.id()})).containsExactly(obj);

    soft.assertThatCode(() -> persist.deleteObj(obj.id())).doesNotThrowAnyException();
    soft.assertThatCode(() -> persist.deleteObjs(new ObjId[] {obj.id()}))
        .doesNotThrowAnyException();

    soft.assertThatThrownBy(() -> persist.fetchObj(obj.id()))
        .isInstanceOf(ObjNotFoundException.class);
    soft.assertThatThrownBy(() -> persist.fetchObjType(obj.id()))
        .isInstanceOf(ObjNotFoundException.class);
    soft.assertThatThrownBy(
            () -> persist.fetchTypedObj(obj.id(), obj.type(), obj.type().targetClass()))
        .isInstanceOf(ObjNotFoundException.class);
    soft.assertThatThrownBy(() -> persist.fetchObjs(new ObjId[] {obj.id()}))
        .isInstanceOf(ObjNotFoundException.class);
  }

  @ParameterizedTest
  @MethodSource("allObjectTypeSamples")
  public void doubleStoreObj(Obj obj) throws Exception {
    soft.assertThat(persist.storeObj(obj)).isTrue();
    soft.assertThat(persist.storeObj(obj)).isFalse();
  }

  @ParameterizedTest
  @MethodSource("allObjectTypeSamples")
  public void doubleStoreObjs(Obj obj) throws Exception {
    soft.assertThat(persist.storeObjs(new Obj[] {obj})).containsExactly(true);
    soft.assertThat(persist.storeObjs(new Obj[] {obj})).hasSize(1).containsOnly(false);
  }

  @Test
  public void multipleStoreAndFetch() throws Exception {
    Obj[] objs = allObjectTypeSamples().toArray(Obj[]::new);
    boolean[] results = persist.storeObjs(objs);
    soft.assertThat(results).doesNotContain(false);

    objs = persist.fetchObjs(stream(objs).map(Obj::id).toArray(ObjId[]::new));
    soft.assertThat(objs).doesNotContainNull();
  }

  @Test
  public void fetchNothing() throws Exception {
    soft.assertThat(persist.fetchObjs(new ObjId[0])).hasSize(0);
  }

  @Test
  public void storeAndFetchMany() throws Exception {
    List<TagObj> objects =
        IntStream.range(0, 957) // 957 is an arbitrary number, just not something "round"
            .mapToObj(i -> tag(randomObjId(), null, null, ByteString.copyFrom(new byte[42])))
            .collect(Collectors.toList());

    boolean[] results = persist.storeObjs(objects.toArray(new Obj[0]));
    soft.assertThat(results).hasSize(objects.size()).containsOnly(true);
    ObjId[] ids = objects.stream().map(Obj::id).toArray(ObjId[]::new);

    Obj[] fetched = persist.fetchObjs(ids);
    soft.assertThat(fetched).containsExactlyElementsOf(objects);
  }

  @Test
  public void multipleStoreObjs() throws Exception {
    Obj obj1 = tag(randomObjId(), null, null, ByteString.EMPTY);
    Obj obj2 = tag(randomObjId(), null, null, ByteString.EMPTY);
    Obj obj3 = tag(randomObjId(), null, null, ByteString.EMPTY);
    Obj obj4 = tag(randomObjId(), null, null, ByteString.EMPTY);
    Obj obj5 = tag(randomObjId(), null, null, ByteString.EMPTY);

    soft.assertThat(persist.storeObjs(new Obj[] {obj1})).containsExactly(true);
    soft.assertThat(persist.fetchObj(requireNonNull(obj1.id()))).isEqualTo(obj1);
    soft.assertThat(persist.fetchObjs(new ObjId[] {obj1.id()})).containsExactly(obj1);

    soft.assertThat(persist.storeObjs(new Obj[] {obj1, obj2})).containsExactly(false, true);
    soft.assertThat(persist.fetchObj(requireNonNull(obj2.id()))).isEqualTo(obj2);
    soft.assertThat(persist.fetchObjs(new ObjId[] {obj1.id(), obj2.id()}))
        .containsExactly(obj1, obj2);

    soft.assertThat(persist.storeObjs(new Obj[] {obj1, obj2, obj3}))
        .containsExactly(false, false, true);
    soft.assertThat(persist.fetchObj(requireNonNull(obj3.id()))).isEqualTo(obj3);
    soft.assertThat(persist.fetchObjs(new ObjId[] {obj1.id(), obj2.id(), obj3.id()}))
        .containsExactly(obj1, obj2, obj3);

    soft.assertThat(persist.storeObjs(new Obj[] {obj1, obj2, obj3, obj4}))
        .containsExactly(false, false, false, true);
    soft.assertThat(persist.fetchObj(requireNonNull(obj4.id()))).isEqualTo(obj4);
    soft.assertThat(persist.fetchObjs(new ObjId[] {obj1.id(), obj2.id(), obj3.id(), obj4.id()}))
        .containsExactly(obj1, obj2, obj3, obj4);

    soft.assertThat(persist.storeObjs(new Obj[] {obj1, obj2, obj3, obj4, obj5}))
        .containsExactly(false, false, false, false, true);
    soft.assertThat(persist.fetchObj(requireNonNull(obj5.id()))).isEqualTo(obj5);
    soft.assertThat(
            persist.fetchObjs(new ObjId[] {obj1.id(), obj2.id(), obj3.id(), obj4.id(), obj5.id()}))
        .containsExactly(obj1, obj2, obj3, obj4, obj5);
  }

  @Test
  public void fetchEmptyObjId() {
    soft.assertThatThrownBy(() -> persist.fetchObj(EMPTY_OBJ_ID))
        .isInstanceOf(ObjNotFoundException.class);
    soft.assertThatThrownBy(() -> persist.fetchObjType(EMPTY_OBJ_ID))
        .isInstanceOf(ObjNotFoundException.class);
    soft.assertThatThrownBy(() -> persist.fetchTypedObj(EMPTY_OBJ_ID, COMMIT, CommitObj.class))
        .isInstanceOf(ObjNotFoundException.class);
  }

  @Test
  public void fetchNonExistingObj() {
    ObjId id = randomObjId();
    soft.assertThatThrownBy(() -> persist.fetchObj(id))
        .isInstanceOf(ObjNotFoundException.class)
        .asInstanceOf(type(ObjNotFoundException.class))
        .extracting(ObjNotFoundException::objIds, list(ObjId.class))
        .containsExactly(id);

    soft.assertThatThrownBy(() -> persist.fetchObjType(id))
        .isInstanceOf(ObjNotFoundException.class)
        .asInstanceOf(type(ObjNotFoundException.class))
        .extracting(ObjNotFoundException::objIds, list(ObjId.class))
        .containsExactly(id);

    soft.assertThatThrownBy(() -> persist.fetchTypedObj(id, COMMIT, CommitObj.class))
        .isInstanceOf(ObjNotFoundException.class)
        .asInstanceOf(type(ObjNotFoundException.class))
        .extracting(ObjNotFoundException::objIds, list(ObjId.class))
        .containsExactly(id);

    soft.assertThatThrownBy(() -> persist.fetchObjs(new ObjId[] {EMPTY_OBJ_ID, id}))
        .isInstanceOf(ObjNotFoundException.class)
        .asInstanceOf(type(ObjNotFoundException.class))
        .extracting(ObjNotFoundException::objIds, list(ObjId.class))
        .containsExactly(EMPTY_OBJ_ID, id);

    ObjId id2 = randomObjId();

    soft.assertThatThrownBy(() -> persist.fetchObjs(new ObjId[] {EMPTY_OBJ_ID, id, id2}))
        .isInstanceOf(ObjNotFoundException.class)
        .asInstanceOf(type(ObjNotFoundException.class))
        .extracting(ObjNotFoundException::objIds, list(ObjId.class))
        .containsExactlyInAnyOrder(EMPTY_OBJ_ID, id, id2);
  }

  @Test
  public void storeCommitObjHardObjectSizeLimit() {
    int hardLimit = persist.hardObjectSizeLimit();
    assumeThat(hardLimit).isNotEqualTo(Integer.MAX_VALUE);

    StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
    for (int i = 0, sz = 0; sz < hardLimit; i++, sz += 80) {
      index.add(
          indexElement(
              key("foo-" + i, "12345678901234567890123456789012345678901234567890"),
              commitOp(ADD, 42, randomObjId())));
    }

    verifyObjSizeLimit(persist, index);
  }

  @Test
  public void storeCommitObjCheckSize(
      @NessieStoreConfig(name = CONFIG_MAX_INCREMENTAL_INDEX_SIZE, value = "1024")
          @NessieStoreConfig(name = CONFIG_MAX_SERIALIZED_INDEX_SIZE, value = "1024")
          @NessiePersist
          Persist persist) {
    StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
    for (int i = 0; i < 25; i++) {
      index.add(
          indexElement(
              key("foo-" + i, "12345678901234567890123456789012345678901234567890"),
              commitOp(ADD, 42, randomObjId())));
    }

    verifyObjSizeLimit(persist, index);
  }

  private void verifyObjSizeLimit(Persist persist, StoreIndex<CommitOp> index) {
    soft.assertThatThrownBy(() -> persist.storeObj(index(randomObjId(), index.serialize())))
        .isInstanceOf(ObjTooLargeException.class);
    soft.assertThatThrownBy(
            () ->
                persist.storeObj(
                    commitBuilder()
                        .id(randomObjId())
                        .created(123L)
                        .seq(123)
                        .message("msg")
                        .incrementalIndex(index.serialize())
                        .headers(EMPTY_COMMIT_HEADERS)
                        .build()))
        .isInstanceOf(ObjTooLargeException.class);
    soft.assertThatThrownBy(
            () ->
                persist.storeObjs(
                    new Obj[] {
                      commitBuilder()
                          .id(randomObjId())
                          .created(123L)
                          .seq(123)
                          .message("msg")
                          .incrementalIndex(index.serialize())
                          .headers(EMPTY_COMMIT_HEADERS)
                          .build()
                    }))
        .isInstanceOf(ObjTooLargeException.class);
  }

  @Test
  public void scanAllObjects(
      @NessieStoreConfig(name = CONFIG_REPOSITORY_ID, value = "some-other") @NessiePersist
          Persist otherRepo) {
    soft.assertThat(persist.config().repositoryId())
        .isNotEqualTo(otherRepo.config().repositoryId());

    ArrayList<Obj> list1;
    try (CloseableIterator<Obj> iter = persist.scanAllObjects(ObjTypes.allObjTypes())) {
      list1 = newArrayList(iter);
    }
    ArrayList<Obj> list2;
    try (CloseableIterator<Obj> iter = otherRepo.scanAllObjects(ObjTypes.allObjTypes())) {
      list2 = newArrayList(iter);
    }
    soft.assertThat(list1).isNotEmpty().doesNotContainAnyElementsOf(list2);
    soft.assertThat(list2).isNotEmpty().doesNotContainAnyElementsOf(list1);

    try (CloseableIterator<Obj> iter = otherRepo.scanAllObjects(Set.of(COMMIT))) {
      soft.assertThat(newArrayList(iter)).isNotEmpty().allMatch(o -> o.type() == COMMIT);
    }
    try (CloseableIterator<Obj> iter = otherRepo.scanAllObjects(Set.of(COMMIT))) {
      soft.assertThat(newArrayList(iter)).isNotEmpty().allMatch(o -> o.type() == COMMIT);
    }
  }

  @ParameterizedTest
  @MethodSource("allObjectTypeSamples")
  public void upsertNonExisting(Obj obj) throws Exception {
    soft.assertThatThrownBy(() -> persist.fetchObj(obj.id()))
        .isInstanceOf(ObjNotFoundException.class);
    persist.upsertObj(obj);
    soft.assertThat(persist.fetchObj(obj.id())).isEqualTo(obj);
  }

  @ParameterizedTest
  @MethodSource("allObjectTypeSamples")
  public void upsertNonExistingBulk(Obj obj) throws Exception {
    soft.assertThatThrownBy(() -> persist.fetchObj(obj.id()))
        .isInstanceOf(ObjNotFoundException.class);
    persist.upsertObjs(new Obj[] {obj});
    soft.assertThat(persist.fetchObj(obj.id())).isEqualTo(obj);
  }

  @ParameterizedTest
  @MethodSource("allObjectTypeSamples")
  public void updateSingle(Obj obj) throws Exception {
    persist.storeObj(obj);
    Obj newObj = updateObjChange(obj);
    if (newObj == null) {
      return;
    }

    soft.assertThat(newObj).isNotEqualTo(obj);

    persist.upsertObj(obj);

    soft.assertThat(persist.fetchObj(obj.id())).isEqualTo(obj);

    persist.upsertObj(newObj);

    soft.assertThat(persist.fetchObj(obj.id())).isEqualTo(newObj);
  }

  @Test
  public void updateManyObjects() throws Exception {
    Supplier<CommitObj> newCommit =
        () -> {
          StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
          index.add(
              indexElement(key("updated", "added", "key"), commitOp(ADD, 123, randomObjId())));
          index.add(
              indexElement(key("updated", "removed", "key"), commitOp(REMOVE, 123, randomObjId())));

          return commitBuilder()
              .id(randomObjId())
              .created(123L)
              .headers(
                  newCommitHeaders().add("Foo", "bar").add("Foo", "baz").add("meep", "moo").build())
              .message("hello world")
              .referenceIndex(objIdFromString("1234567890123456"))
              .addTail(objIdFromString("1234567890000000"))
              .addTail(objIdFromString("aaaaaaaaaaaaaaaa"))
              .addTail(objIdFromString("abababababababab"))
              .addTail(objIdFromString("deadbeefcafebabe"))
              .addTail(objIdFromString("0000000000000000"))
              .addSecondaryParents(objIdFromString("1234567cc8900000"))
              .addSecondaryParents(objIdFromString("aaaaaccaaaaaaaaa"))
              .addSecondaryParents(objIdFromString("abaccbababababab"))
              .addSecondaryParents(objIdFromString("dcceadbeefcafeba"))
              .addSecondaryParents(objIdFromString("cc00000000000000"))
              .addReferenceIndexStripes(indexStripe(key("abc"), key("def"), randomObjId()))
              .addReferenceIndexStripes(indexStripe(key("def"), key("ghi"), randomObjId()))
              .addReferenceIndexStripes(indexStripe(key("ghi"), key("jkl"), randomObjId()))
              .incrementalIndex(index.serialize())
              .commitType(CommitType.INTERNAL)
              .seq(42L)
              .build();
        };

    Obj[] objs = IntStream.range(0, 200).mapToObj(x -> newCommit.get()).toArray(Obj[]::new);
    Obj[] newObjs =
        IntStream.range(0, 400)
            .mapToObj(i -> ((i & 1) == 0) ? objs[i / 2] : newCommit.get())
            .toArray(Obj[]::new);

    persist.storeObjs(objs);
    soft.assertThat(persist.fetchObjs(stream(objs).map(Obj::id).toArray(ObjId[]::new)))
        .containsExactly(objs);

    persist.upsertObjs(newObjs);
    soft.assertThat(persist.fetchObjs(stream(newObjs).map(Obj::id).toArray(ObjId[]::new)))
        .containsExactly(newObjs);
  }

  @Test
  public void updateMultipleObjects() throws Exception {
    Obj[] objs = allObjectTypeSamples().toArray(Obj[]::new);
    Obj[] newObjs = stream(objs).map(AbstractBasePersistTests::updateObjChange).toArray(Obj[]::new);

    for (int i = 0; i < objs.length; i++) {
      soft.assertThat(newObjs[i]).isNotEqualTo(objs[i]);
    }

    persist.storeObjs(objs);
    soft.assertThat(persist.fetchObjs(stream(objs).map(Obj::id).toArray(ObjId[]::new)))
        .containsExactly(objs);

    persist.upsertObjs(newObjs);
    soft.assertThat(persist.fetchObjs(stream(objs).map(Obj::id).toArray(ObjId[]::new)))
        .containsExactly(newObjs);
  }

  public static Obj updateObjChange(Obj obj) {
    ObjType type = obj.type();
    if (type instanceof StandardObjType) {
      switch (((StandardObjType) type)) {
        case COMMIT:
          StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
          index.add(
              indexElement(key("updated", "added", "key"), commitOp(ADD, 123, randomObjId())));
          index.add(
              indexElement(key("updated", "removed", "key"), commitOp(REMOVE, 123, randomObjId())));
          CommitObj c = ((CommitObj) obj);
          return commitBuilder()
              .id(obj.id())
              .created(123123L)
              .headers(newCommitHeaders().add("update", "that").build())
              .message("updated commit")
              .incrementalIndex(index.serialize())
              .commitType(CommitType.values()[c.commitType().ordinal() ^ 1])
              .seq(1231231253L)
              .incompleteIndex(!c.incompleteIndex())
              .build();
        case VALUE:
          return contentValue(obj.id(), randomContentId(), 123, copyFromUtf8("updated stuff"));
        case REF:
          return ref(obj.id(), "hello", randomObjId(), 42L, randomObjId());
        case INDEX:
          index = newStoreIndex(COMMIT_OP_SERIALIZER);
          index.add(
              indexElement(key("updated", "added", "key"), commitOp(ADD, 123, randomObjId())));
          index.add(
              indexElement(key("updated", "removed", "key"), commitOp(REMOVE, 123, randomObjId())));
          return index(obj.id(), index.serialize());
        case INDEX_SEGMENTS:
          return indexSegments(
              obj.id(), singletonList(indexStripe(key("abc"), key("def"), randomObjId())));
        case TAG:
          return tag(obj.id(), null, null, copyFromUtf8("updated-tag"));
        case STRING:
          return stringData(
              obj.id(),
              "text/plain",
              Compression.LZ4,
              "filename",
              asList(randomObjId(), randomObjId(), randomObjId(), randomObjId()),
              ByteString.copyFrom(new byte[123]));
        case UNIQUE:
          return uniqueId(obj.id(), "other_space", uuidToBytes(UUID.randomUUID()));
        default:
          // fall through
      }
    }
    if (obj instanceof SimpleTestObj) {
      return SimpleTestObj.builder()
          .id(obj.id())
          .parent(randomObjId())
          .text("updated")
          .number(43.43d)
          .map(Map.of("k2", "v2", "k3", "v3"))
          .list(List.of("b", "c", "d"))
          .build();
    }
    if (obj instanceof AnotherTestObj) {
      return AnotherTestObj.builder()
          .id(obj.id())
          .parent(randomObjId())
          .text("updated")
          .number(43.43d)
          .map(Map.of("k2", "v2", "k3", "v3"))
          .list(List.of("b", "c", "d"))
          .build();
    }
    if (obj instanceof JsonObj) {
      return json(
          obj.id(),
          ImmutableJsonTestBean.builder()
              .parent(randomObjId())
              .text("updated")
              .number(43.43d)
              .map(Map.of("k2", "v2", "k3", "v3"))
              .list(List.of("b", "c", "d"))
              .build());
    }
    throw new UnsupportedOperationException("Unknown object type " + type);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 3, 10, 50})
  public void scanAllObjects(int numObjs) throws Exception {
    IntFunction<ContentValueObj> valueObj =
        i -> contentValue("content-id-" + i, i, copyFromUtf8("value-" + i));
    IntFunction<StringObj> stringObj =
        i -> stringData("text/foo", NONE, "file-" + i, emptyList(), copyFromUtf8("value-" + i));
    IntFunction<CommitObj> commitObj =
        i ->
            commitBuilder()
                .id(objIdFromString(format("%08x%08x%08x%08x", i, i, i, i)))
                .addTail(EMPTY_OBJ_ID)
                .created(0L)
                .seq(i)
                .headers(EMPTY_COMMIT_HEADERS)
                .message("hello-" + i)
                .incrementalIndex(ByteString.EMPTY)
                .build();

    Obj[] values = IntStream.range(0, numObjs).mapToObj(valueObj).toArray(Obj[]::new);
    Obj[] strings = IntStream.range(0, numObjs).mapToObj(stringObj).toArray(Obj[]::new);
    Obj[] commits = IntStream.range(0, numObjs).mapToObj(commitObj).toArray(Obj[]::new);

    // Clear the already initialized repo...
    persist.erase();

    BooleanArrayAssert storedAssert = soft.assertThat(persist.storeObjs(values)).hasSize(numObjs);
    if (numObjs > 0) {
      storedAssert.containsOnly(true);
    }
    storedAssert = soft.assertThat(persist.storeObjs(strings)).hasSize(numObjs);
    if (numObjs > 0) {
      storedAssert.containsOnly(true);
    }
    storedAssert = soft.assertThat(persist.storeObjs(commits)).hasSize(numObjs);
    if (numObjs > 0) {
      storedAssert.containsOnly(true);
    }

    try (CloseableIterator<Obj> scan = persist.scanAllObjects(ObjTypes.allObjTypes())) {
      soft.assertThat(Lists.newArrayList(scan))
          .hasSize(3 * numObjs)
          .contains(values)
          .contains(strings)
          .contains(commits);
    }
    try (CloseableIterator<Obj> scan = persist.scanAllObjects(Set.of(VALUE, STRING))) {
      soft.assertThat(Lists.newArrayList(scan))
          .hasSize(2 * numObjs)
          .contains(values)
          .contains(strings);
    }
    try (CloseableIterator<Obj> scan = persist.scanAllObjects(Set.of(VALUE, COMMIT))) {
      soft.assertThat(Lists.newArrayList(scan))
          .hasSize(2 * numObjs)
          .contains(values)
          .contains(commits);
    }
    try (CloseableIterator<Obj> scan = persist.scanAllObjects(Set.of(COMMIT))) {
      soft.assertThat(Lists.newArrayList(scan)).containsExactlyInAnyOrder(commits);
    }
    try (CloseableIterator<Obj> scan = persist.scanAllObjects(Set.of(COMMIT, TAG, INDEX))) {
      soft.assertThat(Lists.newArrayList(scan)).containsExactlyInAnyOrder(commits);
    }
    try (CloseableIterator<Obj> scan = persist.scanAllObjects(Set.of(TAG, INDEX))) {
      soft.assertThat(Lists.newArrayList(scan)).isEmpty();
    }
  }

  /**
   * Make sure that objects <em>inserted</em> with {@link Persist#upsertObj(Obj)} and {@link
   * Persist#upsertObjs(Obj[])} can be retrieved with {@link Persist#fetchObjs(ObjId[])} and {@link
   * Persist#scanAllObjects(Set)}.
   */
  @Test
  public void createObjectsWithUpsertThenFetchAndScan() throws Exception {
    Obj[] objs = allObjectTypeSamples().toArray(Obj[]::new);
    Obj[] standardObjs =
        Arrays.stream(objs).filter(o -> o.type() instanceof StandardObjType).toArray(Obj[]::new);
    Obj[] jsonObjs = Arrays.stream(objs).filter(o -> o instanceof JsonObj).toArray(Obj[]::new);
    Obj[] customObjs =
        Arrays.stream(objs)
            .filter(o -> o instanceof SimpleTestObj || o instanceof AnotherTestObj)
            .toArray(Obj[]::new);

    persist.erase();

    persist.upsertObj(objs[0]);
    persist.upsertObjs(stream(objs).skip(1).toArray(Obj[]::new));

    soft.assertThat(persist.fetchObj(objs[0].id())).isEqualTo(objs[0]);

    soft.assertThat(persist.fetchObjs(stream(objs).map(Obj::id).toArray(ObjId[]::new)))
        .containsExactly(objs);

    try (CloseableIterator<Obj> scan = persist.scanAllObjects(ObjTypes.allObjTypes())) {
      soft.assertThat(Lists.newArrayList(scan)).containsExactlyInAnyOrder(objs);
    }
    try (CloseableIterator<Obj> scan =
        persist.scanAllObjects(Set.copyOf(EnumSet.allOf(StandardObjType.class)))) {
      soft.assertThat(Lists.newArrayList(scan)).containsExactlyInAnyOrder(standardObjs);
    }
    try (CloseableIterator<Obj> scan = persist.scanAllObjects(Set.of(JsonObj.TYPE))) {
      soft.assertThat(Lists.newArrayList(scan)).containsExactlyInAnyOrder(jsonObjs);
    }
    try (CloseableIterator<Obj> scan =
        persist.scanAllObjects(Set.of(SimpleTestObj.TYPE, AnotherTestObj.TYPE))) {
      soft.assertThat(Lists.newArrayList(scan)).containsExactlyInAnyOrder(customObjs);
    }
    try (CloseableIterator<Obj> scan = persist.scanAllObjects(Set.of(SimpleTestObj.TYPE))) {
      soft.assertThat(Lists.newArrayList(scan))
          .containsExactlyInAnyOrderElementsOf(
              stream(customObjs)
                  .filter(o -> o.type() == SimpleTestObj.TYPE)
                  .collect(Collectors.toList()));
    }
    try (CloseableIterator<Obj> scan = persist.scanAllObjects(Set.of(COMMIT))) {
      Obj[] expected = stream(objs).filter(c -> c.type() == COMMIT).toArray(Obj[]::new);
      soft.assertThat(newArrayList(scan)).containsExactlyInAnyOrder(expected);
    }
  }

  @Test
  public void nullHandlingInArrays() throws ObjTooLargeException, ObjNotFoundException {
    soft.assertThat(persist.storeObjs(new Obj[] {null})).containsExactly(false);
    soft.assertThatCode(() -> persist.upsertObjs(new Obj[] {null})).doesNotThrowAnyException();
    soft.assertThat(persist.fetchObjs(new ObjId[] {null})).containsExactly((Obj) null);
    soft.assertThatCode(() -> persist.deleteObjs(new ObjId[] {null})).doesNotThrowAnyException();
    soft.assertThat(persist.storeObjs(new Obj[] {null, null})).containsExactly(false, false);
    soft.assertThatCode(() -> persist.upsertObjs(new Obj[] {null, null}))
        .doesNotThrowAnyException();
    soft.assertThat(persist.fetchObjs(new ObjId[] {null, null})).containsExactly(null, null);
    soft.assertThatCode(() -> persist.deleteObjs(new ObjId[] {null, null}))
        .doesNotThrowAnyException();
  }

  public static String randomContentId() {
    return randomUUID().toString();
  }
}
