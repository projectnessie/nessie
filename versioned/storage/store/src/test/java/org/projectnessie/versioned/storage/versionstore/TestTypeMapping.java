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

import static java.lang.Integer.signum;
import static java.util.TimeZone.getTimeZone;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.emptyImmutableIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.AUTHOR;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.AUTHOR_TIME;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.COMMITTER;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.COMMIT_TIME;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.CONTENT_DISCRIMINATOR;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.MAIN_UNIVERSE;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.SIGNED_OFF_BY;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.fromCommitMeta;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.headerValueToInstant;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.headersFromCommitMeta;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.headersToCommitMeta;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.instantToHeaderValue;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.storeKeyToKey;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.toCommitMeta;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CreateCommit;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;

@ExtendWith(SoftAssertionsExtension.class)
public class TestTypeMapping {

  public static final String REF_TIMESTAMP_STRING = "2022-11-11T14:52:42.123456789Z";
  public static final long REF_TIMESTAMP_MILLIS = 1668178362000L;

  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void hashToObjId() {
    soft.assertThat(Hash.of("2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d"))
        .extracting(TypeMapping::hashToObjId)
        .isEqualTo(EMPTY_OBJ_ID);

    soft.assertThat(Hash.of("12345678"))
        .extracting(TypeMapping::hashToObjId)
        .isEqualTo(objIdFromString("12345678"));

    soft.assertThat(Hash.of("12345678"))
        .extracting(TypeMapping::hashToObjId)
        .isEqualTo(objIdFromString("12345678"));
  }

  @Test
  public void objIdToHash() {
    soft.assertThat(objIdFromString("12345678"))
        .extracting(TypeMapping::objIdToHash)
        .isEqualTo(Hash.of("12345678"));
  }

  @Test
  public void instantHeader() {
    LocalDate ld = LocalDate.of(2022, 11, 11);
    LocalTime lt = LocalTime.of(14, 52, 42, 123456789);

    ZonedDateTime zdtUtc = ZonedDateTime.of(ld, lt, ZoneId.of("UTC"));
    Instant instantUtc = zdtUtc.toInstant();
    String stringUtc = instantToHeaderValue(instantUtc);
    Instant parsedUtc = headerValueToInstant(stringUtc);
    soft.assertThat(parsedUtc).isEqualTo(instantUtc);
    soft.assertThat(stringUtc).isEqualTo(instantUtc.toString());

    ZonedDateTime zdtCet = ZonedDateTime.of(ld, lt, ZoneId.of("CET"));
    Instant instantCet = zdtCet.toInstant();
    String stringCet = instantToHeaderValue(instantCet);
    Instant parsedCet = headerValueToInstant(stringCet);
    soft.assertThat(parsedCet).isEqualTo(instantCet);
    soft.assertThat(stringCet).isEqualTo(instantCet.toString());

    @SuppressWarnings("ThreeLetterTimeZoneID")
    ZonedDateTime zdtPst = ZonedDateTime.of(ld, lt, getTimeZone("PST").toZoneId());
    Instant instantPst = zdtPst.toInstant();
    String stringPst = instantToHeaderValue(instantPst);
    Instant parsedPst = headerValueToInstant(stringPst);
    soft.assertThat(parsedPst).isEqualTo(instantPst);
    soft.assertThat(stringPst).isEqualTo(instantPst.toString());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "Fri Nov 11 15:52:42 2022 +0100",
        "Fri Nov 11 14:52:42 2022 GMT",
        "Fri Nov 11 14:52:42 2022",
        "Fri Nov 11 14:52:42.123456789 2022 GMT",
        "Fri Nov 11 14:52:42.123456789 2022",
        "Fri Nov 11 14:52:42.123 2022 GMT",
        "Fri Nov 11 14:52:42.123 2022",
        "Fri, 11 Nov 2022 15:52:42 +0100",
        "Fri, 11 Nov 2022 14:52:42 GMT",
        "Fri, 11 Nov 2022 14:52:42",
        "Fri, 11 Nov 2022 14:52:42.123456789 GMT",
        "Fri, 11 Nov 2022 14:52:42.123456789",
        "Fri, 11 Nov 2022 14:52:42.123 GMT",
        "Fri, 11 Nov 2022 14:52:42.123",
        "2022-11-11T14:52:42Z",
        "2022-11-11T14:52:42.123Z",
        "2022-11-11T14:52:42.123456Z",
        "2022-11-11T15:52:42.123456789+01:00",
        "2022-11-11T06:52:42.123456789-08:00",
        // THIS is the "standard" Instant.toString() representation
        REF_TIMESTAMP_STRING
      })
  public void parseInstant(String v) {
    long epochUtc = REF_TIMESTAMP_MILLIS;
    if (v.contains("123")) {
      epochUtc += 123L;
    }

    soft.assertThat(headerValueToInstant(v))
        .asInstanceOf(type(Instant.class))
        .extracting(Instant::toEpochMilli)
        .isEqualTo(epochUtc);

    soft.assertThat(headerValueToInstant(v))
        .asString()
        .startsWith("2022-11-11T14:52:42")
        .endsWith("Z");
  }

  @Test
  public void parseInstantFail() {
    soft.assertThatThrownBy(() -> headerValueToInstant("foo bar"))
        .isInstanceOf(DateTimeParseException.class);
  }

  static Stream<Arguments> keyConversions() {
    return Stream.of(
        arguments(
            ContentKey.of("foo", "bar", "baz"),
            key(MAIN_UNIVERSE, "foo\u0001bar\u0001baz", CONTENT_DISCRIMINATOR)),
        arguments(ContentKey.of("foo"), key(MAIN_UNIVERSE, "foo", CONTENT_DISCRIMINATOR)),
        arguments(ContentKey.of(), key(MAIN_UNIVERSE, CONTENT_DISCRIMINATOR)),
        // unknown variant
        arguments(null, key(MAIN_UNIVERSE, "foo", "XYZ")),
        // unknown universe
        arguments(null, key("ZYX", "foo", CONTENT_DISCRIMINATOR)));
  }

  @ParameterizedTest
  @MethodSource("keyConversions")
  public void keyConversions(ContentKey key, StoreKey storeKey) {
    if (key != null) {
      soft.assertThat(keyToStoreKey(key)).isEqualTo(storeKey);
    }
    soft.assertThat(storeKeyToKey(storeKey)).isEqualTo(key);
  }

  static Stream<Arguments> keyComparisons() {
    return Stream.of(
        arguments(ContentKey.of("a", "b", "c"), ContentKey.of("a", "b", "c")),
        arguments(ContentKey.of("a", "b", "c"), ContentKey.of("a", "b", "d")),
        arguments(ContentKey.of("a", "b", "c"), ContentKey.of("a", "b", "cc")),
        arguments(ContentKey.of("a", "b", "c"), ContentKey.of("a", "aaa")),
        arguments(ContentKey.of("a", "b", "c"), ContentKey.of("a", "a")),
        arguments(ContentKey.of("a", "b", "c"), ContentKey.of()));
  }

  @ParameterizedTest
  @MethodSource("keyComparisons")
  public void keyComparisons(ContentKey k1, ContentKey k2) {
    int cmp = signum(k1.compareTo(k2));
    soft.assertThat(signum(keyToStoreKey(k1).compareTo(keyToStoreKey(k2)))).isEqualTo(cmp);
    soft.assertThat(signum(keyToStoreKey(k2).compareTo(keyToStoreKey(k1)))).isEqualTo(-cmp);
  }

  static Stream<Arguments> headers() {
    Supplier<ImmutableCommitMeta.Builder> cm = () -> ImmutableCommitMeta.builder().message("m");
    Instant timestamp = Instant.ofEpochMilli(REF_TIMESTAMP_MILLIS);
    return Stream.of(
        arguments(cm.get().build(), EMPTY_COMMIT_HEADERS),
        arguments(
            cm.get().author("author").build(), newCommitHeaders().add(AUTHOR, "author").build()),
        arguments(
            cm.get()
                .addAllAuthors("author1", "author2")
                .addAllSignedOffBy("signoff1", "signoff2")
                .putAllProperties("prop1", List.of("p1", "p2"))
                .putAllProperties("prop2", List.of("x1", "x2"))
                .build(),
            newCommitHeaders()
                .add(AUTHOR, "author1")
                .add(AUTHOR, "author2")
                .add(SIGNED_OFF_BY, "signoff1")
                .add(SIGNED_OFF_BY, "signoff2")
                .add("prop1", "p1")
                .add("prop1", "p2")
                .add("prop2", "x1")
                .add("prop2", "x2")
                .build()),
        arguments(
            cm.get().authorTime(timestamp).build(),
            newCommitHeaders().add(AUTHOR_TIME, instantToHeaderValue(timestamp)).build()),
        arguments(
            cm.get().committer("committer").build(),
            newCommitHeaders().add(COMMITTER, "committer").build()),
        arguments(
            cm.get().commitTime(timestamp).build(),
            newCommitHeaders().add(COMMIT_TIME, instantToHeaderValue(timestamp)).build()),
        arguments(
            cm.get().signedOffBy("signed-off").build(),
            newCommitHeaders().add(SIGNED_OFF_BY, "signed-off").build()),
        arguments(
            cm.get().putProperties("my-data", "value").build(),
            newCommitHeaders().add("my-data", "value").build()),
        arguments(
            cm.get().putProperties("my-data", "value").putProperties("my-other", "value").build(),
            newCommitHeaders().add("my-data", "value").add("my-other", "value").build()),
        arguments(
            cm.get()
                .author("author")
                .authorTime(timestamp)
                .committer("committer")
                .commitTime(timestamp)
                .signedOffBy("signed-off")
                .putProperties("my-data", "value")
                .putProperties("my-other", "value")
                .build(),
            newCommitHeaders()
                .add(AUTHOR, "author")
                .add(AUTHOR_TIME, instantToHeaderValue(timestamp))
                .add(COMMITTER, "committer")
                .add(COMMIT_TIME, instantToHeaderValue(timestamp))
                .add(SIGNED_OFF_BY, "signed-off")
                .add("my-data", "value")
                .add("my-other", "value")
                .build()));
  }

  @ParameterizedTest
  @MethodSource("headers")
  public void headers(CommitMeta commitMete, CommitHeaders headers) {
    soft.assertThat(headersFromCommitMeta(commitMete)).isEqualTo(headers);
    soft.assertThat(headersToCommitMeta(headers, CommitMeta.builder()).message("m").build())
        .isEqualTo(commitMete);
  }

  static Stream<Arguments> commitMeta() {
    return headers()
        .map(
            args -> {
              CommitMeta commitMeta = (CommitMeta) args.get()[0];
              CommitHeaders headers = (CommitHeaders) args.get()[1];

              String msg = "message-" + commitMeta.hashCode();
              commitMeta =
                  commitMeta.toBuilder()
                      .hash(EMPTY_OBJ_ID.toString())
                      .addParentCommitHashes(EMPTY_OBJ_ID.toString())
                      .message(msg)
                      .build();

              CreateCommit createCommit =
                  newCommitBuilder()
                      .parentCommitId(EMPTY_OBJ_ID)
                      .message(msg)
                      .headers(headers)
                      .build();

              CommitObj commitObj =
                  CommitObj.commitBuilder()
                      .headers(headers)
                      .message(msg)
                      .id(EMPTY_OBJ_ID)
                      .incrementalIndex(emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize())
                      .created(0L)
                      .seq(1L)
                      .build();

              return arguments(commitMeta, createCommit, commitObj);
            });
  }

  @ParameterizedTest
  @MethodSource("commitMeta")
  public void commitMeta(CommitMeta commitMete, CreateCommit createCommit, CommitObj commitObj) {
    CreateCommit.Builder commitBuilder = newCommitBuilder();
    fromCommitMeta(commitMete, commitBuilder);
    soft.assertThat(commitBuilder.parentCommitId(EMPTY_OBJ_ID).build()).isEqualTo(createCommit);
    soft.assertThat(toCommitMeta(commitObj))
        .isEqualTo(commitMete)
        .extracting(CommitMeta::getHash)
        .isNotNull();
    soft.assertThat(toCommitMeta(commitObj)).isEqualTo(commitMete);
  }
}
