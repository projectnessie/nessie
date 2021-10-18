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
package org.projectnessie.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.Contents.Type;

/**
 * This test merely checks the JSON serialization/deserialization of the model classes, with an
 * intention to identify breaking cases whenever jackson version varies.
 */
public class TestModelObjectsSerialization {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String HASH =
      "3e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10e";

  @ParameterizedTest
  @MethodSource("goodCases")
  void testGoodSerDeCases(Case goodCase) throws IOException {
    String json = MAPPER.writeValueAsString(goodCase.obj);
    Assertions.assertThat(json).isEqualTo(goodCase.deserializedJson);
    Object deserialized = MAPPER.readValue(json, goodCase.deserializeAs);
    Assertions.assertThat(deserialized).isEqualTo(goodCase.obj);
  }

  @ParameterizedTest
  @MethodSource("negativeCases")
  void testNegativeSerDeCases(Case invalidCase) {
    Assertions.assertThatThrownBy(
            () -> MAPPER.readValue(invalidCase.deserializedJson, invalidCase.deserializeAs))
        .isInstanceOf(JsonProcessingException.class);
  }

  static List<Case> goodCases() {
    final Instant now = Instant.now();
    final String branchName = "testBranch";

    return Arrays.asList(
        new Case(
            Branch.of(branchName, HASH),
            Branch.class,
            Json.from("type", "BRANCH").add("name", "testBranch").add("hash", HASH)),
        new Case(
            Branch.of(branchName, null),
            Branch.class,
            Json.from("type", "BRANCH").add("name", "testBranch").addNoQuotes("hash", "null")),
        new Case(
            Tag.of("tagname", HASH),
            Tag.class,
            Json.from("type", "TAG").add("name", "tagname").add("hash", HASH)),
        new Case(
            ImmutableTransplant.builder()
                .addHashesToTransplant(HASH)
                .fromRefName(branchName)
                .build(),
            Transplant.class,
            Json.arr("hashesToTransplant", HASH).add("fromRefName", "testBranch")),
        new Case(
            ImmutableEntriesResponse.builder()
                .addEntries(
                    ImmutableEntry.builder()
                        .type(Type.ICEBERG_TABLE)
                        .name(ContentsKey.fromPathString("/tmp/testpath"))
                        .build())
                .token(HASH)
                .isHasMore(true)
                .build(),
            EntriesResponse.class,
            Json.from("token", HASH)
                .addArrNoQuotes(
                    "entries",
                    Json.from("type", "ICEBERG_TABLE")
                        .addNoQuotes("name", Json.arr("elements", "/tmp/testpath")))
                .addNoQuotes("hasMore", true)),
        new Case(
            ImmutableLogResponse.builder()
                .addOperations()
                .token(HASH)
                .addOperations(
                    ImmutableCommitMeta.builder()
                        .commitTime(now)
                        .author("author@example.com")
                        .committer("committer@example.com")
                        .authorTime(now)
                        .hash(HASH)
                        .message("test commit")
                        .putProperties("prop1", "val1")
                        .signedOffBy("signer@example.com")
                        .build())
                .isHasMore(true)
                .build(),
            LogResponse.class,
            Json.from("token", HASH)
                .addArrNoQuotes(
                    "operations",
                    Json.from("hash", HASH)
                        .add("committer", "committer@example.com")
                        .add("author", "author@example.com")
                        .add("signedOffBy", "signer@example.com")
                        .add("message", "test commit")
                        .add("commitTime", now.toString())
                        .add("authorTime", now.toString())
                        .addNoQuotes("properties", Json.from("prop1", "val1")))
                .addNoQuotes("hasMore", true)),
        new Case(
            ImmutableMerge.builder().fromHash(HASH).fromRefName(branchName).build(),
            Merge.class,
            Json.from("fromHash", HASH).add("fromRefName", "testBranch")));
  }

  static List<Case> negativeCases() {
    return Arrays.asList(
        // Special chars in the branch name make it invalid
        new Case(
            Branch.class, Json.from("type", "BRANCH").add("name", "$p@c!@L").add("hash", HASH)),

        // Invalid hash
        new Case(
            Branch.class,
            Json.from("type", "BRANCH").add("name", "testBranch").add("hash", "invalidhash")),

        // No name
        new Case(
            Branch.class,
            Json.from("type", "BRANCH").addNoQuotes("name", "null").add("hash", HASH)),
        new Case(
            Transplant.class,
            Json.arr("hashesToTransplant", "invalidhash").addNoQuotes("fromRefName", "null")),

        // Invalid hash
        new Case(
            Transplant.class,
            Json.arr("hashesToTransplant", "invalidhash").add("fromRefName", "testBranch")),
        new Case(
            Tag.class, Json.from("type", "TAG").add("name", "tagname").add("hash", "invalidhash")));
  }

  static class Case {

    final Object obj;
    final Class<?> deserializeAs;
    final String deserializedJson;

    public Case(Class<?> deserializeAs, Json deserializedJson) {
      this(null, deserializeAs, deserializedJson);
    }

    public Case(Object obj, Class<?> deserializeAs, Json deserializedJson) {
      this.obj = obj;
      this.deserializeAs = deserializeAs;
      this.deserializedJson = deserializedJson.toString();
    }

    @Override
    public String toString() {
      return deserializeAs.getName() + " : " + obj;
    }
  }

  static class Json { // Helps in building json strings, which can be used for verification.

    private static final String STR_KV_FORMAT = "%s,\"%s\":\"%s\"";
    private static final String NO_QUOTES_KV_FORMAT = "%s,\"%s\":%s";

    String currentContent;

    private Json(String currentContent) {
      this.currentContent = currentContent;
    }

    static Json from(String key, String val) {
      return new Json(String.format("\"%s\":\"%s\"", key, val));
    }

    static Json noQuotes(String key, Object val) {
      return new Json(String.format("\"%s\":%s", key, val));
    }

    static Json arr(String key, String... val) {
      String currentContent =
          Stream.of(val)
              .collect(Collectors.joining("\",\"", String.format("\"%s\":[\"", key), "\"]"));
      return new Json(currentContent);
    }

    public Json add(String key, String val) {
      this.currentContent = String.format(STR_KV_FORMAT, currentContent, key, val);
      return this;
    }

    public Json addArrNoQuotes(String key, Object... val) {
      String keyContent =
          Stream.of(val)
              .map(Object::toString)
              .collect(Collectors.joining(",", String.format("\"%s\":[", key), "]"));
      this.currentContent = String.format("%s,%s", currentContent, keyContent);
      return this;
    }

    public Json addNoQuotes(String key, Object val) {
      this.currentContent = String.format(NO_QUOTES_KV_FORMAT, currentContent, key, val.toString());
      return this;
    }

    @Override
    public String toString() {
      return String.format("{%s}", currentContent);
    }
  }
}
