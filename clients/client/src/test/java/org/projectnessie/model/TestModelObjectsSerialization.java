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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
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
    assertThrows(
        JsonProcessingException.class,
        () -> MAPPER.readValue(invalidCase.deserializedJson, invalidCase.deserializeAs));
  }

  static List<Case> goodCases() {
    final Instant now = Instant.now();
    final String branchName = "testBranch";

    return Arrays.asList(
        new Case(
            ImmutableBranch.builder().hash(HASH).name(branchName).build(),
            Branch.class,
            "{\"type\":\"BRANCH\",\"name\":\"testBranch\"," + "\"hash\":\"" + HASH + "\"}"),
        new Case(
            ImmutableBranch.builder().name(branchName).build(),
            Branch.class,
            "{\"type\":\"BRANCH\",\"name\":\"testBranch\",\"hash\":null}"),
        new Case(
            ImmutableTag.builder().hash(HASH).name("tagname").build(),
            Tag.class,
            "{\"type\":\"TAG\",\"name\":\"tagname\",\"hash\":\"" + HASH + "\"}"),
        new Case(
            ImmutableHash.builder().name(HASH).build(),
            Hash.class,
            "{\"type\":\"HASH\",\"name\":\"" + HASH + "\",\"hash\":\"" + HASH + "\"}"),
        new Case(
            ImmutableTransplant.builder()
                .addHashesToTransplant(HASH)
                .fromRefName(branchName)
                .build(),
            Transplant.class,
            "{\"hashesToTransplant\":" + "[\"" + HASH + "\"]," + "\"fromRefName\":\"testBranch\"}"),
        new Case(
            ImmutableEntriesResponse.builder()
                .addEntries(
                    ImmutableEntry.builder()
                        .type(Type.ICEBERG_TABLE)
                        .name(ContentsKey.fromPathString("/tmp/testpath"))
                        .build())
                .token(HASH)
                .hasMore(true)
                .build(),
            EntriesResponse.class,
            "{\"hasMore\":true,"
                + "\"token\":\""
                + HASH
                + "\","
                + "\"entries\":[{\"type\":\"ICEBERG_TABLE\","
                + "\"name\":{\"elements\":[\"/tmp/testpath\"]}}]}"),
        new Case(
            ImmutableLogResponse.builder()
                .addOperations()
                .token(HASH)
                .addOperations(
                    ImmutableCommitMeta.builder()
                        .commitTime(now)
                        .author("author@testnessie.com")
                        .committer("committer@testnessie.com")
                        .authorTime(now)
                        .hash(HASH)
                        .message("test commit")
                        .putProperties("prop1", "val1")
                        .signedOffBy("signer@testnessie.com")
                        .build())
                .hasMore(true)
                .build(),
            LogResponse.class,
            "{\"hasMore\":true,"
                + "\"token\":\""
                + HASH
                + "\","
                + "\"operations\":["
                + "{\"hash\":\""
                + HASH
                + "\","
                + "\"committer\":\"committer@testnessie.com\",\"author\":\"author@testnessie.com\","
                + "\"signedOffBy\":\"signer@testnessie.com\",\"message\":\"test commit\","
                + "\"commitTime\":\""
                + now
                + "\","
                + "\"authorTime\":\""
                + now
                + "\","
                + "\"properties\":{\"prop1\":\"val1\"}}]}"),
        new Case(
            ImmutableMerge.builder().fromHash(HASH).fromRefName(branchName).build(),
            Merge.class,
            "{\"fromHash\":\"" + HASH + "\"," + "\"fromRefName\":\"testBranch\"}"));
  }

  static List<Case> negativeCases() {
    return Arrays.asList(
        // Special chars in the branch name make it invalid
        new Case(
            Branch.class,
            "{\"type\":\"BRANCH\",\"name\":\"$p@c!@L\"," + "\"hash\":\"" + HASH + "\"}"),

        // Invalid hash
        new Case(
            Branch.class,
            "{\"type\":\"BRANCH\",\"name\":\"testBranch\"," + "\"hash\":\"invalidhash\"}"),

        // No name
        new Case(
            Branch.class, "{\"type\":\"BRANCH\",\"name\":null," + "\"hash\":\"" + HASH + "\"}"),
        new Case(
            Transplant.class,
            "{\"hashesToTransplant\":" + "[\"invalidhash\"]," + "\"fromRefName\":null}"),

        // Invalid hash
        new Case(
            Transplant.class,
            "{\"hashesToTransplant\":" + "[\"invalidhash\"]," + "\"fromRefName\":\"testBranch\"}"),
        new Case(Tag.class, "{\"type\":\"TAG\",\"name\":\"tagname\",\"hash\":\"invalidhash\"}"),
        new Case(
            Hash.class, "{\"type\":\"HASH\",\"name\":\"invalidhash\",\"hash\":\"invalidhash\"}"));
  }

  static class Case {

    final Object obj;
    final Class<?> deserializeAs;
    final String deserializedJson;

    public Case(Class<?> deserializeAs, String deserializedJson) {
      this(null, deserializeAs, deserializedJson);
    }

    public Case(Object obj, Class<?> deserializeAs, String deserializedJson) {
      this.obj = obj;
      this.deserializeAs = deserializeAs;
      this.deserializedJson = deserializedJson;
    }

    @Override
    public String toString() {
      return deserializeAs.getName() + " : " + obj;
    }
  }

  @Test
  public void testN() throws IOException {
    Tag t = ImmutableTag.builder().hash(HASH).name("tagname").build();
    System.out.println(MAPPER.writeValueAsString(t));
    Hash h = ImmutableHash.builder().name(HASH).build();
    System.out.println(MAPPER.writeValueAsString(h));
  }
}
