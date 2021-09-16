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
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
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

  @ParameterizedTest
  @MethodSource("goodCases")
  void testGoodSerDeCases(Case goodCase) throws JsonProcessingException {
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
    final String hash = "3e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10e";
    final String branchName = "testBranch";

    return Arrays.asList(
        new Case(
            ImmutableBranch.builder().hash(hash).name(branchName).build(),
            Branch.class,
            "{\"type\":\"BRANCH\",\"name\":\"testBranch\","
                + "\"hash\":\"3e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10e\"}"),
        new Case(
            ImmutableBranch.builder().name(branchName).build(),
            Branch.class,
            "{\"type\":\"BRANCH\",\"name\":\"testBranch\",\"hash\":null}"),
        new Case(
            ImmutableTransplant.builder()
                .addHashesToTransplant(hash)
                .fromRefName(branchName)
                .build(),
            Transplant.class,
            "{\"hashesToTransplant\":"
                + "[\"3e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10e\"],"
                + "\"fromRefName\":\"testBranch\"}"),
        new Case(
            ImmutableEntriesResponse.builder()
                .addEntries(
                    ImmutableEntry.builder()
                        .type(Type.ICEBERG_TABLE)
                        .name(ContentsKey.fromPathString("/tmp/testpath"))
                        .build())
                .token(hash)
                .hasMore(true)
                .build(),
            EntriesResponse.class,
            "{\"hasMore\":true,"
                + "\"token\":\"3e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10e\","
                + "\"entries\":[{\"type\":\"ICEBERG_TABLE\","
                + "\"name\":{\"elements\":[\"/tmp/testpath\"]}}]}"),
        new Case(
            ImmutableLogResponse.builder()
                .addOperations()
                .token(hash)
                .addOperations(
                    ImmutableCommitMeta.builder()
                        .commitTime(now)
                        .author("author@testnessie.com")
                        .committer("committer@testnessie.com")
                        .authorTime(now)
                        .hash(hash)
                        .message("test commit")
                        .putProperties("prop1", "val1")
                        .signedOffBy("signer@testnessie.com")
                        .build())
                .hasMore(true)
                .build(),
            LogResponse.class,
            "{\"hasMore\":true,"
                + "\"token\":\"3e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10e\","
                + "\"operations\":["
                + "{\"hash\":\"3e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10e\","
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
            ImmutableMerge.builder().fromHash(hash).fromRefName(branchName).build(),
            Merge.class,
            "{\"fromHash\":\"3e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10e\","
                + "\"fromRefName\":\"testBranch\"}"));
  }

  static List<Case> negativeCases() {
    final Instant now = Instant.now();
    final String hash = "3e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10e";

    return Arrays.asList(
        // Special chars in the branch name make it invalid
        new Case(
            Branch.class,
            "{\"type\":\"BRANCH\",\"name\":\"$p@c!@L\"," + "\"hash\":\"" + hash + "\"}"),

        // Invalid hash
        new Case(
            Branch.class,
            "{\"type\":\"BRANCH\",\"name\":\"testBranch\"," + "\"hash\":\"invalidhash\"}"),

        // No name
        new Case(
            Branch.class, "{\"type\":\"BRANCH\",\"name\":null," + "\"hash\":\"" + hash + "\"}"),

        // Invalid hash
        new Case(
            Transplant.class,
            "{\"hashesToTransplant\":" + "[\"invalidhash\"]," + "\"fromRefName\":\"testBranch\"}"));
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
}
