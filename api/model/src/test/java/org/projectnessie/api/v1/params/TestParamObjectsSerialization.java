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
package org.projectnessie.api.v1.params;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.model.JsonUtil.arrayNode;
import static org.projectnessie.model.JsonUtil.objectNode;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.TestModelObjectsSerialization;

/**
 * This test merely checks the JSON serialization/deserialization of API parameter classes, with an
 * intention to identify breaking cases whenever jackson version varies.
 */
public class TestParamObjectsSerialization extends TestModelObjectsSerialization {

  @Test
  void testLegacyFlagNamesDeserialize() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String flags =
        "\"isDryRun\":true,\"isFetchAdditionalInfo\":true," + "\"isReturnConflictAsResult\":true";
    Merge merge =
        mapper.readValue(
            "{\"fromRefName\":\"testBranch\",\"fromHash\":\"" + HASH + "\"," + flags + "}",
            Merge.class);
    Transplant transplant =
        mapper.readValue(
            "{\"fromRefName\":\"testBranch\",\"hashesToTransplant\":[\""
                + HASH
                + "\"],"
                + flags
                + "}",
            Transplant.class);

    assertThat(merge.isDryRun()).isTrue();
    assertThat(merge.isFetchAdditionalInfo()).isTrue();
    assertThat(merge.isReturnConflictAsResult()).isTrue();
    assertThat(transplant.isDryRun()).isTrue();
    assertThat(transplant.isFetchAdditionalInfo()).isTrue();
    assertThat(transplant.isReturnConflictAsResult()).isTrue();
  }

  @SuppressWarnings({
    "unused",
    "deprecation"
  }) // called by JUnit framework based on annotations in superclass
  static List<Case> goodCases() {
    final String branchName = "testBranch";

    return Arrays.asList(
        new Case(Transplant.class)
            .obj(
                ImmutableTransplant.builder()
                    .addHashesToTransplant(HASH)
                    .fromRefName(branchName)
                    .build())
            .jsonNode(
                o ->
                    o.put("fromRefName", "testBranch")
                        .set("hashesToTransplant", arrayNode().add(HASH))),
        new Case(Transplant.class)
            .obj(
                ImmutableTransplant.builder()
                    .addHashesToTransplant(HASH)
                    .fromRefName(branchName)
                    .isDryRun(true)
                    .isFetchAdditionalInfo(true)
                    .isReturnConflictAsResult(true)
                    .build())
            .jsonNode(
                o ->
                    o.put("fromRefName", branchName)
                        .put("dryRun", true)
                        .put("fetchAdditionalInfo", true)
                        .put("returnConflictAsResult", true)
                        .set("hashesToTransplant", arrayNode().add(HASH))),
        new Case(Transplant.class)
            .obj(
                ImmutableTransplant.builder()
                    .addHashesToTransplant(HASH)
                    .fromRefName(branchName)
                    .keepIndividualCommits(true)
                    .build())
            .jsonNode(
                o ->
                    o.put("fromRefName", "testBranch")
                        .put("keepIndividualCommits", true)
                        .set("hashesToTransplant", arrayNode().add(HASH))),
        new Case(Merge.class)
            .obj(ImmutableMerge.builder().fromHash(HASH).fromRefName(branchName).build())
            .jsonNode(o -> o.put("fromRefName", "testBranch").put("fromHash", HASH)),
        new Case(Merge.class)
            .obj(
                ImmutableMerge.builder()
                    .fromHash(HASH)
                    .fromRefName(branchName)
                    .keepIndividualCommits(true)
                    .build())
            .jsonNode(
                o ->
                    o.put("fromRefName", "testBranch")
                        .put("keepIndividualCommits", true)
                        .put("fromHash", HASH)),
        new Case(Merge.class)
            .obj(
                ImmutableMerge.builder()
                    .fromHash(HASH)
                    .fromRefName(branchName)
                    .defaultKeyMergeMode(MergeBehavior.FORCE)
                    .isFetchAdditionalInfo(true)
                    .isReturnConflictAsResult(true)
                    .addKeyMergeModes(
                        MergeKeyBehavior.of(ContentKey.of("merge", "me"), MergeBehavior.NORMAL),
                        MergeKeyBehavior.of(ContentKey.of("ignore", "this"), MergeBehavior.DROP))
                    .build())
            .jsonNode(
                o ->
                    o.put("fromRefName", "testBranch")
                        .put("defaultKeyMergeMode", "FORCE")
                        .put("fromHash", HASH)
                        .put("fetchAdditionalInfo", true)
                        .put("returnConflictAsResult", true)
                        .set(
                            "keyMergeModes",
                            arrayNode()
                                .add(
                                    objectNode()
                                        .put("mergeBehavior", "NORMAL")
                                        .set(
                                            "key",
                                            objectNode()
                                                .set(
                                                    "elements",
                                                    arrayNode().add("merge").add("me"))))
                                .add(
                                    objectNode()
                                        .put("mergeBehavior", "DROP")
                                        .set(
                                            "key",
                                            objectNode()
                                                .set(
                                                    "elements",
                                                    arrayNode().add("ignore").add("this")))))),
        new Case(Merge.class)
            .obj(
                ImmutableMerge.builder()
                    .fromHash(HASH)
                    .fromRefName(branchName)
                    .keepIndividualCommits(true)
                    .isDryRun(true)
                    .addKeyMergeModes(
                        MergeKeyBehavior.of(ContentKey.of("merge", "me"), MergeBehavior.NORMAL),
                        MergeKeyBehavior.of(ContentKey.of("ignore", "this"), MergeBehavior.DROP))
                    .build())
            .jsonNode(
                o ->
                    o.put("fromRefName", "testBranch")
                        .put("keepIndividualCommits", true)
                        .put("fromHash", HASH)
                        .put("dryRun", true)
                        .set(
                            "keyMergeModes",
                            arrayNode()
                                .add(
                                    objectNode()
                                        .put("mergeBehavior", "NORMAL")
                                        .set(
                                            "key",
                                            objectNode()
                                                .set(
                                                    "elements",
                                                    arrayNode().add("merge").add("me"))))
                                .add(
                                    objectNode()
                                        .put("mergeBehavior", "DROP")
                                        .set(
                                            "key",
                                            objectNode()
                                                .set(
                                                    "elements",
                                                    arrayNode().add("ignore").add("this")))))));
  }

  @SuppressWarnings("unused") // called by JUnit framework based on annotations in superclass
  static List<Case> negativeCases() {
    return Arrays.asList(
        new Case(Transplant.class)
            .jsonNode(
                o ->
                    o.putNull("fromRefName")
                        .set("hashesToTransplant", arrayNode().add("invalidhash"))),

        // Invalid hash
        new Case(Transplant.class)
            .jsonNode(
                o ->
                    o.put("fromRefName", "testBranch")
                        .set("hashesToTransplant", arrayNode().add("invalidhash"))));
  }
}
