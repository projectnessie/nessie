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
package org.projectnessie.api.v2.params;

import static org.projectnessie.model.JsonUtil.arrayNode;
import static org.projectnessie.model.JsonUtil.objectNode;

import java.util.Arrays;
import java.util.List;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.TestModelObjectsSerialization;

/**
 * This test merely checks the JSON serialization/deserialization of API parameter classes, with an
 * intention to identify breaking cases whenever jackson version varies.
 */
public class TestParamObjectsSerialization extends TestModelObjectsSerialization {

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
                    .message("test-msg")
                    .build())
            .jsonNode(
                o ->
                    o.put("fromRefName", "testBranch")
                        .put("message", "test-msg")
                        .set("hashesToTransplant", arrayNode().add(HASH))),
        new Case(Merge.class)
            .obj(ImmutableMerge.builder().fromHash(HASH).fromRefName(branchName).build())
            .jsonNode(o -> o.put("fromRefName", "testBranch").put("fromHash", HASH)),
        new Case(Merge.class)
            .obj(
                ImmutableMerge.builder()
                    .fromHash(HASH)
                    .fromRefName(branchName)
                    .message("test-msg")
                    .build())
            .jsonNode(
                o ->
                    o.put("fromRefName", "testBranch")
                        .put("message", "test-msg")
                        .put("fromHash", HASH)),
        new Case(Merge.class)
            .obj(
                ImmutableMerge.builder()
                    .fromHash(HASH)
                    .fromRefName(branchName)
                    .defaultKeyMergeMode(MergeBehavior.FORCE)
                    .isFetchAdditionalInfo(true)
                    .addKeyMergeModes(
                        MergeKeyBehavior.of(ContentKey.of("merge", "me"), MergeBehavior.NORMAL),
                        MergeKeyBehavior.of(ContentKey.of("ignore", "this"), MergeBehavior.DROP))
                    .build())
            .jsonNode(
                o ->
                    o.put("fromRefName", "testBranch")
                        .put("defaultKeyMergeMode", "FORCE")
                        .put("fromHash", HASH)
                        .put("isFetchAdditionalInfo", true)
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
                    .message("test-msg")
                    .isDryRun(false)
                    .addKeyMergeModes(
                        MergeKeyBehavior.of(ContentKey.of("merge", "me"), MergeBehavior.NORMAL),
                        MergeKeyBehavior.of(ContentKey.of("ignore", "this"), MergeBehavior.DROP))
                    .build())
            .jsonNode(
                o ->
                    o.put("fromRefName", "testBranch")
                        .put("message", "test-msg")
                        .put("fromHash", HASH)
                        .put("isDryRun", false)
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
        // relative hashes
        new Case(Merge.class)
            .obj(ImmutableMerge.builder().fromRefName(branchName).fromHash("~1").build())
            .jsonNode(o -> o.put("fromRefName", "testBranch").put("fromHash", "~1")),
        new Case(Merge.class)
            .obj(ImmutableMerge.builder().fromRefName(branchName).fromHash("cafebabe~1").build())
            .jsonNode(o -> o.put("fromRefName", "testBranch").put("fromHash", "cafebabe~1")),
        new Case(Transplant.class)
            .obj(
                ImmutableTransplant.builder()
                    .fromRefName(branchName)
                    .addHashesToTransplant("~1")
                    .build())
            .jsonNode(
                o ->
                    o.put("fromRefName", "testBranch")
                        .set("hashesToTransplant", arrayNode().add("~1"))),
        new Case(Transplant.class)
            .obj(
                ImmutableTransplant.builder()
                    .fromRefName(branchName)
                    .addHashesToTransplant("cafebabe~1")
                    .build())
            .jsonNode(
                o ->
                    o.put("fromRefName", "testBranch")
                        .set("hashesToTransplant", arrayNode().add("cafebabe~1"))));
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
