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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.TestModelObjectsSerialization;

/**
 * This test merely checks the JSON serialization/deserialization of API parameter classes, with an
 * intention to identify breaking cases whenever jackson version varies.
 */
@Execution(ExecutionMode.CONCURRENT)
public class TestParamObjectsSerialization extends TestModelObjectsSerialization {

  @SuppressWarnings("unused") // called by JUnit framework based on annotations in superclass
  static List<Case> goodCases() {
    final String branchName = "testBranch";

    return Arrays.asList(
        new Case(
            ImmutableTransplant.builder()
                .addHashesToTransplant(HASH)
                .fromRefName(branchName)
                .build(),
            Transplant.class,
            Json.from("fromRefName", "testBranch").addArr("hashesToTransplant", HASH)),
        new Case(
            ImmutableTransplant.builder()
                .message("test-msg")
                .addHashesToTransplant(HASH)
                .fromRefName(branchName)
                .build(),
            Transplant.class,
            Json.from("fromRefName", "testBranch")
                .add("message", "test-msg")
                .addArr("hashesToTransplant", HASH)),
        new Case(
            ImmutableMerge.builder().fromHash(HASH).fromRefName(branchName).build(),
            Merge.class,
            Json.from("fromRefName", "testBranch").add("fromHash", HASH)),
        new Case(
            ImmutableMerge.builder()
                .message("test-msg")
                .fromHash(HASH)
                .fromRefName(branchName)
                .build(),
            Merge.class,
            Json.from("fromRefName", "testBranch")
                .add("message", "test-msg")
                .add("fromHash", HASH)),
        new Case(
            ImmutableMerge.builder()
                .fromHash(HASH)
                .fromRefName(branchName)
                .defaultKeyMergeMode(MergeBehavior.FORCE)
                .isFetchAdditionalInfo(true)
                .addKeyMergeModes(
                    MergeKeyBehavior.of(ContentKey.of("merge", "me"), MergeBehavior.NORMAL),
                    MergeKeyBehavior.of(ContentKey.of("ignore", "this"), MergeBehavior.DROP))
                .build(),
            Merge.class,
            Json.from("fromRefName", "testBranch")
                .addArrNoQuotes(
                    "keyMergeModes",
                    Json.noQuotes("key", Json.arr("elements", "merge", "me"))
                        .add("mergeBehavior", "NORMAL"),
                    Json.noQuotes("key", Json.arr("elements", "ignore", "this"))
                        .add("mergeBehavior", "DROP"))
                .add("defaultKeyMergeMode", "FORCE")
                .add("fromHash", HASH)
                .addNoQuotes("isFetchAdditionalInfo", "true")),
        new Case(
            ImmutableMerge.builder()
                .fromHash(HASH)
                .fromRefName(branchName)
                .isDryRun(false)
                .addKeyMergeModes(
                    MergeKeyBehavior.of(ContentKey.of("merge", "me"), MergeBehavior.NORMAL),
                    MergeKeyBehavior.of(ContentKey.of("ignore", "this"), MergeBehavior.DROP))
                .build(),
            Merge.class,
            Json.from("fromRefName", "testBranch")
                .addArrNoQuotes(
                    "keyMergeModes",
                    Json.noQuotes("key", Json.arr("elements", "merge", "me"))
                        .add("mergeBehavior", "NORMAL"),
                    Json.noQuotes("key", Json.arr("elements", "ignore", "this"))
                        .add("mergeBehavior", "DROP"))
                .add("fromHash", HASH)
                .addNoQuotes("isDryRun", "false")));
  }

  @SuppressWarnings("unused") // called by JUnit framework based on annotations in superclass
  static List<Case> negativeCases() {
    return Arrays.asList(
        new Case(
            Transplant.class,
            Json.arr("hashesToTransplant", "invalidhash").addNoQuotes("fromRefName", "null")),

        // Invalid hash
        new Case(
            Transplant.class,
            Json.arr("hashesToTransplant", "invalidhash").add("fromRefName", "testBranch")));
  }
}
