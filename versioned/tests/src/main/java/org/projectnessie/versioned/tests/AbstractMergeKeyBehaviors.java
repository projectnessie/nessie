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
package org.projectnessie.versioned.tests;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.MergeBehavior.DROP;
import static org.projectnessie.model.MergeBehavior.FORCE;
import static org.projectnessie.model.MergeBehavior.NORMAL;
import static org.projectnessie.versioned.tests.AbstractVersionStoreTestBase.METADATA_REWRITER;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.VersionStore;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractMergeKeyBehaviors extends AbstractNestedVersionStore {

  @InjectSoftAssertions protected SoftAssertions soft;

  static ContentKey keyInitial = ContentKey.of("initial");
  static ContentKey keyTarget1 = ContentKey.of("target1");
  static ContentKey keySource1 = ContentKey.of("source1");
  static ContentKey keyCommon1 = ContentKey.of("common1");
  static ContentKey keyCommon2 = ContentKey.of("common2");
  static IcebergTable initialTable = IcebergTable.of("initial", 1, 2, 3, 4);
  static IcebergTable targetTable1 = IcebergTable.of("target1", 1, 2, 3, 4);
  static IcebergTable sourceTable1 = IcebergTable.of("source1", 1, 2, 3, 4);
  static IcebergTable commonTable1onTarget = IcebergTable.of("common1onTarget", 1, 2, 3, 4);
  static IcebergTable commonTable2onTarget = IcebergTable.of("common2onTarget", 1, 2, 3, 4);
  static IcebergTable commonTable1onSource = IcebergTable.of("common1onSource", 1, 2, 3, 4);
  static IcebergTable commonTable2onSource = IcebergTable.of("common2onSource", 1, 2, 3, 4);

  protected AbstractMergeKeyBehaviors(VersionStore store) {
    super(store);
  }

  static Stream<Arguments> mergeKeyBehavior() {
    return Stream.of(
        // 1
        arguments(
            ImmutableMap.of(
                keyCommon1, MergeKeyBehavior.of(keyCommon1, DROP),
                keyCommon2, MergeKeyBehavior.of(keyCommon2, DROP)),
            NORMAL,
            ImmutableMap.of(
                keyInitial, initialTable,
                keyTarget1, targetTable1,
                keySource1, sourceTable1,
                keyCommon1, commonTable1onTarget,
                keyCommon2, commonTable2onTarget)),
        // 2
        arguments(
            ImmutableMap.of(
                keyCommon1, MergeKeyBehavior.of(keyCommon1, FORCE),
                keyCommon2, MergeKeyBehavior.of(keyCommon2, FORCE)),
            NORMAL,
            ImmutableMap.of(
                keyInitial, initialTable,
                keyTarget1, targetTable1,
                keySource1, sourceTable1,
                keyCommon1, commonTable1onSource,
                keyCommon2, commonTable2onSource)),
        // 3
        arguments(
            ImmutableMap.of(
                keyCommon1, MergeKeyBehavior.of(keyCommon1, DROP),
                keyCommon2, MergeKeyBehavior.of(keyCommon2, FORCE)),
            NORMAL,
            ImmutableMap.of(
                keyInitial, initialTable,
                keyTarget1, targetTable1,
                keySource1, sourceTable1,
                keyCommon1, commonTable1onTarget,
                keyCommon2, commonTable2onSource)),
        // 4
        arguments(
            ImmutableMap.of(
                keyCommon1, MergeKeyBehavior.of(keyCommon1, DROP),
                keyCommon2, MergeKeyBehavior.of(keyCommon2, DROP)),
            NORMAL,
            ImmutableMap.of(
                keyInitial, initialTable,
                keyTarget1, targetTable1,
                keySource1, sourceTable1,
                keyCommon1, commonTable1onTarget,
                keyCommon2, commonTable2onTarget)),
        // 5
        arguments(
            ImmutableMap.of(
                keyCommon1, MergeKeyBehavior.of(keyCommon1, FORCE),
                keyCommon2, MergeKeyBehavior.of(keyCommon2, FORCE)),
            NORMAL,
            ImmutableMap.of(
                keyInitial, initialTable,
                keyTarget1, targetTable1,
                keySource1, sourceTable1,
                keyCommon1, commonTable1onSource,
                keyCommon2, commonTable2onSource)),
        // 6
        arguments(
            ImmutableMap.of(
                keyCommon1, MergeKeyBehavior.of(keyCommon1, DROP),
                keyCommon2, MergeKeyBehavior.of(keyCommon2, FORCE),
                keySource1, MergeKeyBehavior.of(keySource1, DROP)),
            NORMAL,
            ImmutableMap.of(
                keyInitial, initialTable,
                keyTarget1, targetTable1,
                keyCommon1, commonTable1onTarget,
                keyCommon2, commonTable2onSource)),
        // 7
        arguments(
            ImmutableMap.of(
                keyCommon1, MergeKeyBehavior.of(keyCommon1, DROP),
                keyCommon2, MergeKeyBehavior.of(keyCommon2, FORCE)),
            DROP,
            ImmutableMap.of(
                keyInitial, initialTable,
                keyTarget1, targetTable1,
                keyCommon1, commonTable1onTarget,
                keyCommon2, commonTable2onSource)),
        // 8
        arguments(
            ImmutableMap.of(),
            FORCE,
            ImmutableMap.of(
                keyInitial, initialTable,
                keyTarget1, targetTable1,
                keySource1, sourceTable1,
                keyCommon1, commonTable1onSource,
                keyCommon2, commonTable2onSource)),
        // 9
        arguments(
            ImmutableMap.of(keySource1, MergeKeyBehavior.of(keySource1, DROP)),
            FORCE,
            ImmutableMap.of(
                keyInitial,
                initialTable,
                keyTarget1,
                targetTable1,
                keyCommon1,
                commonTable1onSource,
                keyCommon2,
                commonTable2onSource)));
  }

  @ParameterizedTest
  @MethodSource("mergeKeyBehavior")
  void mergeKeyBehavior(
      Map<ContentKey, MergeKeyBehavior> mergeKeyBehaviors,
      MergeBehavior defaultMergeBehavior,
      Map<ContentKey, Content> expectedContentsAfter)
      throws Exception {
    BranchName targetName = BranchName.of("target");
    Hash target = store().create(targetName, Optional.empty()).getHash();

    List<ContentKey> allKeys = asList(keyInitial, keyTarget1, keySource1, keyCommon1, keyCommon2);

    target =
        store()
            .commit(
                targetName,
                Optional.of(target),
                fromMessage("initial"),
                singletonList(Put.of(keyInitial, initialTable)))
            .getCommitHash();

    BranchName sourceName = BranchName.of("source");
    Hash source = store().create(sourceName, Optional.of(target)).getHash();

    target =
        store()
            .commit(
                targetName,
                Optional.of(target),
                fromMessage("target 2"),
                asList(
                    Put.of(keyTarget1, targetTable1),
                    Put.of(keyCommon1, commonTable1onTarget),
                    Put.of(keyCommon2, commonTable2onTarget)))
            .getCommitHash();
    source =
        store()
            .commit(
                sourceName,
                Optional.of(source),
                fromMessage("source 2"),
                asList(
                    Put.of(keySource1, sourceTable1),
                    Put.of(keyCommon1, commonTable1onSource),
                    Put.of(keyCommon2, commonTable2onSource)))
            .getCommitHash();

    MergeResult<Commit> mergeResult =
        store()
            .merge(
                sourceName,
                source,
                targetName,
                Optional.of(target),
                METADATA_REWRITER,
                false,
                mergeKeyBehaviors,
                defaultMergeBehavior,
                false,
                false);
    soft.assertThat(mergeResult)
        .extracting(MergeResult::wasApplied, MergeResult::wasSuccessful)
        .containsExactly(true, true);
    soft.assertThat(mergeResult.getResultantTargetHash()).isNotEqualTo(target);
    target = mergeResult.getResultantTargetHash();
    Map<ContentKey, Content> contentsOnTargetAfter =
        store().getValues(target, allKeys).entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, e -> ((ImmutableIcebergTable) e.getValue()).withId(null)));
    soft.assertThat(contentsOnTargetAfter)
        .containsExactlyInAnyOrderEntriesOf(expectedContentsAfter);
  }
}
