/*
 * Copyright (C) 2023 Dremio
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;

import java.util.HashSet;
import java.util.Set;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.versioned.VersionStore.MergeTransplantOpBase;

final class MergeBehaviors {
  final Set<ContentKey> remainingKeys;
  final Set<ContentKey> keysUsedForCommit;
  final MergeTransplantOpBase mergeTransplantOpBase;

  MergeBehaviors(MergeTransplantOpBase mergeTransplantOpBase) {
    this.mergeTransplantOpBase = mergeTransplantOpBase;
    Set<ContentKey> allKeys = mergeTransplantOpBase.mergeKeyBehaviors().keySet();
    this.remainingKeys = new HashSet<>(allKeys);
    this.keysUsedForCommit = newHashSetWithExpectedSize(allKeys.size());
    validate();
  }

  void postValidate() {
    checkArgument(
        remainingKeys.isEmpty(),
        "Not all merge key behaviors specified in the request have been used. The following keys were not used: %s",
        remainingKeys);
    mergeTransplantOpBase
        .mergeKeyBehaviors()
        .forEach(
            (key, mergeKeyBehavior) ->
                checkArgument(
                    mergeKeyBehavior.getResolvedContent() == null
                        || keysUsedForCommit.contains(key),
                    "The merge behavior for key %s has an unused resolvedContent attribute.",
                    key));
  }

  MergeBehavior mergeBehavior(ContentKey key) {
    MergeKeyBehavior behavior = mergeTransplantOpBase.mergeKeyBehaviors().get(key);
    return behavior == null
        ? mergeTransplantOpBase.defaultMergeBehavior()
        : behavior.getMergeBehavior();
  }

  MergeKeyBehavior useKey(boolean add, ContentKey key) {
    remainingKeys.remove(key);
    MergeKeyBehavior behavior = mergeTransplantOpBase.mergeKeyBehaviors().get(key);
    if (behavior == null) {
      return MergeKeyBehavior.of(key, mergeTransplantOpBase.defaultMergeBehavior());
    }
    if (add) {
      // Add commit-op / Put operation
      keysUsedForCommit.add(key);
    }
    return behavior;
  }

  private void validate() {
    // Require the resolvedContent and expectedTargetContent attributes.
    mergeTransplantOpBase
        .mergeKeyBehaviors()
        .forEach(
            (key, mergeKeyBehavior) -> {
              checkArgument(
                  !mergeTransplantOpBase.keepIndividualCommits()
                      || (mergeKeyBehavior.getExpectedTargetContent() == null
                          && mergeKeyBehavior.getResolvedContent() == null),
                  "MergeKeyBehavior.expectedTargetContent and MergeKeyBehavior.resolvedContent are only supported for squashing merge/transplant operations.");

              switch (mergeKeyBehavior.getMergeBehavior()) {
                case NORMAL:
                  if (mergeKeyBehavior.getResolvedContent() != null) {
                    checkArgument(
                        mergeKeyBehavior.getExpectedTargetContent() != null,
                        "MergeKeyBehavior.resolvedContent requires setting MergeKeyBehavior.expectedTarget as well for key %s",
                        key);
                  }
                  break;
                case DROP:
                case FORCE:
                  checkArgument(
                      mergeKeyBehavior.getResolvedContent() == null,
                      "MergeKeyBehavior.resolvedContent must be null for MergeBehavior.%s for %s",
                      mergeKeyBehavior.getMergeBehavior(),
                      key);
                  break;
                default:
                  throw new IllegalArgumentException(
                      "Unknown MergeBehavior " + mergeKeyBehavior.getMergeBehavior());
              }
            });
  }
}
