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
package org.projectnessie.client.builder;

import java.util.HashMap;
import java.util.Map;
import org.projectnessie.client.api.OnBranchBuilder;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;

public abstract class BaseMergeTransplantBuilder<B extends OnBranchBuilder<B>>
    extends BaseOnBranchBuilder<B> {

  protected String fromRefName;
  protected Boolean keepIndividualCommits;
  protected Boolean dryRun;
  protected Boolean returnConflictAsResult;
  protected Boolean fetchAdditionalInfo;
  protected MergeBehavior defaultMergeMode;
  protected Map<ContentKey, MergeKeyBehavior> mergeModes;
  protected String message;
  protected CommitMeta commitMeta;

  @SuppressWarnings("unchecked")
  public B message(String message) {
    this.message = message;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B commitMeta(CommitMeta commitMeta) {
    this.commitMeta = commitMeta;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B fromRefName(String fromRefName) {
    this.fromRefName = fromRefName;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B keepIndividualCommits(boolean keepIndividualCommits) {
    this.keepIndividualCommits = keepIndividualCommits;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B dryRun(boolean dryRun) {
    this.dryRun = dryRun;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B fetchAdditionalInfo(boolean fetchAdditionalInfo) {
    this.fetchAdditionalInfo = fetchAdditionalInfo;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B returnConflictAsResult(boolean returnConflictAsResult) {
    this.returnConflictAsResult = returnConflictAsResult;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B defaultMergeMode(MergeBehavior mergeBehavior) {
    defaultMergeMode = mergeBehavior;
    return (B) this;
  }

  @SuppressWarnings("unchecked")
  public B mergeMode(ContentKey key, MergeBehavior mergeBehavior) {
    return mergeKeyBehavior(MergeKeyBehavior.of(key, mergeBehavior));
  }

  @SuppressWarnings("unchecked")
  public B mergeKeyBehavior(MergeKeyBehavior mergeKeyBehavior) {
    if (mergeModes == null) {
      mergeModes = new HashMap<>();
    }

    mergeModes.put(mergeKeyBehavior.getKey(), mergeKeyBehavior);
    return (B) this;
  }
}
