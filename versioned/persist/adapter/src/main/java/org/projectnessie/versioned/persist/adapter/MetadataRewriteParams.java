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
package org.projectnessie.versioned.persist.adapter;

import java.util.Map;
import org.immutables.value.Value;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.MergeType;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.NamedRef;

public interface MetadataRewriteParams extends ToBranchParams {

  /** Ref to merge or transplant from. */
  NamedRef getFromRef();

  /** Whether to keep the individual commits and do not squash the commits to merge. */
  @Value.Default
  default boolean keepIndividualCommits() {
    return false;
  }

  Map<ContentKey, MergeType> getMergeTypes();

  @Value.Default
  default boolean isDryRun() {
    return false;
  }

  @Value.Default
  default MergeType getDefaultMergeType() {
    return MergeType.NORMAL;
  }

  /** Function to rewrite the commit-metadata. */
  MetadataRewriter<ByteString> getUpdateCommitMetadata();

  @SuppressWarnings({"override", "UnusedReturnValue"})
  interface Builder<B> extends ToBranchParams.Builder<B> {
    B fromRef(NamedRef fromBranch);

    B keepIndividualCommits(boolean keepIndividualCommits);

    B defaultMergeType(MergeType defaultMergeType);

    B putMergeTypes(ContentKey key, MergeType value);

    B mergeTypes(Map<? extends ContentKey, ? extends MergeType> entries);

    B isDryRun(boolean dryRun);

    B updateCommitMetadata(MetadataRewriter<ByteString> updateCommitMetadata);
  }
}
