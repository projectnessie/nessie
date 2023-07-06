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
package org.projectnessie.client.api.impl;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.immutables.value.Value;
import org.projectnessie.api.v2.params.Merge;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.MergeResponse;

@Value.Immutable
public abstract class MergeResponseInspectorImpl extends BaseMergeResponseInspector {
  public static ImmutableMergeResponseInspectorImpl.Builder builder() {
    return ImmutableMergeResponseInspectorImpl.builder();
  }

  protected abstract NessieApiV2 api();

  @Override
  public abstract Merge getRequest();

  @Override
  public abstract MergeResponse getResponse();

  @Value.Lazy
  protected List<GetMultipleContentsResponse.ContentWithKey> mergeBaseContents()
      throws NessieNotFoundException {
    Map<ContentKey, Conflict> conflictMap = conflictMap();
    if (conflictMap.isEmpty()) {
      return emptyList();
    }

    @SuppressWarnings("resource")
    NessieApiV2 api = api();

    // Collect the contents on the merge base for the conflicting keys.
    return api.getContent()
        .keys(new ArrayList<>(conflictMap.keySet()))
        .refName(Detached.REF_NAME)
        .hashOnRef(getResponse().getCommonAncestor())
        .getWithResponse()
        .getContents();
  }

  @Value.Lazy
  protected Map<ContentKey, Conflict> conflictMap() {
    return super.conflictMap();
  }

  @Value.Lazy
  protected Set<String> mergeBaseContentIds() throws NessieNotFoundException {
    return super.mergeBaseContentIds();
  }

  @Value.Lazy
  protected Map<ContentKey, Content> mergeBaseContentByKey() throws NessieNotFoundException {
    return super.mergeBaseContentByKey();
  }

  @Value.Lazy
  protected Map<String, List<DiffResponse.DiffEntry>> sourceDiff() throws NessieNotFoundException {
    return super.sourceDiff();
  }

  @Value.Lazy
  protected Map<String, List<DiffResponse.DiffEntry>> targetDiff() throws NessieNotFoundException {
    return super.targetDiff();
  }

  @Override
  protected Map<String, List<DiffResponse.DiffEntry>> diffByContentId(String ref, String hash)
      throws NessieNotFoundException {
    @SuppressWarnings("resource")
    NessieApiV2 api = api();

    Set<String> mergeBaseContentIds = mergeBaseContentIds();

    // Use the diff from merge-base to merge-target and collect the diff-entries by content ID.
    // This is needed to look up the contents irrespective of a rename between merge-base and
    // merge-target.
    return api
        .getDiff()
        .fromRefName(Detached.REF_NAME)
        .fromHashOnRef(getResponse().getCommonAncestor())
        .toRefName(ref)
        .toHashOnRef(hash)
        .stream()
        .filter(diff -> mergeBaseContentIds.contains(contentIdFromDiffEntry(diff)))
        .collect(groupingBy(MergeResponseInspectorImpl::contentIdFromDiffEntry));
  }
}
