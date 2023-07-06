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
import static java.util.Objects.requireNonNull;
import static org.projectnessie.client.api.impl.MapEntry.mapEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.client.api.ImmutableMergeConflictDetails;
import org.projectnessie.client.api.MergeResponseInspector;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.model.MergeResponse;

/** Base class used for testing and production code, because testing does not use the Nessie API. */
public abstract class BaseMergeResponseInspector implements MergeResponseInspector {

  @Override
  public Stream<MergeConflictDetails> collectMergeConflictDetails() throws NessieNotFoundException {
    // Note: The API exposes a `Stream` so we can optimize this implementation later to reduce
    // runtime or heap pressure.
    return mergeConflictDetails().stream();
  }

  protected List<MergeConflictDetails> mergeConflictDetails() throws NessieNotFoundException {
    Map<ContentKey, Conflict> conflictMap = conflictMap();
    Map<ContentKey, Content> mergeBaseContentByKey = mergeBaseContentByKey();
    if (mergeBaseContentByKey.isEmpty()) {
      return emptyList();
    }

    Map<String, List<DiffResponse.DiffEntry>> sourceDiff = sourceDiff();
    Map<String, List<DiffResponse.DiffEntry>> targetDiff = targetDiff();

    List<MergeConflictDetails> details = new ArrayList<>(mergeBaseContentByKey.size());

    for (Map.Entry<ContentKey, Content> base : mergeBaseContentByKey.entrySet()) {
      ContentKey baseKey = base.getKey();
      Content baseContent = base.getValue();

      Map.Entry<ContentKey, Content> source = keyAndContent(baseKey, baseContent, sourceDiff);
      Map.Entry<ContentKey, Content> target = keyAndContent(baseKey, baseContent, targetDiff);

      MergeConflictDetails detail =
          ImmutableMergeConflictDetails.builder()
              .conflict(conflictMap.get(baseKey))
              .keyOnMergeBase(baseKey)
              .keyOnSource(source.getKey())
              .keyOnTarget(target.getKey())
              .contentOnMergeBase(baseContent)
              .contentOnSource(source.getValue())
              .contentOnTarget(target.getValue())
              .build();

      details.add(detail);
    }
    return details;
  }

  protected Map<ContentKey, Conflict> conflictMap() {
    return getResponse().getDetails().stream()
        .map(MergeResponse.ContentKeyDetails::getConflict)
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(Conflict::key, Function.identity()));
  }

  protected Set<String> mergeBaseContentIds() throws NessieNotFoundException {
    return mergeBaseContents().stream()
        .map(GetMultipleContentsResponse.ContentWithKey::getContent)
        .map(Content::getId)
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }

  protected Map<ContentKey, Content> mergeBaseContentByKey() throws NessieNotFoundException {
    return mergeBaseContents().stream()
        .collect(
            Collectors.toMap(
                GetMultipleContentsResponse.ContentWithKey::getKey,
                GetMultipleContentsResponse.ContentWithKey::getContent));
  }

  protected Map<String, List<DiffResponse.DiffEntry>> sourceDiff() throws NessieNotFoundException {
    // TODO it would help to have the effective from-hash in the response, in case the merge-request
    //  did not contain it. If we have that merge, we can use 'DETACHED' here.
    String hash = getRequest().getFromHash();
    String ref = hash != null ? Detached.REF_NAME : getRequest().getFromRefName();
    return diffByContentId(ref, hash);
  }

  protected Map<String, List<DiffResponse.DiffEntry>> targetDiff() throws NessieNotFoundException {
    return diffByContentId(Detached.REF_NAME, getResponse().getEffectiveTargetHash());
  }

  protected abstract List<GetMultipleContentsResponse.ContentWithKey> mergeBaseContents()
      throws NessieNotFoundException;

  protected abstract Map<String, List<DiffResponse.DiffEntry>> diffByContentId(
      String ref, String hash) throws NessieNotFoundException;

  static String contentIdFromDiffEntry(DiffResponse.DiffEntry e) {
    Content from = e.getFrom();
    return requireNonNull(from != null ? from.getId() : requireNonNull(e.getTo()).getId());
  }

  static Map.Entry<ContentKey, Content> keyAndContent(
      ContentKey baseKey, Content baseContent, Map<String, List<DiffResponse.DiffEntry>> diff) {
    List<DiffResponse.DiffEntry> diffs = diff.get(baseContent.getId());
    if (diffs != null) {
      int size = diffs.size();
      DiffResponse.DiffEntry last = diffs.get(size - 1);
      return mapEntry(last.getKey(), last.getTo());
    }
    return mapEntry(baseKey, baseContent);
  }
}
