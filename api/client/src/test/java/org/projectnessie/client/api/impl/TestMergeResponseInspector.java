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

import static org.projectnessie.model.Conflict.conflict;
import static org.projectnessie.model.DiffResponse.DiffEntry.diffEntry;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.api.v2.params.ImmutableMerge;
import org.projectnessie.api.v2.params.Merge;
import org.projectnessie.client.api.MergeResponseInspector;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.GetMultipleContentsResponse.ContentWithKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableContentKeyDetails;
import org.projectnessie.model.ImmutableMergeResponse;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeResponse;

@ExtendWith(SoftAssertionsExtension.class)
public class TestMergeResponseInspector {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void foo() throws Exception {
    String mainHash = "00000000";
    String sourceHash = "deadbeef";
    String mergeBase = "12345678";

    String cidOne = "1";
    String cidTwo = "2";

    ContentKey keyOne = ContentKey.of("one");
    ContentKey keyTwo = ContentKey.of("two");
    ContentKey keyOneSource = ContentKey.of("one-s");
    ContentKey keyOneTarget = ContentKey.of("one-t");

    IcebergTable oneBase = IcebergTable.of("one", 1, 1, 1, 1, cidOne);
    IcebergTable twoBase = IcebergTable.of("two", 2, 2, 2, 2, cidTwo);
    IcebergTable oneSource = IcebergTable.of("one-source", 1, 1, 1, 1, cidOne);
    IcebergTable twoSource = IcebergTable.of("two-source", 2, 2, 2, 2, cidTwo);
    IcebergTable oneTarget = IcebergTable.of("one-target", 1, 1, 1, 1, cidOne);
    IcebergTable twoTarget = IcebergTable.of("two-target", 2, 2, 2, 2, cidTwo);
    Map<String, List<DiffEntry>> diffsSource = new HashMap<>();
    diffsSource.put(
        oneSource.getId(),
        diffForRename(keyOne, oneBase, keyOneSource, oneSource).collect(Collectors.toList()));
    diffsSource.put(
        twoSource.getId(), diffForUpdate(keyTwo, twoBase, twoTarget).collect(Collectors.toList()));
    Map<String, List<DiffEntry>> diffsTarget = new HashMap<>();

    Function<ContentKey, MergeResponse.ContentKeyDetails> buildContentKeyDetails =
        key ->
            ImmutableContentKeyDetails.builder()
                .key(key)
                .conflict(conflict(Conflict.ConflictType.VALUE_DIFFERS, key, "value differs"))
                .mergeBehavior(MergeBehavior.NORMAL)
                .build();

    Merge request = ImmutableMerge.builder().fromRefName("source").fromHash(sourceHash).build();
    MergeResponse response =
        ImmutableMergeResponse.builder()
            .targetBranch("main")
            .effectiveTargetHash(mainHash)
            .commonAncestor(mergeBase)
            .addDetails(buildContentKeyDetails.apply(keyOne))
            .addDetails(buildContentKeyDetails.apply(keyTwo))
            .build();

    MergeResponseInspector tested =
        new AbstractTestMergeResponseTest(request, response) {
          @Override
          protected List<ContentWithKey> mergeBaseContents() {
            return Arrays.asList(
                ContentWithKey.of(keyOne, oneBase), ContentWithKey.of(keyTwo, twoBase));
          }

          @Override
          protected Map<String, List<DiffEntry>> diffByContentId(String ref, String hash) {
            if ((ref.equals("main") || ref.equals(Detached.REF_NAME)) && hash.equals(mainHash)) {
              return diffsTarget;
            } else if ((ref.equals("source") || ref.equals(Detached.REF_NAME))
                && hash.equals(sourceHash)) {
              return diffsSource;
            } else {
              throw new IllegalArgumentException(ref + " / " + hash);
            }
          }
        };

    List<MergeResponseInspector.MergeConflictDetails> details =
        tested.collectMergeConflictDetails().collect(Collectors.toList());
    soft.assertThat(details)
        .extracting(MergeResponseInspector.MergeConflictDetails::keyOnMergeBase)
        .containsExactlyInAnyOrder(keyOne, keyTwo);
  }

  static Stream<DiffEntry> diffForRename(
      ContentKey keyFrom, Content from, ContentKey keyTo, Content to) {
    return Stream.of(diffEntry(keyFrom, from, null), diffEntry(keyTo, null, to));
  }

  static Stream<DiffEntry> diffForReadd(ContentKey key, Content from, Content to) {
    return diffForUpdate(key, from, to);
  }

  static Stream<DiffEntry> diffForUpdate(ContentKey key, Content from, Content to) {
    return Stream.of(diffEntry(key, from, to));
  }

  abstract static class AbstractTestMergeResponseTest extends BaseMergeResponseInspector {
    final Merge request;
    final MergeResponse response;

    AbstractTestMergeResponseTest(Merge request, MergeResponse response) {
      this.request = request;
      this.response = response;
    }

    @Override
    public Merge getRequest() {
      return request;
    }

    @Override
    public MergeResponse getResponse() {
      return response;
    }
  }
}
