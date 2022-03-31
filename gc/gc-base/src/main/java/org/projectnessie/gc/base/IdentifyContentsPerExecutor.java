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
package org.projectnessie.gc.base;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;

/**
 * Contains the methods that executes in spark executor for {@link
 * GCImpl#identifyExpiredContents(SparkSession)}.
 */
public class IdentifyContentsPerExecutor implements Serializable {

  private final GCParams gcParams;

  public IdentifyContentsPerExecutor(GCParams gcParams) {
    this.gcParams = gcParams;
  }

  protected Function<String, Map<String, ContentBloomFilter>> computeLiveContentsFunc(
      long bloomFilterSize, Map<String, Instant> droppedRefTimeMap, NessieApiV1 api) {
    return reference ->
        computeLiveContents(
            getCutoffTimeForRef(reference, droppedRefTimeMap),
            reference,
            droppedRefTimeMap.get(reference),
            bloomFilterSize,
            api);
  }

  protected Function<String, IdentifiedResult> computeExpiredContentsFunc(
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap, NessieApiV1 api) {
    return reference -> computeExpiredContents(liveContentsBloomFilterMap, reference, api);
  }

  private Map<String, ContentBloomFilter> computeLiveContents(
      Instant cutOffTimestamp,
      String reference,
      Instant droppedRefTime,
      long bloomFilterSize,
      NessieApiV1 api) {
    boolean isRefDroppedAfterCutoffTimeStamp =
        droppedRefTime == null || droppedRefTime.compareTo(cutOffTimestamp) >= 0;
    if (!isRefDroppedAfterCutoffTimeStamp) {
      // reference is dropped before cutoff time.
      // All the contents for all the keys are expired.
      return new HashMap<>();
    }
    Predicate<CommitMeta> liveCommitPredicate =
        commitMeta ->
            // If the commit time is newer than (think: greater than or equal to) cutoff-time,
            // then commit is live.
            commitMeta.getCommitTime().compareTo(cutOffTimestamp) >= 0;

    ImmutableGCStateParamsPerTask gcStateParamsPerTask =
        ImmutableGCStateParamsPerTask.builder()
            .api(api)
            .reference(GCUtil.deserializeReference(reference))
            .liveCommitPredicate(liveCommitPredicate)
            .bloomFilterSize(bloomFilterSize)
            .build();

    return walkLiveCommitsInReference(gcStateParamsPerTask);
  }

  private IdentifiedResult computeExpiredContents(
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap,
      String reference,
      NessieApiV1 api) {
    return walkAllCommitsInReference(
        api, GCUtil.deserializeReference(reference), liveContentsBloomFilterMap);
  }

  private Map<String, ContentBloomFilter> walkLiveCommitsInReference(
      GCStateParamsPerTask gcStateParamsPerTask) {
    Map<String, ContentBloomFilter> bloomFilterMap = new HashMap<>();
    Set<ContentKey> liveContentKeys = new HashSet<>();
    try (Stream<LogResponse.LogEntry> commits =
        StreamingUtil.getCommitLogStream(
            gcStateParamsPerTask.getApi(),
            builder ->
                builder
                    .hashOnRef(gcStateParamsPerTask.getReference().getHash())
                    .refName(Detached.REF_NAME)
                    .fetch(FetchOption.ALL),
            OptionalInt.empty())) {
      MutableBoolean foundAllLiveCommitHeadsBeforeCutoffTime = new MutableBoolean(false);
      // commit handler for the spliterator
      Consumer<LogResponse.LogEntry> commitHandler =
          logEntry ->
              handleLiveCommit(
                  gcStateParamsPerTask,
                  logEntry,
                  bloomFilterMap,
                  foundAllLiveCommitHeadsBeforeCutoffTime,
                  liveContentKeys);
      // traverse commits using the spliterator
      GCUtil.traverseLiveCommits(foundAllLiveCommitHeadsBeforeCutoffTime, commits, commitHandler);
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
    return bloomFilterMap;
  }

  private IdentifiedResult walkAllCommitsInReference(
      NessieApiV1 api,
      Reference reference,
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap) {
    IdentifiedResult result = new IdentifiedResult();
    Instant commitProtectionTime = Instant.now().minus(gcParams.getCommitProtectionDuration());
    try (Stream<LogResponse.LogEntry> commits =
        StreamingUtil.getCommitLogStream(
            api,
            builder ->
                builder
                    .hashOnRef(reference.getHash())
                    .refName(Detached.REF_NAME)
                    .fetch(FetchOption.ALL),
            OptionalInt.empty())) {
      commits.forEach(
          logEntry -> {
            // Between the bloom filter creation and this step,
            // there can be some more commits in the backend.
            // Checking them against bloom filter will give false results.
            // Hence, protect those commits using commitProtectionTime.
            if (logEntry.getCommitMeta().getCommitTime().compareTo(commitProtectionTime) < 0) {
              // this commit can be tested for expiry as it is unprotected.
              handleCommitForExpiredContents(
                  reference, logEntry, liveContentsBloomFilterMap, result);
            }
          });
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  private void handleLiveCommit(
      GCStateParamsPerTask gcStateParamsPerTask,
      LogResponse.LogEntry logEntry,
      Map<String, ContentBloomFilter> bloomFilterMap,
      MutableBoolean foundAllLiveCommitHeadsBeforeCutoffTime,
      Set<ContentKey> liveContentKeys) {
    if (logEntry.getOperations() != null) {
      boolean isExpired =
          !gcStateParamsPerTask.getLiveCommitPredicate().test(logEntry.getCommitMeta());
      if (isExpired && liveContentKeys.isEmpty()) {
        // get live content keys for this reference at hash
        // as it is the first expired commit. Time travel is supported till this state.
        try {
          gcStateParamsPerTask
              .getApi()
              .getEntries()
              .refName(Detached.REF_NAME)
              .hashOnRef(logEntry.getCommitMeta().getHash())
              .get()
              .getEntries()
              .forEach(entries -> liveContentKeys.add(entries.getName()));
        } catch (NessieNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
      logEntry.getOperations().stream()
          .filter(operation -> operation instanceof Operation.Put)
          .forEach(
              operation -> {
                boolean addContent;
                if (liveContentKeys.contains(operation.getKey())) {
                  // commit head of this key
                  addContent = true;
                  liveContentKeys.remove(operation.getKey());
                  if (liveContentKeys.isEmpty()) {
                    // found all the live commit heads before cutoff time.
                    foundAllLiveCommitHeadsBeforeCutoffTime.setTrue();
                  }
                } else {
                  addContent = !isExpired;
                }
                if (addContent) {
                  Content content = ((Operation.Put) operation).getContent();
                  bloomFilterMap
                      .computeIfAbsent(
                          content.getId(),
                          k ->
                              new ContentBloomFilter(
                                  gcStateParamsPerTask.getBloomFilterSize(),
                                  gcParams.getBloomFilterFpp()))
                      .put(content);
                }
              });
    }
  }

  private static void handleCommitForExpiredContents(
      Reference reference,
      LogResponse.LogEntry logEntry,
      Map<String, ContentBloomFilter> liveContentsBloomFilterMap,
      IdentifiedResult result) {
    if (logEntry.getOperations() != null) {
      logEntry.getOperations().stream()
          .filter(operation -> operation instanceof Operation.Put)
          .forEach(
              operation -> {
                Content content = ((Operation.Put) operation).getContent();
                ContentBloomFilter bloomFilter = liveContentsBloomFilterMap.get(content.getId());
                // when no live bloom filter exist for this content id, all the contents are
                // definitely expired.

                // Checking against live contents bloom filter can result
                //    no  --> content is considered expired (filter cannot say 'no' for live
                // contents).
                //    may be  -->  content is considered live (filter can say 'maybe' for expired
                // contents).
                // Worst case few expired contents will be considered live due to bloom filter
                // fpp.
                // But live contents never be considered as expired.
                if (bloomFilter == null || !bloomFilter.mightContain(content)) {
                  result.addContent(reference.getName(), content);
                }
              });
    }
  }

  private Instant getCutoffTimeForRef(String reference, Map<String, Instant> droppedRefTimeMap) {
    if (droppedRefTimeMap.containsKey(reference)
        && gcParams.getDeadReferenceCutOffTimeStamp() != null) {
      // if the reference is dropped and deadReferenceCutOffTimeStamp is configured, use it.
      return gcParams.getDeadReferenceCutOffTimeStamp();
    }
    return gcParams.getCutOffTimestampPerRef() == null
        ? gcParams.getDefaultCutOffTimestamp()
        : gcParams
            .getCutOffTimestampPerRef()
            .getOrDefault(
                GCUtil.deserializeReference(reference).getName(),
                gcParams.getDefaultCutOffTimestamp());
  }
}
