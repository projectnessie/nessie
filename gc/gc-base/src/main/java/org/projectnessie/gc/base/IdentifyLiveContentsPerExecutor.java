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

import com.google.common.hash.BloomFilter;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the methods that execute in spark executor for computing live content bloom filter in
 * {@link GCImpl#identifyExpiredContents(SparkSession)}.
 */
public class IdentifyLiveContentsPerExecutor extends IdentifyContentsPerExecutor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IdentifyLiveContentsPerExecutor.class);

  public IdentifyLiveContentsPerExecutor(GCParams gcParams) {
    super(gcParams);
  }

  protected Function<String, LiveContentsResult> computeLiveContentsFunc(
      long bloomFilterSize, Map<String, Instant> droppedRefTimeMap) {
    return reference ->
        computeLiveContents(
            getCutoffTimeForRef(reference, droppedRefTimeMap),
            reference,
            droppedRefTimeMap.get(reference),
            bloomFilterSize);
  }

  private LiveContentsResult computeLiveContents(
      Instant cutOffTimestamp, String reference, Instant droppedRefTime, long bloomFilterSize)
      throws NessieNotFoundException {
    NessieApiV1 api = GCUtil.getApi(gcParams.getNessieClientConfigs());
    TaskContext.get()
        .addTaskCompletionListener(
            context -> {
              LOGGER.info("Closing the nessie api for compute live contents task");
              api.close();
            });
    LOGGER.debug("Computing live contents for {}", reference);
    Reference ref = GCUtil.deserializeReference(reference);
    boolean isRefDroppedAfterCutoffTimeStamp =
        droppedRefTime == null || droppedRefTime.compareTo(cutOffTimestamp) >= 0;
    if (!isRefDroppedAfterCutoffTimeStamp) {
      // reference is dropped before cutoff time.
      // All the contents for all the keys are expired.
      return ImmutableLiveContentsResult.builder()
          .referenceName(ref.getName())
          .hashOnReference(ref.getHash())
          .bloomFilter(null)
          .build();
    }
    return walkLiveCommitsInReference(
        api,
        ref,
        commitMeta ->
            // If the commit time is newer than (think: greater than or equal to) cutoff-time,
            // then commit is live.
            commitMeta.getCommitTime().compareTo(cutOffTimestamp) >= 0,
        bloomFilterSize);
  }

  private LiveContentsResult walkLiveCommitsInReference(
      NessieApiV1 api,
      Reference reference,
      Predicate<CommitMeta> liveCommitPredicate,
      long bloomFilterSize)
      throws NessieNotFoundException {
    Stream<LogResponse.LogEntry> commits =
        api
            .getCommitLog()
            .hashOnRef(reference.getHash())
            .refName(Detached.REF_NAME)
            .fetch(FetchOption.ALL)
            .stream();
    BloomFilter<Content> bloomFilter =
        BloomFilter.create(ContentFunnel.INSTANCE, bloomFilterSize, gcParams.getBloomFilterFpp());
    Set<ContentKey> liveContentKeys = new HashSet<>();
    MutableBoolean foundAllRequiredCommits = new MutableBoolean(false);
    // traverse commits using the spliterator
    String lastVisitedHash =
        traverseLiveCommits(
            foundAllRequiredCommits,
            commits,
            logEntry ->
                handleLiveCommit(
                    api,
                    liveCommitPredicate,
                    logEntry,
                    bloomFilter,
                    foundAllRequiredCommits,
                    liveContentKeys));
    if (lastVisitedHash == null) {
      // can happen when the branch has no commits.
      lastVisitedHash = reference.getHash();
    }
    LOGGER.debug(
        "For the reference {} last traversed commit {}", reference.getName(), lastVisitedHash);
    return ImmutableLiveContentsResult.builder()
        .referenceName(reference.getName())
        .hashOnReference(reference.getHash())
        .bloomFilter(bloomFilter)
        .build();
  }

  /**
   * Traverse the live commits stream till an entry is seen for each live content key at the time of
   * cutoff time and at least reached expired commits.
   *
   * @param foundAllRequiredCommits condition to stop traversing
   * @param commits stream of {@link LogResponse.LogEntry}
   * @param commitHandler consumer of {@link LogResponse.LogEntry}
   * @return last visited commit hash. It is the commit hash when latest commit is found for all the
   *     live keys at cutoff time.
   */
  static String traverseLiveCommits(
      MutableBoolean foundAllRequiredCommits,
      Stream<LogResponse.LogEntry> commits,
      Consumer<LogResponse.LogEntry> commitHandler) {
    AtomicReference<String> lastVisitedHash = new AtomicReference<>();
    Spliterator<LogResponse.LogEntry> src = commits.spliterator();
    // Use a Spliterator to limit the processed commits to the "live" commits - i.e. stop traversing
    // the expired commits once a commit is seen for each live content key at the time of cutoff
    // time.
    new Spliterators.AbstractSpliterator<LogResponse.LogEntry>(src.estimateSize(), 0) {
      private boolean more = true;

      @Override
      public boolean tryAdvance(Consumer<? super LogResponse.LogEntry> action) {
        if (!more) {
          return false;
        }
        more =
            src.tryAdvance(
                    logEntry -> {
                      // traverse until latest commit is found for all the live keys
                      // at the time of cutoff time.
                      if (foundAllRequiredCommits.isFalse()) {
                        action.accept(logEntry);
                      }
                      lastVisitedHash.set(logEntry.getCommitMeta().getHash());
                    })
                // check if the commitHandler has updated the foundAllRequiredCommits.
                && foundAllRequiredCommits.isFalse();
        return more;
      }
    }.forEachRemaining(commitHandler);

    return lastVisitedHash.get();
  }

  private void handleLiveCommit(
      NessieApiV1 api,
      Predicate<CommitMeta> liveCommitPredicate,
      LogResponse.LogEntry logEntry,
      BloomFilter<Content> bloomFilter,
      MutableBoolean foundAllRequiredCommits,
      Set<ContentKey> liveContentKeys) {
    if (logEntry.getOperations() != null) {
      boolean isExpired = !liveCommitPredicate.test(logEntry.getCommitMeta());
      if (isExpired && liveContentKeys.isEmpty()) {
        // get live content keys for this reference at hash
        // as it is the first expired commit. Time travel is supported till this state.
        try {
          api
              .getEntries()
              .hashOnRef(logEntry.getCommitMeta().getHash())
              .refName(Detached.REF_NAME)
              .stream()
              .forEach(entry -> liveContentKeys.add(entry.getName()));

          if (liveContentKeys.isEmpty()) {
            // no contents are live at the time of cutoff time
            foundAllRequiredCommits.setTrue();
            return;
          }
        } catch (NessieNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
      logEntry.getOperations().stream()
          .filter(Operation.Put.class::isInstance)
          .forEach(
              operation -> {
                boolean addContent;
                if (liveContentKeys.remove(operation.getKey())) {
                  // found the latest commit for this live key at the cutoff time.
                  addContent = true;
                  if (liveContentKeys.isEmpty()) {
                    // found the latest commit for all the live keys at the cutoff time.
                    foundAllRequiredCommits.setTrue();
                  }
                } else {
                  addContent = !isExpired;
                }
                if (addContent) {
                  bloomFilter.put(((Operation.Put) operation).getContent());
                }
              });
    }
  }
}
