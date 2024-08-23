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
package org.projectnessie.gc.identify;

import static java.lang.String.format;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.gc.contents.AddContents;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.repository.RepositoryConnector;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Detached;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Identifies all live {@link Content} objects in Nessie, the <em>mark</em> phase of the
 * mark-and-sweep approach.
 *
 * <p>Nessie's garbage collection is a mark-and-sweep collector. Instances of this class should only
 * be {@link #identifyLiveContents() used} once.
 *
 * <p>This class implements the mark phase that identifies all live content objects.
 *
 * <p>Behavior of this class is controlled via a couple of parameters, which provide:
 *
 * <ul>
 *   <li>A function to retrieve the {@link #cutOffPolicySupplier() cut-off timestamp for a
 *       reference}.
 *   <li>A predicate to test whether a {@link #contentTypeFilter() content type is handled} by the
 *       GC run.
 *   <li>A consumer via {@link #liveContentSetsRepository()} for the identified live content
 *       objects}.
 *   <li>The desired number of named-references being walked concurrently.
 *   <li>A {@link #visitedDeduplicator() de-duplication functionality} to prevent walking the same
 *       commit(s) with compatible cut-off timestamps.
 * </ul>
 */
@Value.Immutable
public abstract class IdentifyLiveContents {
  private static final Logger LOGGER = LoggerFactory.getLogger(IdentifyLiveContents.class);

  public static final int DEFAULT_PARALLELISM = 4;

  private final AtomicBoolean executed = new AtomicBoolean();

  public static Builder builder() {
    return ImmutableIdentifyLiveContents.builder();
  }

  public interface Builder {
    /**
     * Provides a function that provides the {@link CutoffPolicy} for a given reference.
     *
     * <p>If the implementation uses relative times (aka durations), it is highly recommended that
     * the result of equal relative times (durations) returns deterministic results. This means,
     * that for two (different) references configured with the same cut-off-duration, the function
     * shall return the same {@link Instant}. In other words, all calculations should be performed
     * against a "constant" value of "now". This will improve the efficiency of implementations for
     * {@link #visitedDeduplicator()} like {@link VisitedDeduplicator}.
     *
     * <p>The implementing {@link Function} must never return {@code null}.
     *
     * @see #referenceComparator(ReferenceComparator)
     */
    @CanIgnoreReturnValue
    Builder cutOffPolicySupplier(PerRefCutoffPolicySupplier cutOffTimestamp);

    /** Checks whether the given content-type is handled by the GC implementation. */
    @CanIgnoreReturnValue
    Builder contentTypeFilter(ContentTypeFilter contentTypeFilter);

    /**
     * The consumer that is called when a live {@link Content} object has been found. Parameters are
     * the commit-ID at which the content object has been found and the content object itself.
     *
     * <p>Implementations must be aware that multiple calls for the same {@link Content} object will
     * occur, so deduplication is recommended.
     */
    @CanIgnoreReturnValue
    Builder liveContentSetsRepository(LiveContentSetsRepository liveContentSetsRepository);

    @CanIgnoreReturnValue
    Builder contentToContentReference(ContentToContentReference contentToContentReference);

    /** Encapsulates all calls against Nessie, abstracted for testing purposes. */
    @CanIgnoreReturnValue
    Builder repositoryConnector(RepositoryConnector repositoryConnector);

    /**
     * The predicate called to prevent traversing the same commit-ID with a compatible
     * cut-off-timestamp. The default implementation does not prevent unnecessary commit-log
     * scanning.
     *
     * @see #referenceComparator(ReferenceComparator)
     */
    @CanIgnoreReturnValue
    Builder visitedDeduplicator(VisitedDeduplicator visitedDeduplicator);

    /**
     * Optional ability to sort all Nessie named references to make the configured {@link
     * #visitedDeduplicator()} work more efficiently. For example the {@link
     * DefaultVisitedDeduplicator} benefits from processing references in the reverse order of a
     * reference's highest retention time.
     *
     * @see #cutOffPolicySupplier(PerRefCutoffPolicySupplier)
     * @see #visitedDeduplicator(VisitedDeduplicator)
     */
    @CanIgnoreReturnValue
    Builder referenceComparator(ReferenceComparator referenceComparator);

    /**
     * Configures the number of references that can expire concurrently, default is {@value
     * #DEFAULT_PARALLELISM}.
     */
    @CanIgnoreReturnValue
    Builder parallelism(int parallelism);

    IdentifyLiveContents build();
  }

  /**
   * Identifies the live content objects.
   *
   * @return the ID of the live-contents-set, from {@link AddContents#id()}, which is used to later
   *     retrieve the {@link org.projectnessie.gc.contents.LiveContentSet} used by {@link
   *     org.projectnessie.gc.expire.Expire} implementations via {@link
   *     org.projectnessie.gc.expire.ExpireParameters#liveContentSet()}.
   */
  public UUID identifyLiveContents() {
    if (!executed.compareAndSet(false, true)) {
      throw new IllegalStateException("identifyLiveContents() has already been called.");
    }

    @SuppressWarnings("resource")
    ForkJoinPool forkJoinPool = new ForkJoinPool(parallelism());
    try {
      return forkJoinPool.invoke(ForkJoinTask.adapt(this::walkAllReferences));
    } finally {
      forkJoinPool.shutdown();
    }
  }

  private UUID walkAllReferences() {
    try (AddContents addContents = liveContentSetsRepository().newAddContents()) {
      try {
        @SuppressWarnings("resource")
        Stream<Reference> refs = repositoryConnector().allReferences();

        // If a Reference comparator is configured, then apply it to the stream of references.
        // Note: Stream.sorted() has the side effect that all references will be fetched first and
        // sorted.
        ReferenceComparator refsCmp = referenceComparator();
        if (refsCmp != null) {
          refs = refs.sorted(refsCmp);
        }

        Optional<ReferencesWalkResult> result =
            refs.parallel()
                .map(ref -> identifyContentsForReference(addContents, ref))
                .reduce(ReferencesWalkResult::add);

        LOGGER.info(
            "live-set#{}: Finished walking all named references, took {}: {}.",
            addContents.id(),
            Duration.between(addContents.created(), clock().instant()),
            result.isPresent() ? result.get() : "<no result>");

        addContents.finished();

        return addContents.id();
      } catch (NessieNotFoundException e) {
        LOGGER.error("Failed to walk all references.", e);
        addContents.finishedExceptionally(e);
        throw new RuntimeException(e);
      } catch (RuntimeException e) {
        LOGGER.error("Failed to walk all references.", e);
        addContents.finishedExceptionally(e);
        throw e;
      }
    }
  }

  @SuppressWarnings("resource")
  private ReferencesWalkResult identifyContentsForReference(
      AddContents addContents, Reference namedReference) {
    CutoffPolicy cutoffPolicy = cutOffPolicySupplier().get(namedReference);

    if (visitedDeduplicator().alreadyVisited(cutoffPolicy.timestamp(), namedReference.getHash())) {
      // This commit-ID has already been visited with the same (or maybe an older/smaller)
      // cut-off-timestamp, can abort.
      LOGGER.debug(
          "live-set#{}: Not submitting task to walk {}, it has already already visited using a"
              + "compatible cut-off timestamp.",
          addContents.id(),
          namedReference);
      return ReferencesWalkResult.singleShortCircuit(0, 0);
    }

    LOGGER.info(
        "live-set#{}: Start walking the commit log of {} using {}.",
        addContents.id(),
        namedReference,
        cutoffPolicy);

    int numCommits = 0;
    long numContents = 0;

    try (Stream<LogResponse.LogEntry> commits = repositoryConnector().commitLog(namedReference)) {

      LogEntryHolder holder = new LogEntryHolder();
      String lastCommitId = null;
      String finalCommitId = null;

      for (Spliterator<LogResponse.LogEntry> spliterator = commits.spliterator();
          spliterator.tryAdvance(holder::set); ) {

        numCommits++;

        LogResponse.LogEntry logEntry = holder.logEntry;
        CommitMeta commitMeta = logEntry.getCommitMeta();
        Instant commitTime = commitMeta.getCommitTime();
        String commitHash = commitMeta.getHash();
        List<Operation> operations = logEntry.getOperations();
        if (operations == null) {
          // Shout never happen, but in case it's a commit without any operation, just ignore it.
          continue;
        }
        if (commitTime == null || commitHash == null) {
          throw new IllegalStateException("Mandatory information is null in log entry " + logEntry);
        }

        if (visitedDeduplicator().alreadyVisited(cutoffPolicy.timestamp(), commitHash)) {
          // This commit-ID has already been visited with the same (or maybe an older/smaller)
          // cut-off-timestamp, can abort.
          LOGGER.info(
              "live-set#{}: Finished walking the commit log of {} using {} after {} commits, commit {} has already been "
                  + "checked using a compatible cut-off timestamp.",
              addContents.id(),
              namedReference,
              cutoffPolicy,
              numCommits,
              commitHash);
          return ReferencesWalkResult.singleShortCircuit(numCommits, numContents);
        }

        // The HEAD commit is always live, consult cutoff-policy for all other commits
        if (lastCommitId == null || !cutoffPolicy.isCutoff(commitTime, numCommits)) {
          // commit is "live"
          LOGGER.debug(
              "live-set#{}: Checking commit {} with {} operations via {}.",
              addContents.id(),
              commitHash,
              operations.size(),
              namedReference);
          lastCommitId = commitHash;
          numContents +=
              addContents.addLiveContent(
                  operations.stream()
                      .filter(operation -> operation instanceof Operation.Put)
                      .filter(
                          operation ->
                              contentTypeFilter()
                                  .test(((Operation.Put) operation).getContent().getType()))
                      .map(
                          operation -> {
                            Operation.Put put = (Operation.Put) operation;
                            Content content = put.getContent();

                            LOGGER.debug(
                                "live-set#{}: Adding content reference for {} from commit {}.",
                                addContents.id(),
                                put,
                                commitHash);

                            return contentToContentReference()
                                .contentToReference(content, commitHash, put.getKey());
                          }));
        } else {
          // 1st non-live commit
          finalCommitId = commitHash;
          break;
        }
      }

      // Always consider all content reachable from the last live commit.
      if (lastCommitId != null) {
        try {
          numContents += collectAllKeys(addContents, Detached.of(lastCommitId));
        } catch (NessieNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      LOGGER.info(
          "live-set#{}: Finished walking the commit log of {} using {} after {} commits, {}",
          addContents.id(),
          namedReference,
          cutoffPolicy,
          numCommits,
          finalCommitId != null
              ? format("commit %s is the first non-live commit.", finalCommitId)
              : "no more commits");
      return ReferencesWalkResult.single(numCommits, numContents);
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(
          "GC-run#" + addContents.id() + ": Could not find reference " + namedReference, e);
    }
  }

  @SuppressWarnings("resource")
  private long collectAllKeys(AddContents addContents, Detached ref)
      throws NessieNotFoundException {
    return addContents.addLiveContent(
        repositoryConnector()
            .allContents(ref, contentTypeFilter().validTypes())
            .map(
                e ->
                    contentToContentReference()
                        .contentToReference(e.getValue(), ref.getHash(), e.getKey())));
  }

  private static final class ReferencesWalkResult {
    final int numReferences;
    final int numCommits;
    final int shortCircuits;
    final long numContents;

    private ReferencesWalkResult(
        int numReferences, int numCommits, int shortCircuits, long numContents) {
      this.numReferences = numReferences;
      this.numCommits = numCommits;
      this.shortCircuits = shortCircuits;
      this.numContents = numContents;
    }

    static ReferencesWalkResult singleShortCircuit(int numCommits, long numContents) {
      return new ReferencesWalkResult(1, numCommits, 1, numContents);
    }

    static ReferencesWalkResult single(int numCommits, long numContents) {
      return new ReferencesWalkResult(1, numCommits, 0, numContents);
    }

    ReferencesWalkResult add(ReferencesWalkResult other) {
      return new ReferencesWalkResult(
          numReferences + other.numReferences,
          numCommits + other.numCommits,
          shortCircuits + other.shortCircuits,
          numContents + other.numContents);
    }

    @Override
    public String toString() {
      return "numReferences="
          + numReferences
          + ", numCommits="
          + numCommits
          + ", numContents="
          + numContents
          + ", shortCircuits="
          + shortCircuits;
    }
  }

  private static final class LogEntryHolder {
    LogResponse.LogEntry logEntry;

    void set(LogResponse.LogEntry logEntry) {
      this.logEntry = logEntry;
    }
  }

  abstract PerRefCutoffPolicySupplier cutOffPolicySupplier();

  abstract ContentTypeFilter contentTypeFilter();

  abstract LiveContentSetsRepository liveContentSetsRepository();

  abstract ContentToContentReference contentToContentReference();

  abstract RepositoryConnector repositoryConnector();

  @Value.Default
  Clock clock() {
    return Clock.systemUTC();
  }

  @Value.Default
  VisitedDeduplicator visitedDeduplicator() {
    return VisitedDeduplicator.NOOP;
  }

  @Nullable
  abstract ReferenceComparator referenceComparator();

  @Value.Default
  int parallelism() {
    return DEFAULT_PARALLELISM;
  }

  @Value.Check
  void verify() {
    Preconditions.checkArgument(parallelism() >= 1, "Parallelism must be greater than 0");
  }
}
