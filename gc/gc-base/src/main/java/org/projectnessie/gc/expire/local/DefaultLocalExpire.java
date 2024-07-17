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
package org.projectnessie.gc.expire.local;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.projectnessie.gc.expire.Expire;
import org.projectnessie.gc.expire.ExpireParameters;
import org.projectnessie.gc.expire.PerContentDeleteExpired;
import org.projectnessie.gc.files.DeleteSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expire-contents &amp; delete-orphan-files (the <em>sweep</em> phase of the mark-and-sweep
 * approach) implementation using a local thread pool.
 */
@Value.Immutable
public abstract class DefaultLocalExpire implements Expire {

  public static final int DEFAULT_PARALLELISM = 4;

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLocalExpire.class);

  public static Builder builder() {
    return ImmutableDefaultLocalExpire.builder();
  }

  public interface Builder {
    @CanIgnoreReturnValue
    Builder expireParameters(ExpireParameters expireParameters);

    /**
     * Configures the number of contents that can expire concurrently, default is {@value
     * #DEFAULT_PARALLELISM}.
     */
    @CanIgnoreReturnValue
    Builder parallelism(int parallelism);

    DefaultLocalExpire build();
  }

  @Override
  public DeleteSummary expire() {
    LOGGER.info("live-set#{}: Starting expiry.", expireParameters().liveContentSet().id());
    Instant started = clock().instant();
    expireParameters().liveContentSet().startExpireContents(started);

    @SuppressWarnings("resource")
    ForkJoinPool forkJoinPool = new ForkJoinPool(parallelism());
    RuntimeException error = null;
    try {
      DeleteSummary deleteSummary =
          forkJoinPool.invoke(ForkJoinTask.adapt(this::expireInForkJoinPool));
      LOGGER.info(
          "live-set#{}: Expiry finished, took {}, deletion summary: {}.",
          expireParameters().liveContentSet().id(),
          Duration.between(started, clock().instant()),
          deleteSummary);
      return deleteSummary;
    } catch (RuntimeException e) {
      error = e;
      throw e;
    } finally {
      expireParameters().liveContentSet().finishedExpireContents(clock().instant(), error);
      forkJoinPool.shutdown();
    }
  }

  private DeleteSummary expireInForkJoinPool() {
    try (Stream<String> contentIds = expireParameters().liveContentSet().fetchContentIds()) {
      return contentIds
          .parallel()
          .map(this::expireSingleContent)
          .reduce(DeleteSummary.EMPTY, DeleteSummary::add);
    }
  }

  private DeleteSummary expireSingleContent(String contentId) {
    LOGGER.debug(
        "live-set#{}: Expiring content ID {}.",
        expireParameters().liveContentSet().id(),
        contentId);
    return PerContentDeleteExpired.builder()
        .expireParameters(expireParameters())
        .contentId(contentId)
        .build()
        .expire();
  }

  abstract ExpireParameters expireParameters();

  @Value.Default
  int parallelism() {
    return DEFAULT_PARALLELISM;
  }

  @Value.Default
  @VisibleForTesting
  Clock clock() {
    return Clock.systemUTC();
  }

  @Value.Check
  void verify() {
    Preconditions.checkArgument(parallelism() >= 1, "Parallelism must be greater than 0");
  }
}
