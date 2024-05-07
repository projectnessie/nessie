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
package org.projectnessie.gc.expire;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.PrimitiveSink;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.MustBeClosed;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.files.NessieFileIOException;
import org.projectnessie.model.Content;
import org.projectnessie.storage.uri.StorageUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
public abstract class PerContentDeleteExpired {

  private static final Logger LOGGER = LoggerFactory.getLogger(PerContentDeleteExpired.class);

  public static Builder builder() {
    return ImmutablePerContentDeleteExpired.builder();
  }

  public interface Builder {
    @CanIgnoreReturnValue
    Builder expireParameters(ExpireParameters expireParameters);

    @CanIgnoreReturnValue
    Builder contentId(String contentId);

    PerContentDeleteExpired build();
  }

  /** Returns a stream of files that can be expired. */
  @SuppressWarnings("UnstableApiUsage")
  public DeleteSummary expire() {
    BloomFilter<StorageUri> filter = createBloomFilter();

    Set<StorageUri> baseLocations = new HashSet<>();
    Consumer<StorageUri> addBaseLocation =
        l -> {
          synchronized (baseLocations) {
            if (baseLocations.add(l)) {
              LOGGER.debug(
                  "live-set#{} content#{}: Identified base location {}",
                  expireParameters().liveContentSet().id(),
                  contentId(),
                  l);
            }
          }
        };

    long identifiedLiveFiles = identifyLiveFiles(filter, addBaseLocation);

    double expectedFpp = filter.expectedFpp();
    long approximateElementCount = filter.approximateElementCount();
    if (filter.expectedFpp() > expireParameters().allowedFalsePositiveProbability()) {
      LOGGER.warn(
          "live-set#{} content#{}: Aborting expire - expected FPP {} is higher than the allowed "
              + "FPP {}. Approximate files count is {}, expected is {}, real is {} live (probably less).",
          expireParameters().liveContentSet().id(),
          contentId(),
          expectedFpp,
          expireParameters().allowedFalsePositiveProbability(),
          approximateElementCount,
          expireParameters().expectedFileCount(),
          identifiedLiveFiles);
      return DeleteSummary.EMPTY;
    }

    expireParameters().liveContentSet().associateBaseLocations(contentId(), baseLocations);

    return baseLocations.stream()
        .map(
            baseLocation -> {
              try (Stream<FileReference> fileObjects = identifyExpiredFiles(filter, baseLocation)) {
                return expireParameters().fileDeleter().deleteMultiple(baseLocation, fileObjects);
              } catch (Exception e) {
                String msg = "Failed to expire objects in base location " + baseLocation;
                LOGGER.error("{}", msg, e);
                throw new RuntimeException(msg, e);
              }
            })
        .reduce(DeleteSummary.EMPTY, DeleteSummary::add, DeleteSummary::add);
  }

  /**
   * First part of {@link #expire()} to identify all files that are referenced by all live {@link
   * Content} objects.
   */
  @SuppressWarnings("UnstableApiUsage")
  private long identifyLiveFiles(
      BloomFilter<StorageUri> filter, Consumer<StorageUri> addBaseLocation) {
    LOGGER.debug(
        "live-set#{} content#{}: Start collecting files and base locations, max file modification time: {}.",
        expireParameters().liveContentSet().id(),
        contentId(),
        expireParameters().maxFileModificationTime());

    long liveFileCount;
    try (Stream<FileReference> contents =
        expireParameters()
            .liveContentSet()
            .fetchContentReferences(contentId())
            .flatMap(
                c -> {
                  @SuppressWarnings("MustBeClosedChecker")
                  Stream<FileReference> r = expireParameters().contentToFiles().extractFiles(c);
                  return r;
                })) {
      liveFileCount =
          contents
              .peek(f -> addBaseLocation.accept(f.base()))
              .map(FileReference::path)
              .peek(filter::put)
              .count();
    }

    LOGGER.debug(
        "live-set#{} content#{}: Identified {} live files (configured: {}), with an expected "
            + "false-positive-probability of {} (configured: {}).",
        expireParameters().liveContentSet().id(),
        contentId(),
        liveFileCount,
        expireParameters().expectedFileCount(),
        filter.expectedFpp(),
        expireParameters().falsePositiveProbability());

    return liveFileCount;
  }

  /**
   * Second part of {@link #expire()} to walk all base locations and identify the files that are not
   * referenced by any live content object.
   */
  @SuppressWarnings("UnstableApiUsage")
  @MustBeClosed
  private Stream<FileReference> identifyExpiredFiles(
      BloomFilter<StorageUri> filter, StorageUri baseLocation) throws NessieFileIOException {
    ExpireStats expireStats = new ExpireStats();
    long maxFileTime = expireParameters().maxFileModificationTime().toEpochMilli();

    LOGGER.debug(
        "live-set#{} content#{}: Start walking base location {}.",
        expireParameters().liveContentSet().id(),
        contentId(),
        baseLocation);

    @SuppressWarnings("MustBeClosedChecker")
    Stream<FileReference> list = expireParameters().filesLister().listRecursively(baseLocation);
    return list.filter(
            f -> {
              expireStats.totalFiles++;
              if (filter.mightContain(f.path())) {
                expireStats.liveFiles++;
                return false;
              }
              if (f.modificationTimeMillisEpoch() > maxFileTime) {
                expireStats.newFiles++;
                return false;
              }
              expireStats.expiredFiles++;
              return true;
            })
        .onClose(
            () ->
                LOGGER.info(
                    "live-set#{} content#{}: Found {} total files in base location {}, "
                        + "{} files considered expired, "
                        + "{} files considered live, "
                        + "{} files are newer than max-file-modification-time.",
                    expireParameters().liveContentSet().id(),
                    contentId(),
                    expireStats.totalFiles,
                    baseLocation,
                    expireStats.expiredFiles,
                    expireStats.liveFiles,
                    expireStats.newFiles));
  }

  private static final class ExpireStats {
    long totalFiles = 0;
    long expiredFiles = 0;
    long liveFiles = 0;
    long newFiles = 0;
  }

  @SuppressWarnings("UnstableApiUsage")
  BloomFilter<StorageUri> createBloomFilter() {
    return BloomFilter.create(
        PerContentDeleteExpired::funnel,
        expireParameters().expectedFileCount(),
        expireParameters().falsePositiveProbability());
  }

  /**
   * Add URI components discretely to the {@link PrimitiveSink}, because that is more efficient than
   * converting the {@code StorageUri} to a {@code String}, especially since the URIs are almost
   * always relative and have only the path component.
   */
  @SuppressWarnings("UnstableApiUsage")
  private static void funnel(StorageUri uri, PrimitiveSink sink) {
    funnelString(uri.scheme(), sink);
    funnelString(uri.authority(), sink);
    funnelString(uri.path(), sink);
  }

  @SuppressWarnings("UnstableApiUsage")
  private static void funnelString(String s, PrimitiveSink sink) {
    if (s != null) {
      sink.putUnencodedChars(s);
    }
  }

  abstract ExpireParameters expireParameters();

  abstract String contentId();
}
