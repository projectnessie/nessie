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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Instant;
import org.immutables.value.Value;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.files.FilesLister;
import org.projectnessie.gc.identify.IdentifyLiveContents;

@Value.Immutable
public interface ExpireParameters {

  // Following defaults result in a serialized bloom filter size of about 3000000 bytes.
  long DEFAULT_EXPECTED_FILE_COUNT = 1_000_000L;
  double DEFAULT_FALSE_POSITIVE_PROBABILITY = 0.00001d;
  double DEFAULT_ALLOWED_FALSE_POSITIVE_PROBABILITY = 0.0001d;

  static Builder builder() {
    return ImmutableExpireParameters.builder();
  }

  interface Builder {

    /**
     * The total number of expected live files for a single content, defaults to {@value
     * #DEFAULT_EXPECTED_FILE_COUNT}, used to construct the bloom-filter identifying whether a file
     * is live, see {@link #falsePositiveProbability(double)}.
     */
    @CanIgnoreReturnValue
    Builder expectedFileCount(long expectedFileCount);

    /**
     * The false-positive-probability used to construct the bloom-filter identifying whether a file
     * is live, defaults to {@value #DEFAULT_EXPECTED_FILE_COUNT}, see {@link
     * #expectedFileCount(long)}.
     */
    @CanIgnoreReturnValue
    Builder falsePositiveProbability(double falsePositiveProbability);

    @CanIgnoreReturnValue
    Builder allowedFalsePositiveProbability(double allowedFalsePositiveProbability);

    /** Function used to recustively list files from a base location. */
    @CanIgnoreReturnValue
    Builder filesLister(FilesLister filesLister);

    /** Function to retrieve all live files for a content-reference. */
    @CanIgnoreReturnValue
    Builder contentToFiles(ContentToFiles contentToFiles);

    /**
     * The set of live content objects, the result of {@link
     * IdentifyLiveContents#identifyLiveContents()}.
     */
    @CanIgnoreReturnValue
    Builder liveContentSet(LiveContentSet liveContentSet);

    /** Files newer than this instant will not be deleted. */
    @CanIgnoreReturnValue
    Builder maxFileModificationTime(Instant maxFileModificationTime);

    /** Function to delete files. */
    @CanIgnoreReturnValue
    Builder fileDeleter(FileDeleter fileDeleter);

    ExpireParameters build();
  }

  @Value.Default
  default long expectedFileCount() {
    return DEFAULT_EXPECTED_FILE_COUNT;
  }

  @Value.Default
  default double falsePositiveProbability() {
    return DEFAULT_FALSE_POSITIVE_PROBABILITY;
  }

  @Value.Default
  default double allowedFalsePositiveProbability() {
    return DEFAULT_ALLOWED_FALSE_POSITIVE_PROBABILITY;
  }

  FilesLister filesLister();

  ContentToFiles contentToFiles();

  LiveContentSet liveContentSet();

  Instant maxFileModificationTime();

  FileDeleter fileDeleter();
}
