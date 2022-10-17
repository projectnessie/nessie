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
package org.projectnessie.gc.tool.cli.options;

import static org.projectnessie.gc.expire.ExpireParameters.DEFAULT_ALLOWED_FALSE_POSITIVE_PROBABILITY;
import static org.projectnessie.gc.expire.ExpireParameters.DEFAULT_EXPECTED_FILE_COUNT;
import static org.projectnessie.gc.expire.ExpireParameters.DEFAULT_FALSE_POSITIVE_PROBABILITY;

import java.time.Instant;
import picocli.CommandLine;

public class SweepOptions {

  @CommandLine.Option(
      names = "--max-file-modification",
      description =
          "The maximum allowed file modification time. "
              + "Files newer than this timestamp will not be deleted. "
              + "Defaults to the created timestamp of the live-content-set.")
  Instant maxFileModificationTime;

  @CommandLine.Option(
      names = "--expiry-parallelism",
      description = "Number of contents that are checked in parallel.",
      defaultValue = "4")
  int parallelism;

  @CommandLine.Option(
      names = "--expected-file-count",
      description =
          "The total number of expected live files for a single content, defaults to "
              + DEFAULT_EXPECTED_FILE_COUNT
              + ".",
      defaultValue = "" + DEFAULT_EXPECTED_FILE_COUNT)
  long expectedFileCount;

  @CommandLine.Option(
      names = "--fpp",
      description =
          "The false-positive-probability used to construct the bloom-filter identifying "
              + "whether a file is live, defaults to "
              + DEFAULT_FALSE_POSITIVE_PROBABILITY
              + ".",
      defaultValue = "" + DEFAULT_FALSE_POSITIVE_PROBABILITY)
  double falsePositiveProbability;

  @CommandLine.Option(
      names = "--allowed-fpp",
      description =
          "The worst allowed effective false-positive-probability checked after the files "
              + "for a single content have been checked, defaults to "
              + DEFAULT_ALLOWED_FALSE_POSITIVE_PROBABILITY
              + ".",
      defaultValue = "" + DEFAULT_ALLOWED_FALSE_POSITIVE_PROBABILITY)
  double allowedFalsePositiveProbability;

  @CommandLine.Option(
      names = "--defer-deletes",
      negatable = true,
      description =
          "Identified unused/orphan files are by default immediately deleted. "
              + "Using deferred deletion stores the files to be deleted, so the can be inspected and deleted later. "
              + "This option is incompatible with --inmemory.")
  boolean deferDeletes;

  public boolean isDeferDeletes() {
    return deferDeletes;
  }

  public long getExpectedFileCount() {
    return expectedFileCount;
  }

  public double getFalsePositiveProbability() {
    return falsePositiveProbability;
  }

  public double getAllowedFalsePositiveProbability() {
    return allowedFalsePositiveProbability;
  }

  public int getParallelism() {
    return parallelism;
  }

  public Instant getMaxFileModificationTime() {
    return maxFileModificationTime;
  }
}
