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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.projectnessie.gc.expire.StoragePrefixes;
import org.projectnessie.storage.uri.StorageUri;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;

public class SweepOptions {

  /** The command that this set of options is mixed into. */
  @CommandLine.Spec(CommandLine.Spec.Target.MIXEE)
  CommandSpec commandSpec;

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

  @CommandLine.Option(
      names = "--storage-prefix",
      split = ",",
      description = {
        "Optional storage prefix, an absolute URI like 's3://bucket/team_a/'. Can be repeated or "
            + "given as a comma separated list.",
        "If set, the sweep phase only lists and deletes files in table/view base locations at or "
            + "below one of the prefixes. Whole path segments are matched, 's3://bucket/team_a' "
            + "does not match 's3://bucket/team_ab/'.",
        "The scheme and authority must be the same as in the table/view metadata, for example "
            + "'s3a://' does not match 's3://'.",
        "The live files of all contents are still collected, because the base locations are only "
            + "known after that. Base locations above the prefix are skipped.",
        "A base location that is shared with, or contains, the base location of another table can "
            + "delete files of that table, with or without this option."
      })
  List<String> storagePrefixes = new ArrayList<>();

  public boolean isDeferDeletes() {
    return deferDeletes;
  }

  public List<String> getStoragePrefixes() {
    return storagePrefixes;
  }

  /**
   * Creates the filter for the base locations to sweep from the {@code --storage-prefix} options.
   *
   * @return the filter, or empty if no storage prefix is configured, which means no restriction
   * @throws ParameterException if a storage prefix is not an absolute URI
   */
  public Optional<Predicate<StorageUri>> createBaseLocationFilter() {
    if (storagePrefixes.isEmpty()) {
      return Optional.empty();
    }
    List<StorageUri> prefixes =
        storagePrefixes.stream().map(this::parseStoragePrefix).collect(Collectors.toList());
    return Optional.of(StoragePrefixes.atOrBelowAnyOf(prefixes));
  }

  /**
   * Fails for invalid option values. Meant to be called before long-running work starts, for
   * example before the mark phase of the {@code gc} command.
   */
  public void validate() {
    createBaseLocationFilter();
  }

  private StorageUri parseStoragePrefix(String prefix) {
    String trimmed = prefix.trim();
    if (trimmed.isEmpty()) {
      throw new ParameterException(
          commandSpec.commandLine(), "Option --storage-prefix must not be empty");
    }
    StorageUri uri = StorageUri.of(trimmed);
    if (!uri.isAbsolute()) {
      throw new ParameterException(
          commandSpec.commandLine(),
          "Invalid --storage-prefix '"
              + prefix
              + "', must be an absolute URI including the scheme, for example 's3://bucket/team_a/'");
    }
    return uri;
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
