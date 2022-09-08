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

import com.google.common.collect.Maps;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.projectnessie.gc.identify.CutoffPolicy;
import org.projectnessie.gc.identify.PerRefCutoffPolicySupplier;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;

public class MarkOptions {
  public static final Instant NOW = Instant.now();

  @CommandLine.Mixin NessieOptions nessie;

  @CommandLine.Option(
      names = "--cutoff-ref-time",
      description = "Reference timestamp for durations specified for --cutoff. Defaults to 'now'.")
  Instant cutoffPolicyRefTime = NOW;

  @CommandLine.Option(
      names = {"-C", "--cutoff"},
      arity = "*",
      split = ",",
      description = {
        "Cutoff policies per reference names. Supplied as a ref-name-pattern=policy tuple.",
        "Reference name patterns are regular expressions.",
        "Policies can be one of:",
        "- number of commits as an integer value",
        "- a duration (see java.time.Duration)",
        "- an ISO instant"
      })
  Map<String, String> cutoffPolicies = new LinkedHashMap<>();

  @CommandLine.Option(
      names = "--write-live-set-id-to",
      description = "Optional, the file name to persist the created live-set-id to.")
  Path liveSetIdFile;

  @CommandLine.Option(
      names = "--identify-parallelism",
      defaultValue = "4",
      description = "Number of Nessie references that can be walked in parallel.")
  int parallelism;

  @CommandLine.Spec CommandSpec commandSpec;

  public NessieOptions getNessie() {
    return nessie;
  }

  public int getParallelism() {
    return parallelism;
  }

  public Path getLiveSetIdFile() {
    return liveSetIdFile;
  }

  /**
   * Parses the {@code --cutoff} parameters, syntax is {@code ref-name-regex '=' number-of-commits |
   * duration | ISO-instant}.
   */
  public PerRefCutoffPolicySupplier createPerRefCutoffPolicySupplier() {
    List<Entry<Predicate<String>, CutoffPolicy>> policies =
        cutoffPolicies.entrySet().stream()
            .map(
                e -> {
                  Predicate<String> predicate = Pattern.compile(e.getKey()).asPredicate();

                  Exception ex;
                  try {
                    return Maps.immutableEntry(
                        predicate, CutoffPolicy.numCommits(Integer.parseInt(e.getValue())));
                  } catch (NumberFormatException f) {
                    ex = f;
                  }
                  try {
                    return Maps.immutableEntry(
                        predicate,
                        CutoffPolicy.atTimestamp(
                            cutoffPolicyRefTime.minus(Duration.parse(e.getValue()))));
                  } catch (DateTimeException f) {
                    ex.addSuppressed(f);
                  }
                  try {
                    return Maps.immutableEntry(
                        predicate,
                        CutoffPolicy.atTimestamp(
                            Instant.from(DateTimeFormatter.ISO_INSTANT.parse(e.getValue()))));
                  } catch (DateTimeException f) {
                    ex.addSuppressed(f);
                  }
                  throw new ParameterException(
                      commandSpec.commandLine(),
                      "Failed to parse cutoff-value '"
                          + e.getValue()
                          + "' for key '"
                          + e.getKey()
                          + "', must be either the number of commits, a duration (as per java.time.Duration) or an ISO instant (like 2011-12-03T10:15:30Z)",
                      ex);
                })
            .collect(Collectors.toList());

    return namedReference ->
        policies.stream()
            .filter(e -> e.getKey().test(namedReference.getName()))
            .map(Entry::getValue)
            .findFirst()
            .orElse(CutoffPolicy.NONE);
  }
}
