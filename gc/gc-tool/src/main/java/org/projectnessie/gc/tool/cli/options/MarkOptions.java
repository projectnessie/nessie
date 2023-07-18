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
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.projectnessie.gc.identify.CutoffPolicy;
import org.projectnessie.gc.identify.PerRefCutoffPolicySupplier;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;

public class MarkOptions {
  @SuppressWarnings("JavaTimeDefaultTimeZone")
  public static final ZonedDateTime NOW = ZonedDateTime.now();

  @CommandLine.Mixin NessieOptions nessie;

  @CommandLine.Option(
      names = {"-R", "--cutoff-ref-time"},
      description = "Reference timestamp for durations specified for --cutoff. Defaults to 'now'.")
  ZonedDateTime cutoffPolicyRefTime = NOW;

  @CommandLine.Option(
      names = {"-c", "--default-cutoff"},
      description = {
        "Default cutoff policy. Policies can be one of:",
        "- number of commits as an integer value",
        "- a duration (see java.time.Duration)",
        "- an ISO instant",
        "- 'NONE', means everything's considered as live"
      },
      defaultValue = "NONE")
  String defaultCutoffPolicy;

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
        "- an ISO instant",
        "- 'NONE', means everything's considered as live"
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
                  String pattern = "^" + e.getKey() + "$";
                  Predicate<String> predicate = Pattern.compile(pattern).asPredicate();

                  CutoffPolicy cutoffPolicy =
                      parseCutoffPolicy(
                          e.getValue(),
                          () -> "for reference name regexpression predicate '" + e.getKey() + "'");

                  return Maps.immutableEntry(predicate, cutoffPolicy);
                })
            .collect(Collectors.toList());

    return namedReference ->
        policies.stream()
            .filter(e -> e.getKey().test(namedReference.getName()))
            .map(Entry::getValue)
            .findFirst()
            .orElse(defaultCutoffPolicy());
  }

  private CutoffPolicy defaultCutoffPolicy() {
    return parseCutoffPolicy(defaultCutoffPolicy, () -> "default cutoff policy");
  }

  CutoffPolicy parseCutoffPolicy(String value, Supplier<String> errorMessageSuffix) {
    try {
      return CutoffPolicy.parseStringToCutoffPolicy(value, cutoffPolicyRefTime);
    } catch (Exception ex) {
      throw new ParameterException(
          commandSpec.commandLine(),
          "Failed to parse cutoff-value '"
              + value
              + "' "
              + errorMessageSuffix.get()
              + ", must be either the number of commits, a duration (as per java.time.Duration) or an ISO instant (like 2011-12-03T10:15:30Z)",
          ex);
    }
  }
}
