/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.operator.reconciler.nessiegc.resource.options;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Min;
import io.sundr.builder.annotations.Buildable;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import org.projectnessie.operator.events.EventReason;
import org.projectnessie.operator.exception.InvalidSpecException;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record MarkOptions(
    @JsonPropertyDescription(
            """
            Default cutoff policy. Policies can be one of:
            - number of commits as an integer value;
            - an ISO duration (see java.time.Duration);
            - an ISO instant (see java.time.Instant);
            - 'NONE', means everything's considered as live.""")
        @Default("NONE")
        String defaultCutoffPolicy,
    @JsonPropertyDescription(
            """
            Cutoff policies per reference names. Supplied as a ref-name-pattern=policy tuple.
            Reference name patterns are regular expressions.
            Policies can be one of:
            - number of commits as an integer value;
            - an ISO duration (see java.time.Duration);
            - an ISO instant (see java.time.Instant);
            - 'NONE', means everything's considered as live.""")
        @Default("{}")
        Map<String, String> cutoffPolicies,
    @JsonPropertyDescription("Number of Nessie references that can be walked in parallel.")
        @Default("4")
        @Min(1)
        Integer parallelism) {

  public MarkOptions() {
    this(null, null, null);
  }

  public MarkOptions {
    defaultCutoffPolicy = defaultCutoffPolicy != null ? defaultCutoffPolicy : "NONE";
    cutoffPolicies = cutoffPolicies != null ? cutoffPolicies : Map.of();
    parallelism = parallelism != null ? parallelism : 4;
  }

  public void validate() {
    if (parallelism < 1) {
      throw new InvalidSpecException(EventReason.InvalidGcConfig, "parallelism must be at least 1");
    }
    validateCutoffPolicy(defaultCutoffPolicy);
    cutoffPolicies.forEach((refNamePattern, policy) -> validateCutoffPolicy(policy));
  }

  private static void validateCutoffPolicy(String cutoffPolicy) {
    if (!cutoffPolicy.equals("NONE")) {
      try {
        // try to parse as integer
        Integer.parseInt(cutoffPolicy);
      } catch (NumberFormatException e) {
        // try to parse as duration
        try {
          Duration.parse(cutoffPolicy);
        } catch (DateTimeParseException e2) {
          // try to parse as instant
          try {
            Instant.parse(cutoffPolicy);
          } catch (DateTimeParseException e3) {
            throw new InvalidSpecException(
                EventReason.InvalidGcConfig,
                "Invalid cutoff policy '"
                    + cutoffPolicy
                    + "'. Must be an integer, an ISO duration, an ISO instant, or 'NONE'.");
          }
        }
      }
    }
  }
}
