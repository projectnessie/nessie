/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.model;

import static org.projectnessie.model.Validation.validateDefaultCutOffPolicy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Duration;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.Pattern;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.immutables.value.Value;

/**
 * Configuration for Nessie Garbage Collector.
 *
 * <p>Some parameters for Nessie GC can and should be consistent across multiple invocations of the
 * GC tool. Those parameters can be configured via this configuration object, persisted in Nessie,
 * accessible via the Nessie API. Note that the Nessie GC tool shall default to the values from this
 * configuration, but overriding these values on the command line will still be possible.
 */
@Schema(type = SchemaType.OBJECT, title = "Garbage collector config object")
@Tag(name = "v2")
@Value.Immutable
@JsonSerialize(as = ImmutableGarbageCollectorConfig.class)
@JsonDeserialize(as = ImmutableGarbageCollectorConfig.class)
@JsonTypeName("GARBAGE_COLLECTOR")
public interface GarbageCollectorConfig extends RepositoryConfig {

  static ImmutableGarbageCollectorConfig.Builder builder() {
    return ImmutableGarbageCollectorConfig.builder();
  }

  @Schema(
      description =
          "The default cutoff policy."
              + "\n"
              + "Policies can be one of: "
              + "- number of commits as an integer value "
              + "- a duration (see java.time.Duration) "
              + "- an ISO instant "
              + "- 'NONE', means everything's considered as live")
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Pattern(
      regexp = Validation.DEFAULT_CUT_OFF_POLICY_REGEX,
      message = Validation.DEFAULT_CUT_OFF_POLICY_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = Validation.DEFAULT_CUT_OFF_POLICY_REGEX,
      message = Validation.DEFAULT_CUT_OFF_POLICY_MESSAGE)
  String getDefaultCutoffPolicy();

  /**
   * Validation rule using {@link
   * org.projectnessie.model.Validation#validateDefaultCutOffPolicy(String)}.
   */
  @Value.Check
  default void checkDefaultPolicy() {
    String defaultCutoffPolicy = getDefaultCutoffPolicy();
    if (defaultCutoffPolicy != null) {
      validateDefaultCutOffPolicy(defaultCutoffPolicy);
    }
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<ReferenceCutoffPolicy> getPerRefCutoffPolicies();

  @Schema(
      title = "Grace period for files created concurrent to GC runs.",
      description =
          "Files that have been created after 'gc-start-time - new-files-grace-period' are not being deleted.")
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonSerialize(using = Util.DurationSerializer.class)
  @JsonDeserialize(using = Util.DurationDeserializer.class)
  Duration getNewFilesGracePeriod();

  @Schema(title = "The total number of expected live files for a single content.")
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer getExpectedFileCountPerContent();

  @Override
  default Type getType() {
    return Type.GARBAGE_COLLECTOR;
  }

  @Schema(
      type = SchemaType.OBJECT,
      title = "References cutoff policy",
      description =
          "Cutoff policies per reference names. Supplied as a ref-name-pattern=policy tuple. "
              + "Reference name patterns are regular expressions.")
  @Value.Immutable
  @JsonSerialize(as = ImmutableReferenceCutoffPolicy.class)
  @JsonDeserialize(as = ImmutableReferenceCutoffPolicy.class)
  interface ReferenceCutoffPolicy {
    static ReferenceCutoffPolicy referenceCutoffPolicy(String referenceNamePattern, String policy) {
      return ImmutableReferenceCutoffPolicy.of(referenceNamePattern, policy);
    }

    static ImmutableReferenceCutoffPolicy.Builder builder() {
      return ImmutableReferenceCutoffPolicy.builder();
    }

    @Schema(description = "Reference name patterns as a regular expressions.")
    @Value.Parameter(order = 1)
    String getReferenceNamePattern();

    @Schema(
        description =
            "Policies can be one of: "
                + "- number of commits as an integer value "
                + "- a duration (see java.time.Duration) "
                + "- an ISO instant "
                + "- 'NONE', means everything's considered as live")
    @Value.Parameter(order = 2)
    String getPolicy();
  }
}
