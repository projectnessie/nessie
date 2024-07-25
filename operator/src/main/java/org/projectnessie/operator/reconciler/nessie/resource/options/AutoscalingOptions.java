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
package org.projectnessie.operator.reconciler.nessie.resource.options;

import static org.projectnessie.operator.events.EventReason.InvalidAutoScalingConfig;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Min;
import io.fabric8.generator.annotation.Nullable;
import io.sundr.builder.annotations.Buildable;
import org.projectnessie.operator.exception.InvalidSpecException;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record AutoscalingOptions(
    @JsonPropertyDescription(
            """
            Specifies whether automatic horizontal scaling should be enabled. \
            Do not enable this when using InMemory or RocksDb version store type.
            """)
        @Default("false")
        Boolean enabled,
    @JsonPropertyDescription("The minimum number of replicas to maintain.") //
        @Default("1")
        @Min(1)
        Integer minReplicas,
    @JsonPropertyDescription("The maximum number of replicas to maintain.") //
        @Default("3")
        @Min(1)
        Integer maxReplicas,
    @JsonPropertyDescription(
            "The target CPU utilization percentage. Set to zero or empty to disable.")
        @Nullable
        @jakarta.annotation.Nullable
        Integer targetCpuUtilizationPercentage,
    @JsonPropertyDescription(
            "The target memory utilization percentage. Set to zero or empty to disable.")
        @Nullable
        @jakarta.annotation.Nullable
        Integer targetMemoryUtilizationPercentage) {

  public AutoscalingOptions() {
    this(null, null, null, null, null);
  }

  public AutoscalingOptions {
    enabled = enabled != null ? enabled : false;
    minReplicas = minReplicas != null ? minReplicas : 1;
    maxReplicas = maxReplicas != null ? maxReplicas : 3;
  }

  public void validate() {
    if (enabled) {
      Integer cpu = targetCpuUtilizationPercentage();
      Integer memory = targetMemoryUtilizationPercentage();
      if (isNullOrZero(cpu) && isNullOrZero(memory)) {
        throw new InvalidSpecException(
            InvalidAutoScalingConfig,
            "At least one of 'targetCpuUtilizationPercentage' or 'targetMemoryUtilizationPercentage' "
                + "must be set when autoscaling is enabled.");
      }
      if (minReplicas() > maxReplicas()) {
        throw new InvalidSpecException(
            InvalidAutoScalingConfig, "'minReplicas' must be less than or equal to 'maxReplicas'.");
      }
    }
  }

  private static boolean isNullOrZero(Integer i) {
    return i == null || i == 0;
  }
}
