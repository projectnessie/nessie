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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Min;
import io.fabric8.generator.annotation.Nullable;
import io.sundr.builder.annotations.Buildable;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record ProbeOptions(
    @JsonPropertyDescription(
            """
            Number of seconds after the container has started before probes are initiated. \
            Defaults to 0 seconds. Minimum value is 0.
            """)
        @Nullable
        @jakarta.annotation.Nullable
        @Min(0)
        Integer initialDelaySeconds,
    @JsonPropertyDescription(
            """
            How often (in seconds) to perform the probe. Defaults to 10 seconds. Minimum value is 1.
            """)
        @Nullable
        @jakarta.annotation.Nullable
        @Min(1)
        Integer periodSeconds,
    @JsonPropertyDescription(
            """
            Number of seconds after which the probe times out. Defaults to 1 second. Minimum value is 1.
            """)
        @Nullable
        @jakarta.annotation.Nullable
        @Min(1)
        Integer timeoutSeconds,
    @JsonPropertyDescription(
            """
            Minimum consecutive successes for the probe to be considered successful after having failed. \
            Defaults to 1. Minimum value is 1.
            """)
        @Nullable
        @jakarta.annotation.Nullable
        @Min(1)
        Integer successThreshold,
    @JsonPropertyDescription(
            """
            After a probe fails failureThreshold times in a row, Kubernetes considers that the overall check has failed: \
            the container is not healthy.
            """)
        @Nullable
        @jakarta.annotation.Nullable
        @Min(1)
        Integer failureThreshold) {

  public static final ProbeOptions DEFAULT_LIVENESS_PROBE_OPTIONS =
      new ProbeOptions(2, 30, 10, 1, 3);

  public static final ProbeOptions DEFAULT_READINESS_PROBE_OPTIONS =
      new ProbeOptions(3, 45, 10, 1, 3);
}
