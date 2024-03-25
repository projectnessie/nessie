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

import static org.projectnessie.operator.events.EventReason.InvalidTelemetryConfig;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Nullable;
import io.sundr.builder.annotations.Buildable;
import java.util.Map;
import org.projectnessie.operator.exception.InvalidSpecException;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record TelemetryOptions(
    @JsonPropertyDescription("Specifies whether tracing for the nessie server should be enabled.")
        @Default("false")
        @PrinterColumn(name = "Telemetry", priority = 1)
        Boolean enabled,
    @JsonPropertyDescription(
            "The collector endpoint URL to connect to. Required if telemetry is enabled.")
        @Nullable
        @jakarta.annotation.Nullable
        String endpoint,
    @JsonPropertyDescription(
            """
            Which requests should be sampled. Valid values are: "all", "none", or a ratio between 0.0 and \
            "1.0d" (inclusive). E.g. "0.5d" means that 50% of the requests will be sampled.""")
        @Default("1.0d")
        String sample,
    @JsonPropertyDescription(
            """
            Resource attributes to identify the nessie service among other tracing sources. \
            See https://opentelemetry.io/docs/reference/specification/resource/semantic_conventions/#service. \
            If left empty, traces will be attached to a service named after the Nessie CRD name; \
            to change this, provide a service.name attribute here.""")
        @Default("{}")
        Map<String, String> attributes) {

  public TelemetryOptions() {
    this(null, null, null, null);
  }

  public TelemetryOptions {
    enabled = enabled != null ? enabled : false;
    sample = sample != null ? sample : "1.0d";
    attributes = attributes != null ? Map.copyOf(attributes) : Map.of();
  }

  public void validate() {
    if (enabled)
      if (endpoint == null) {
        throw new InvalidSpecException(
            InvalidTelemetryConfig,
            "Telemetry is enabled, but no telemetry endpoint is configured.");
      }
  }
}
