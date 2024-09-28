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

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Nullable;
import io.fabric8.generator.annotation.Pattern;
import io.sundr.builder.annotations.Buildable;
import java.util.Map;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
public record MonitoringOptions(
    @JsonPropertyDescription(
            """
            Specifies whether to enable monitoring with Prometheus. \
            If enabled, then a ServiceMonitor will be created. \
            The default is true if Prometheus monitoring is available in the cluster, false otherwise.""")
        @Default("true")
        Boolean enabled,
    @JsonPropertyDescription(
            "The scrape interval; if not specified, Prometheus' global scrape interval is used. Must be a valid duration, e.g. 1d, 1h30m, 5m, 10s.")
        @Pattern(
            "^(0|(([0-9]+)y)?(([0-9]+)w)?(([0-9]+)d)?(([0-9]+)h)?(([0-9]+)m)?(([0-9]+)s)?(([0-9]+)ms)?)$")
        @Nullable
        @jakarta.annotation.Nullable
        String interval,
    @JsonPropertyDescription(
            "Labels for the created ServiceMonitor so that Prometheus operator can properly pick it up.")
        @Default("{}")
        Map<String, String> labels) {

  public MonitoringOptions() {
    this(true, null, null);
  }

  public MonitoringOptions {
    enabled = enabled != null ? enabled : true;
    labels = labels != null ? Map.copyOf(labels) : Map.of();
  }
}
