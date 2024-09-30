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
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Nullable;
import io.sundr.builder.annotations.Buildable;
import java.util.Map;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record ServiceOptions(
    @JsonPropertyDescription("The type of service to create. Defaults to ClusterIP.")
        @Default("ClusterIP")
        ServiceOptions.Type type,
    @JsonPropertyDescription(
            "The port on which the service should listen. Defaults to " + DEFAULT_NESSIE_PORT + ".")
        @Default("19120")
        Integer port,
    @JsonPropertyDescription(
            """
            The node port on which the service should be exposed. \
            Only valid if the service type is NodePort or LoadBalancer, ignored otherwise. \
            If unspecified, a random node port will be assigned.""")
        @Nullable
        @jakarta.annotation.Nullable
        Integer nodePort,
    @JsonPropertyDescription("The session affinity to use for the service. Defaults to None.")
        @Default("None")
        SessionAffinity sessionAffinity,
    @JsonPropertyDescription("Additional service labels.") //
        @Default("{}")
        Map<String, String> labels,
    @JsonPropertyDescription("Additional service annotations.") //
        @Default("{}")
        Map<String, String> annotations) {

  public static final int DEFAULT_NESSIE_PORT = 19120;

  public enum Type {
    ClusterIP,
    NodePort,
    LoadBalancer
  }

  public enum SessionAffinity {
    @JsonPropertyDescription("None disables session affinity.")
    None,
    @JsonPropertyDescription("ClientIP enables session affinity based on the client's IP address.")
    ClientIP
  }

  public ServiceOptions() {
    this(null, null, null, null, null, null);
  }

  public ServiceOptions {
    type = type != null ? type : Type.ClusterIP;
    port = port != null ? port : DEFAULT_NESSIE_PORT;
    sessionAffinity = sessionAffinity != null ? sessionAffinity : SessionAffinity.None;
    labels = labels != null ? Map.copyOf(labels) : Map.of();
    annotations = annotations != null ? Map.copyOf(annotations) : Map.of();
  }
}
