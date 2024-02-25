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

import static org.projectnessie.operator.events.EventReason.InvalidIngressConfig;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Nullable;
import io.fabric8.generator.annotation.Required;
import io.sundr.builder.annotations.Buildable;
import java.util.List;
import java.util.Map;
import org.projectnessie.operator.exception.InvalidSpecException;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record IngressOptions(
    @JsonPropertyDescription(
            "Specifies whether an ingress should be created. The default is false.")
        @Default("false")
        Boolean enabled,
    @JsonPropertyDescription(
            """
            The ingress class name to use. If not specified, the default class name is used.""")
        @Nullable
        @jakarta.annotation.Nullable
        String ingressClassName,
    @JsonPropertyDescription("Annotations to add to the ingress.") //
        @Default("{}")
        Map<String, String> annotations,
    @JsonPropertyDescription(
            "A list of rules used configure the ingress. Required if ingress is enabled.")
        @Default("[]")
        List<Rule> rules,
    @JsonPropertyDescription(
            """
            A list of TLS certificates; each entry has a list of hosts in the certificate, \
            along with the secret name used to terminate TLS traffic on port 443.""")
        @Default("[]")
        List<Tls> tls) {

  public record Rule(@Required String host, @Required List<String> paths) {}

  public record Tls(@Required List<String> hosts, @Required String secret) {}

  public IngressOptions() {
    this(null, null, null, null, null);
  }

  public IngressOptions {
    enabled = enabled != null ? enabled : false;
    annotations = annotations != null ? Map.copyOf(annotations) : Map.of();
    rules = rules != null ? List.copyOf(rules) : List.of();
    tls = tls != null ? List.copyOf(tls) : List.of();
  }

  public void validate() {
    if (enabled) {
      if (rules().isEmpty()) {
        throw new InvalidSpecException(
            InvalidIngressConfig, "At least one Ingress 'rule' must be defined.");
      }
    }
  }
}
