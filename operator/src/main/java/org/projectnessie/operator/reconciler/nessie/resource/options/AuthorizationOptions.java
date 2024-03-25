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

import static org.projectnessie.operator.events.EventReason.InvalidAuthorizationConfig;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import io.fabric8.generator.annotation.Default;
import io.sundr.builder.annotations.Buildable;
import java.util.Map;
import org.projectnessie.operator.exception.InvalidSpecException;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record AuthorizationOptions(
    @JsonPropertyDescription(
            "Specifies whether authorization for the Nessie server should be enabled.")
        @Default("false")
        @PrinterColumn(name = "AuthZ", priority = 1)
        Boolean enabled,
    @JsonPropertyDescription(
            """
            The authorization rules when authorization.enabled=true. \
            Example rules can be found at \
            https://projectnessie.org/features/metadata_authorization/#authorization-rules""")
        @Default("{}")
        Map<String, String> rules) {

  public AuthorizationOptions() {
    this(null, null);
  }

  public AuthorizationOptions {
    enabled = enabled != null ? enabled : false;
    rules = rules != null ? Map.copyOf(rules) : Map.of();
  }

  public void validate() {
    if (enabled)
      if (rules().isEmpty()) {
        throw new InvalidSpecException(
            InvalidAuthorizationConfig,
            "Authorization is enabled, but no authorization rules are configured.");
      }
  }
}
