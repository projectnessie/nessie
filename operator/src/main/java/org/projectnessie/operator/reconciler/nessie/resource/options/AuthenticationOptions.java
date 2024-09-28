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

import static org.projectnessie.operator.events.EventReason.InvalidAuthenticationConfig;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.crd.generator.annotation.PrinterColumn;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Nullable;
import io.sundr.builder.annotations.Buildable;
import org.projectnessie.operator.exception.InvalidSpecException;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record AuthenticationOptions(
    @JsonPropertyDescription(
            "Specifies whether authentication for the nessie server should be enabled.")
        @PrinterColumn(name = "AuthN", priority = 1)
        Boolean enabled,
    @JsonPropertyDescription(
            "Sets the base URL of the OpenID Connect (OIDC) server. Required if authentication is enabled.")
        @Nullable
        @jakarta.annotation.Nullable
        String oidcAuthServerUrl,
    @JsonPropertyDescription(
            "OIDC client ID to use when authentication is enabled, in order to identify the application.")
        @Default("nessie")
        String oidcClientId,
    @JsonPropertyDescription(
            """
            OIDC client secret to use when authentication is enabled. Whether the client secret \
            is required depends on the OIDC server configuration. If tokens can be introspected locally, this is usually not required. \
            If token introspection requires a round-trip to the OIDC server, then the client secret is required.
            """)
        @Nullable
        @jakarta.annotation.Nullable
        SecretValue oidcClientSecret) {

  public AuthenticationOptions() {
    this(null, null, null, null);
  }

  public AuthenticationOptions {
    enabled = enabled != null ? enabled : false;
    oidcClientId = oidcClientId != null ? oidcClientId : "nessie";
  }

  public void validate() {
    if (enabled) {
      if (oidcAuthServerUrl == null) {
        throw new InvalidSpecException(
            InvalidAuthenticationConfig,
            "OIDC authentication is enabled, but no OIDC auth server URL is configured.");
      }
      if (oidcClientId == null) {
        throw new InvalidSpecException(
            InvalidAuthenticationConfig,
            "OIDC authentication is enabled, but no OIDC client ID is configured.");
      }
    }
  }
}
