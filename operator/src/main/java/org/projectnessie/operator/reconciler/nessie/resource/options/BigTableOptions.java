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
import io.fabric8.generator.annotation.Required;
import io.sundr.builder.annotations.Buildable;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record BigTableOptions(
    @JsonPropertyDescription("The Google Cloud project ID.") @Required String projectId,
    @JsonPropertyDescription("The Google Cloud BigTable instance ID.") @Default("nessie-bigtable")
        String instanceId,
    @JsonPropertyDescription("The Google Cloud BigTable app profile ID.") @Default("default")
        String appProfileId,
    @JsonPropertyDescription(
            """
            The BigTable credentials. When provided, the referenced secret key must contain a \
            valid JSON key that will be used for authentication. \
            If left empty, then no credentials will be mounted, which means that \
            Workload Identity will be used for authentication.""")
        @Nullable
        @jakarta.annotation.Nullable
        SecretValue credentials) {

  public BigTableOptions {
    instanceId = instanceId != null ? instanceId : "nessie-bigtable";
    appProfileId = appProfileId != null ? appProfileId : "default";
  }
}
