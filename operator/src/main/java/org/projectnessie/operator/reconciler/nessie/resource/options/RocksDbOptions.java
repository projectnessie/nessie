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
import io.fabric8.kubernetes.api.model.Quantity;
import io.sundr.builder.annotations.Buildable;
import java.util.Map;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record RocksDbOptions(
    @JsonPropertyDescription(
            "The storage class name of the persistent volume claim to create. Leave unset if using dynamic provisioning.")
        @Nullable
        @jakarta.annotation.Nullable
        String storageClassName,
    @JsonPropertyDescription("The size of the persistent volume claim to create.") //
        @Default("1Gi")
        Quantity storageSize,
    @JsonPropertyDescription(
            """
            Labels to add to the persistent volume claim spec selector; \
            a persistent volume with matching labels must exist. \
            Leave empty if using dynamic provisioning.""")
        @Default("{}")
        Map<String, String> selectorLabels) {

  public RocksDbOptions {
    storageSize = storageSize != null ? storageSize : new Quantity("1Gi");
    selectorLabels = selectorLabels != null ? Map.copyOf(selectorLabels) : Map.of();
  }
}
