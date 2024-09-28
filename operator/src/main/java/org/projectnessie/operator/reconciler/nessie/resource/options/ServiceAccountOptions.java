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
public record ServiceAccountOptions(
    @JsonPropertyDescription("Specifies whether a service account should be created.")
        @Default("false")
        Boolean create,
    @JsonPropertyDescription(
            """
            The name of the service account to use. \
            If not set and create is true, the account will be named after the resource's name; \
            if not set and create is false, the account will be 'default'.""")
        @Nullable
        @jakarta.annotation.Nullable
        String name,
    @JsonPropertyDescription(
            "Annotations to add to the service account. Only relevant if create is true, ignored otherwise.")
        @Default("{}")
        Map<String, String> annotations) {

  public ServiceAccountOptions() {
    this(null, null, null);
  }

  public ServiceAccountOptions {
    create = create != null ? create : false;
    annotations = annotations != null ? Map.copyOf(annotations) : Map.of();
  }
}
