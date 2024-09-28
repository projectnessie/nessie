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
package org.projectnessie.operator.reconciler.nessiegc.resource.options;

import static org.projectnessie.operator.events.EventReason.InvalidGcConfig;

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
public record DatasourceOptions(
    @JsonPropertyDescription("The type of datasource to use.")
        @Default("InMemory")
        @PrinterColumn(name = "Datasource")
        DatasourceType type,
    @JsonPropertyDescription(
            "JDBC options. Only required when using Jdbc datasource type; must be null otherwise.")
        @Nullable
        @jakarta.annotation.Nullable
        JdbcOptions jdbc) {

  public enum DatasourceType {
    InMemory,
    Jdbc;
  }

  public DatasourceOptions() {
    this(null, null);
  }

  public DatasourceOptions {
    type = type != null ? type : DatasourceType.InMemory;
  }

  public void validate() {
    if (type == DatasourceType.Jdbc && jdbc == null) {
      throw new InvalidSpecException(
          InvalidGcConfig,
          "Datasource type is '%s', but spec.datasource.jdbc is not configured.".formatted(type));
    }
    if (type == DatasourceType.InMemory && jdbc != null) {
      throw new InvalidSpecException(
          InvalidGcConfig,
          "Datasource type is '%s', but spec.datasource.jdbc is configured.".formatted(type));
    }
  }
}
