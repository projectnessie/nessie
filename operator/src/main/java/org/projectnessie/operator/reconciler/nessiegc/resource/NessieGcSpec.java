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
package org.projectnessie.operator.reconciler.nessiegc.resource;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Nullable;
import io.fabric8.generator.annotation.Required;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.sundr.builder.annotations.Buildable;
import org.projectnessie.operator.events.EventReason;
import org.projectnessie.operator.exception.InvalidSpecException;
import org.projectnessie.operator.reconciler.nessie.resource.options.WorkloadOptions;
import org.projectnessie.operator.reconciler.nessiegc.resource.options.DatasourceOptions;
import org.projectnessie.operator.reconciler.nessiegc.resource.options.DatasourceOptions.DatasourceType;
import org.projectnessie.operator.reconciler.nessiegc.resource.options.IcebergOptions;
import org.projectnessie.operator.reconciler.nessiegc.resource.options.MarkOptions;
import org.projectnessie.operator.reconciler.nessiegc.resource.options.ScheduleOptions;
import org.projectnessie.operator.reconciler.nessiegc.resource.options.SweepOptions;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record NessieGcSpec(
    @JsonPropertyDescription("The target Nessie resource.") //
        @Required
        LocalObjectReference nessieRef,
    @JsonPropertyDescription(
            "The options for the GC job schedule. If not specified, then the GC job will run only once.")
        @Nullable
        @jakarta.annotation.Nullable
        ScheduleOptions schedule,
    @JsonPropertyDescription("The options for the mark phase.") //
        @Default("{}")
        MarkOptions mark,
    @JsonPropertyDescription("The options for the sweep phase.") //
        @Default("{}")
        SweepOptions sweep,
    @JsonPropertyDescription("The options for the GC job datasource.") //
        @Default("{}")
        DatasourceOptions datasource,
    @JsonPropertyDescription("Iceberg options.") //
        @Default("{}")
        IcebergOptions iceberg,
    @JsonPropertyDescription(
            """
            Options for the Nessie GC jobs and cron jobs (service account, container image, \
            security context, etc.).""")
        @Default("{}")
        WorkloadOptions job) {

  public NessieGcSpec() {
    this(null, null, null, null, null, null, null);
  }

  public NessieGcSpec {
    mark = mark != null ? mark : new MarkOptions();
    sweep = sweep != null ? sweep : new SweepOptions();
    datasource = datasource != null ? datasource : new DatasourceOptions();
    iceberg = iceberg != null ? iceberg : new IcebergOptions();
    job = job != null ? job : new WorkloadOptions();
  }

  public void validate() {
    mark.validate();
    sweep.validate();
    datasource.validate();
    if (sweep.deferDeletes() && datasource().type() == DatasourceType.InMemory) {
      throw new InvalidSpecException(
          EventReason.InvalidGcConfig,
          "In-memory datasource not allowed when deferred deletes are enabled");
    }
    // TODO better done with a validating webhook:
    // - forbid changes to the Job PodTemplateSpec
    // - forbid switches to/from cron job and job
  }
}
