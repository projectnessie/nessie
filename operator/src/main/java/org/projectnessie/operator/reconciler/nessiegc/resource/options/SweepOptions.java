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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Min;
import io.sundr.builder.annotations.Buildable;
import org.projectnessie.operator.events.EventReason;
import org.projectnessie.operator.exception.InvalidSpecException;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record SweepOptions(
    @JsonPropertyDescription("Number of contents that are checked in parallel.")
        @Default("4")
        @Min(1)
        Integer parallelism,
    @JsonPropertyDescription(
            "Identified unused/orphan files are by default immediately deleted. "
                + "Using deferred deletion stores the files to be deleted, so they can be inspected and deleted later.")
        @Default("false")
        Boolean deferDeletes) {

  public SweepOptions() {
    this(null, null);
  }

  public SweepOptions {
    parallelism = parallelism != null ? parallelism : 4;
  }

  public void validate() {
    if (parallelism < 1) {
      throw new InvalidSpecException(EventReason.InvalidGcConfig, "parallelism must be at least 1");
    }
  }
}
