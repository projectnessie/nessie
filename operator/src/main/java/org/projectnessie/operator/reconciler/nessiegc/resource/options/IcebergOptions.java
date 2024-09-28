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
import io.sundr.builder.annotations.Buildable;
import java.util.Map;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record IcebergOptions(
    @JsonPropertyDescription("Iceberg properties used to configure the FileIO.") //
        @Default("{}")
        Map<String, String> icebergProperties,
    @JsonPropertyDescription(
            "Hadoop configuration options, required when using an Iceberg FileIO that is not S3.")
        @Default("{}")
        Map<String, String> hadoopConf) {

  public IcebergOptions() {
    this(null, null);
  }

  public IcebergOptions {
    icebergProperties = icebergProperties == null ? Map.of() : icebergProperties;
    hadoopConf = hadoopConf == null ? Map.of() : hadoopConf;
  }
}
