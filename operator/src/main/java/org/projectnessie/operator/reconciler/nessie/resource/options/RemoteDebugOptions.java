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
import io.sundr.builder.annotations.Buildable;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record RemoteDebugOptions(
    @JsonPropertyDescription("Whether to enable remote debugging.") //
        @Default("false")
        Boolean enabled,
    @JsonPropertyDescription("The port to use for remote debugging.") //
        @Default("5005")
        Integer port,
    @JsonPropertyDescription("Whether to suspend.") //
        @Default("false")
        Boolean suspend) {

  public static final int DEFAULT_DEBUG_PORT = 5005;

  public RemoteDebugOptions() {
    this(null, null, null);
  }

  public RemoteDebugOptions {
    enabled = enabled != null ? enabled : false;
    port = port != null ? port : DEFAULT_DEBUG_PORT;
    suspend = suspend != null ? suspend : false;
  }
}
