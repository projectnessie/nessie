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
import io.fabric8.generator.annotation.Nullable;
import io.fabric8.generator.annotation.Required;
import io.sundr.builder.annotations.Buildable;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record MongoDbOptions(
    @JsonPropertyDescription("The MongoDB connection string (required).") //
        @Required
        String connectionString,
    @JsonPropertyDescription("The MongoDB database name (required).") //
        @Required
        String database,
    @JsonPropertyDescription("The MongoDB username (optional).")
        @Nullable
        @jakarta.annotation.Nullable
        String username,
    @JsonPropertyDescription("The MongoDB password (optional).")
        @Nullable
        @jakarta.annotation.Nullable
        SecretValue password) {}
