/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.DiscriminatorMapping;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.immutables.value.Value;
import org.projectnessie.model.types.RepositoryConfigTypeIdResolver;
import org.projectnessie.model.types.RepositoryConfigTypes;

/** Base interface for all configurations that shall be consistent for a Nessie repository. */
@Schema(
    type = SchemaType.OBJECT,
    title = "RepositoryConfig",
    anyOf = {GarbageCollectorConfig.class},
    discriminatorMapping = {
      @DiscriminatorMapping(value = "GARBAGE_COLLECTOR", schema = GarbageCollectorConfig.class)
    },
    discriminatorProperty = "type")
@Tag(name = "v2")
@JsonTypeIdResolver(RepositoryConfigTypeIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "type", visible = true)
public interface RepositoryConfig {

  /**
   * Returns the {@link Type} value for this repository config object.
   *
   * <p>The name of the returned value should match the JSON type name used for serializing the
   * repository config object.
   */
  @Value.Redacted
  @JsonIgnore
  Type getType();

  @JsonDeserialize(using = Util.RepositoryConfigTypeDeserializer.class)
  @JsonSerialize(using = Util.RepositoryConfigTypeSerializer.class)
  @Schema(
      type = SchemaType.STRING,
      name = "RepositoryConfigType",
      description =
          "Declares the type of a Nessie repository config object, which is currently only "
              + "GARBAGE_COLLECTOR, which is the discriminator mapping value of the 'RepositoryConfig' type.")
  @Tag(name = "v2")
  interface Type {
    Type UNKNOWN = RepositoryConfigTypes.forName("UNKNOWN");
    Type GARBAGE_COLLECTOR = RepositoryConfigTypes.forName("GARBAGE_COLLECTOR");

    /** The name of the repository config type. */
    String name();

    Class<? extends RepositoryConfig> type();
  }
}
