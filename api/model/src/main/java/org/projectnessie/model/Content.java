/*
 * Copyright (C) 2020 Dremio
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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.DiscriminatorMapping;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;
import org.projectnessie.model.types.ContentTypeIdResolver;
import org.projectnessie.model.types.ContentTypes;

/** Base class for an object stored within Nessie. */
@Schema(
    type = SchemaType.OBJECT,
    title = "Content",
    anyOf = {
      IcebergTable.class,
      DeltaLakeTable.class,
      IcebergView.class,
      Namespace.class,
      UDF.class,
      Map.class
    },
    discriminatorMapping = {
      @DiscriminatorMapping(value = "ICEBERG_TABLE", schema = IcebergTable.class),
      @DiscriminatorMapping(value = "DELTA_LAKE_TABLE", schema = DeltaLakeTable.class),
      @DiscriminatorMapping(value = "ICEBERG_VIEW", schema = IcebergView.class),
      @DiscriminatorMapping(value = "NAMESPACE", schema = Namespace.class),
      @DiscriminatorMapping(value = "UDF", schema = UDF.class)
    },
    discriminatorProperty = "type")
@JsonTypeIdResolver(ContentTypeIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "type", visible = true)
public abstract class Content {

  @JsonDeserialize(using = Util.ContentTypeDeserializer.class)
  @JsonSerialize(using = Util.ContentTypeSerializer.class)
  @Schema(
      type = SchemaType.STRING,
      description =
          "Declares the type of a Nessie content object, which is currently one of "
              + "ICEBERG_TABLE, DELTA_LAKE_TABLE, ICEBERG_VIEW, NAMESPACE or UDF, which are the "
              + "discriminator mapping values of the 'Content' type.")
  public interface Type {
    Content.Type UNKNOWN = ContentTypes.forName("UNKNOWN");
    Content.Type ICEBERG_TABLE = ContentTypes.forName("ICEBERG_TABLE");
    Content.Type DELTA_LAKE_TABLE = ContentTypes.forName("DELTA_LAKE_TABLE");
    Content.Type ICEBERG_VIEW = ContentTypes.forName("ICEBERG_VIEW");
    Content.Type NAMESPACE = ContentTypes.forName("NAMESPACE");
    Content.Type UDF = ContentTypes.forName("UDF");

    /** The name of the content-type. */
    String name();

    Class<? extends Content> type();
  }

  /**
   * Unique id for this object.
   *
   * <p>This id is unique for the entire lifetime of this Content object and persists across
   * renames. Two content objects with the same key will have different id.
   */
  @Nullable
  @jakarta.annotation.Nullable
  public abstract String getId();

  /**
   * Returns the {@link Type} value for this content object.
   *
   * <p>The name of the returned value should match the JSON type name used for serializing the
   * content object.
   */
  @Value.Redacted
  @JsonIgnore
  public abstract Type getType();

  public abstract Content withId(String id);

  /**
   * Unwrap object if possible, otherwise throw.
   *
   * @param <T> Type to wrap to.
   * @param clazz Class we're trying to return.
   * @return The return value
   */
  public <T> Optional<T> unwrap(Class<T> clazz) {
    Objects.requireNonNull(clazz, "class argument");
    if (clazz.isAssignableFrom(getClass())) {
      return Optional.of(clazz.cast(this));
    }
    return Optional.empty();
  }
}
