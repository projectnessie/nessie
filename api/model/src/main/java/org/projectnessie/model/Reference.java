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

import static org.projectnessie.model.Validation.validateHash;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import java.io.IOException;
import java.util.Locale;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.DiscriminatorMapping;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.immutables.value.Value;
import org.projectnessie.model.types.ReferenceTypeIdResolver;

@Schema(
    type = SchemaType.OBJECT,
    title = "Reference",
    oneOf = {Branch.class, Tag.class, Detached.class},
    discriminatorMapping = {
      @DiscriminatorMapping(value = "TAG", schema = Tag.class),
      @DiscriminatorMapping(value = "BRANCH", schema = Branch.class),
      @DiscriminatorMapping(value = "DETACHED", schema = Detached.class)
    },
    discriminatorProperty = "type",
    // Smallrye does neither support JsonFormat nor javax.validation.constraints.Pattern :(
    properties = {
      @SchemaProperty(name = "name", pattern = Validation.REF_NAME_REGEX),
      @SchemaProperty(name = "hash", pattern = Validation.HASH_REGEX),
      @SchemaProperty(name = "metadata", nullable = true)
    })
@JsonSubTypes({
  @JsonSubTypes.Type(Branch.class),
  @JsonSubTypes.Type(Tag.class),
  @JsonSubTypes.Type(Detached.class)
})
@JsonTypeIdResolver(ReferenceTypeIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface Reference extends Base {
  /** Human-readable reference name. */
  @NotBlank
  @jakarta.validation.constraints.NotBlank
  @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = Validation.REF_NAME_REGEX,
      message = Validation.REF_NAME_MESSAGE)
  String getName();

  /** backend system id. Usually the 32-byte hash of the commit this reference points to. */
  @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = Validation.HASH_REGEX,
      message = Validation.HASH_MESSAGE)
  String getHash();

  /** Validation rule using {@link org.projectnessie.model.Validation#validateHash(String)}. */
  @Value.Check
  default void checkHash() {
    String hash = getHash();
    if (hash != null) {
      validateHash(hash);
    }
  }

  /**
   * Returns a {@link ReferenceMetadata} instance that contains additional metadata about this
   * reference. Note that this is <b>only added</b> by the server when <b>explicitly</b> requested
   * by the client.
   *
   * @return A {@link ReferenceMetadata} instance that contains additional metadata about this
   *     reference. Note that this is <b>only added</b> by the server when <b>explicitly</b>
   *     requested by the client.
   */
  @JsonInclude(Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  ReferenceMetadata getMetadata();

  @JsonIgnore
  @Value.Redacted
  @JsonDeserialize(using = ReferenceType.Deserializer.class)
  ReferenceType getType();

  default String toPathString() {
    return toPathString(getName(), getHash());
  }

  static String toPathString(String name, String hash) {
    return Util.toPathStringRef(name, hash);
  }

  /** The reference type as an enum. */
  @Schema(enumeration = {"branch", "tag"}) // Required to have lower-case values in OpenAPI
  enum ReferenceType {
    BRANCH,
    TAG;

    public static ReferenceType parse(String referenceType) {
      try {
        if (referenceType != null) {
          referenceType = referenceType.toUpperCase(Locale.ROOT);
          return "DETACHED".equals(referenceType) ? null : ReferenceType.valueOf(referenceType);
        }
        return null;
      } catch (IllegalArgumentException e) {
        return TAG;
      }
    }

    static final class Deserializer extends JsonDeserializer<ReferenceType> {
      @Override
      public ReferenceType deserialize(JsonParser p, DeserializationContext ctxt)
          throws IOException {
        String name = p.readValueAs(String.class);
        return name != null ? ReferenceType.parse(name) : null;
      }
    }
  }
}
