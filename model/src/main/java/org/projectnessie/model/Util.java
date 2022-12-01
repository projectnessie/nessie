/*
 * Copyright (C) 2022 Dremio
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.projectnessie.model.types.ContentTypes;

final class Util {

  private Util() {}

  public static final char ZERO_BYTE = '\u0000';
  public static final char DOT = '.';
  public static final char GROUP_SEPARATOR = '\u001D';
  public static final char REF_HASH_SEPARATOR = '@';
  public static final char URL_PATH_SEPARATOR = '/';
  public static final String DOT_STRING = ".";
  public static final String ZERO_BYTE_STRING = Character.toString(ZERO_BYTE);
  public static final String GROUP_SEPARATOR_STRING = Character.toString(GROUP_SEPARATOR);

  /**
   * Convert from path encoded string to normal string.
   *
   * @param encoded Path encoded string
   * @return Actual key.
   */
  public static List<String> fromPathString(String encoded) {
    return Arrays.stream(encoded.split("\\."))
        .map(x -> x.replace(GROUP_SEPARATOR, DOT).replace(ZERO_BYTE, DOT))
        .collect(Collectors.toList());
  }

  /**
   * Convert these elements to a URL encoded path string.
   *
   * @return String encoded for path use.
   */
  public static String toPathString(List<String> elements) {
    return elements.stream()
        .map(x -> x.replace(DOT, GROUP_SEPARATOR).replace(ZERO_BYTE, GROUP_SEPARATOR))
        .collect(Collectors.joining("."));
  }

  public static String toPathStringRef(String name, String hash) {
    StringBuilder builder = new StringBuilder();
    boolean separatorRequired = hash != null;
    if (name != null) {
      builder.append(name);
      separatorRequired |= name.indexOf(URL_PATH_SEPARATOR) >= 0;
    }

    if (separatorRequired) {
      builder.append(REF_HASH_SEPARATOR);
    }

    if (hash != null) {
      builder.append(hash);
    }

    return builder.toString();
  }

  public static Reference fromPathStringRef(
      @Nonnull String value, @Nonnull Reference.ReferenceType namedRefType) {
    String name = null;
    String hash = null;
    int hashIdx = value.indexOf(REF_HASH_SEPARATOR);

    if (hashIdx > 0) {
      name = value.substring(0, hashIdx);
    }

    if (hashIdx < 0) {
      name = value;
    }

    if (hashIdx >= 0) {
      hash = value.substring(hashIdx + 1);
      if (hash.isEmpty()) {
        hash = null;
      }
    }

    if (name == null) {
      return Detached.of(hash);
    } else {
      switch (namedRefType) {
        case TAG:
          return Tag.of(name, hash);
        case BRANCH:
          return Branch.of(name, hash);
        default:
          throw new IllegalArgumentException("Unsupported reference type: " + namedRefType);
      }
    }
  }

  static final class ContentTypeDeserializer extends JsonDeserializer<Content.Type> {
    @Override
    public Content.Type deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      String name = p.readValueAs(String.class);
      return name != null ? ContentTypes.forName(name) : null;
    }
  }

  static final class ContentTypeSerializer extends JsonSerializer<Content.Type> {
    @Override
    public void serialize(Content.Type value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      if (value == null) {
        gen.writeNull();
      } else {
        gen.writeString(value.name());
      }
    }
  }
}
