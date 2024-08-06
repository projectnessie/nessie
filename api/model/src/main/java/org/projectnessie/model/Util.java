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
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.projectnessie.model.types.ContentTypes;
import org.projectnessie.model.types.RepositoryConfigTypes;

final class Util {

  private Util() {}

  public static final int FIRST_ALLOWED_KEY_CHAR = 0x20;
  public static final char ZERO_BYTE = '\u0000';
  public static final char DOT = '.';
  public static final char GROUP_SEPARATOR = '\u001D';
  public static final char URL_PATH_SEPARATOR = '/';
  public static final String DOT_STRING = ".";
  public static final char REF_HASH_SEPARATOR = '@';

  /**
   * Convert from path encoded string to normal string, supports all Nessie Spec versions.
   *
   * <p>The {@code encoded} parameter is split at dot ({@code .}) characters.
   *
   * <p>Legacy compatibility: a dot ({@code .}) character can be represented using ASCII 31 (0x1F)
   * or ASCII 0 (0x00).
   *
   * <p>Escaping, for compatibility w/ <a
   * href="https://jakarta.ee/specifications/servlet/6.0/jakarta-servlet-spec-6.0.html#uri-path-canonicalization">Jakarta
   * Servlet Specification 6, URI Path Canonicalization</a>. This is available with Nessie Spec
   * version 2.2.0, as advertised via {@link NessieConfiguration#getSpecVersion()}.
   *
   * <ul>
   *   <li>Fact: two consecutive dots ({@code ..}) would represent an <em>empty</em> namespace
   *       elements, which is illegal. It is correct to assume that this character sequence does not
   *       occur in encoded content keys.
   *   <li>The sequence {@code ..~} is the encoded representation for a single dot character {@code
   *       .}.
   *   <li>The sequence {@code ..} followed by character {@code != '~'} means that the first dot
   *       represents an element boundary, decoding should continue after the first {@code .}
   *       character.
   * </ul>
   *
   * @param encoded Path encoded string
   * @return Actual key.
   */
  public static List<String> fromPathString(String encoded) {
    List<String> elements = new ArrayList<>();
    int l = encoded.length();
    StringBuilder e = new StringBuilder();
    for (int i = 0; i < l; i++) {
      char c = encoded.charAt(i);
      switch (c) {
        case DOT:
          if (encoded.charAt(i + 1) == DOT) {
            // '..' sequence - empty elements are not allowed in content-keys
            char ctl = encoded.charAt(i + 2);
            if (ctl == '~') {
              i += 2;
              // '..~' sequence -> single dot
              e.append(DOT);
              break;
            } else {
              elements.add(e.toString());
              e.setLength(0);
            }
          } else {
            elements.add(e.toString());
            e.setLength(0);
          }
          break;
        case GROUP_SEPARATOR:
        case ZERO_BYTE:
          e.append(DOT);
          break;
        default:
          e.append(c);
          break;
      }
    }
    if (e.length() > 0) {
      elements.add(e.toString());
    }
    return elements;
  }

  /**
   * Convert these elements to a URL encoded path string.
   *
   * @return String encoded for path use.
   */
  public static String toPathString(List<String> elements) {
    StringBuilder sb = new StringBuilder();
    for (String element : elements) {
      if (sb.length() > 0) {
        sb.append('.');
      }
      int l = element.length();
      for (int i = 0; i < l; i++) {
        char c = element.charAt(i);
        sb.append(c == DOT || c == ZERO_BYTE ? GROUP_SEPARATOR : c);
      }
    }
    return sb.toString();
  }

  public static String toPathStringEscaped(List<String> elements) {
    StringBuilder sb = new StringBuilder();
    for (String element : elements) {
      if (sb.length() > 0) {
        sb.append('.');
      }
      int l = element.length();
      for (int i = 0; i < l; i++) {
        char c = element.charAt(i);
        if (c == DOT) {
          sb.append("..~");
        } else {
          sb.append(c);
        }
      }
    }
    return sb.toString();
  }

  public static String toPathStringRef(String name, String hash) {
    StringBuilder builder = new StringBuilder();
    boolean separatorRequired = hash != null && !hash.isEmpty() && isHexChar(hash.charAt(0));
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

  private static boolean isHexChar(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
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

  static final class RepositoryConfigTypeDeserializer
      extends JsonDeserializer<RepositoryConfig.Type> {
    @Override
    public RepositoryConfig.Type deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      String name = p.readValueAs(String.class);
      return name != null ? RepositoryConfigTypes.forName(name) : null;
    }
  }

  static final class RepositoryConfigTypeSerializer extends JsonSerializer<RepositoryConfig.Type> {
    @Override
    public void serialize(
        RepositoryConfig.Type value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      if (value == null) {
        gen.writeNull();
      } else {
        gen.writeString(value.name());
      }
    }
  }

  static class DurationSerializer extends StdSerializer<Duration> {
    public DurationSerializer() {
      this(Duration.class);
    }

    protected DurationSerializer(Class<Duration> t) {
      super(t);
    }

    @Override
    public void serialize(Duration value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeString(value.toString());
    }
  }

  static class DurationDeserializer extends StdDeserializer<Duration> {
    public DurationDeserializer() {
      this(null);
    }

    protected DurationDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public Duration deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return Duration.parse(p.getText());
    }
  }
}
