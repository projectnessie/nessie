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

  private static final char DOT = '.';
  private static final char STAR = '*';
  private static final char SLASH = '/';
  private static final char BACKSLASH = '\\';
  private static final char PERCENT = '%';
  private static final char ESCAPE_FOR_DOT = '.';
  private static final char ESCAPE_FOR_STAR = '*';
  private static final char ESCAPE_FOR_SLASH = '{';
  private static final char ESCAPE_FOR_BACKSLASH = '}';
  private static final char ESCAPE_FOR_PERCENT = '[';
  private static final String ESCAPE_STRING_FOR_DOT = "" + STAR + ESCAPE_FOR_DOT;
  private static final String ESCAPE_STRING_FOR_STAR = "" + STAR + ESCAPE_FOR_STAR;
  private static final String ESCAPE_STRING_FOR_SLASH = "" + STAR + ESCAPE_FOR_SLASH;
  private static final String ESCAPE_STRING_FOR_BACKSLASH = "" + STAR + ESCAPE_FOR_BACKSLASH;
  private static final String ESCAPE_STRING_FOR_PERCENT = "" + STAR + ESCAPE_FOR_PERCENT;

  private Util() {}

  public static final int FIRST_ALLOWED_KEY_CHAR = 0x20;
  public static final char ZERO_BYTE = '\u0000';
  public static final char GROUP_SEPARATOR = '\u001D';
  public static final char URL_PATH_SEPARATOR = SLASH;
  public static final String DOT_STRING = ".";
  public static final char REF_HASH_SEPARATOR = '@';

  /**
   * Convert from path encoded string to elements, see {@link
   * Elements#elementsFromPathString(String)}.
   */
  public static List<String> fromPathString(String encoded) {
    List<String> elements = new ArrayList<>();
    int l = encoded.length();
    StringBuilder e = new StringBuilder();
    for (int i = 0; i < l; i++) {
      char c = encoded.charAt(i);
      switch (c) {
        case DOT:
          if (e.length() == 0) {
            // Got a '.' at the beginning of an element. Escaped syntax.
            return fromPathStringEscaped(elements, encoded);
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

  private static List<String> fromPathStringEscaped(List<String> elements, String encoded) {
    int l = encoded.length();
    StringBuilder e = new StringBuilder();
    for (int i = 1; i < l; i++) {
      char c = encoded.charAt(i);
      switch (c) {
        case DOT:
          elements.add(e.toString());
          e.setLength(0);
          break;
        case STAR:
          i++;
          if (i == l) {
            throw new IllegalArgumentException(
                "Illegal escaping sequence at the end of encoded path: " + encoded);
          }
          c = encoded.charAt(i);
          switch (c) {
            case ESCAPE_FOR_DOT:
              e.append(DOT);
              break;
            case ESCAPE_FOR_SLASH:
              e.append(SLASH);
              break;
            case ESCAPE_FOR_BACKSLASH:
              e.append(BACKSLASH);
              break;
            case ESCAPE_FOR_PERCENT:
              e.append(PERCENT);
              break;
            case ESCAPE_FOR_STAR:
              e.append(STAR);
              break;
            default:
              throw new IllegalArgumentException(
                  "Illegal escaping sequence *" + c + " in encoded path: " + encoded);
          }
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

  /** Escapes content-key elements, see {@link Elements#toPathString()}. */
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

  /** Escapes content-key elements, see {@link Elements#toPathStringEscaped()}. */
  public static String toPathStringEscaped(List<String> elements) {
    StringBuilder sb = new StringBuilder();

    for (String element : elements) {
      if (sb.length() > 0) {
        sb.append(DOT);
      }
      int l = element.length();
      for (int i = 0; i < l; i++) {
        char c = element.charAt(i);
        switch (c) {
          case DOT:
          case SLASH:
          case BACKSLASH:
          case PERCENT:
            sb.setLength(0);
            return toPathStringEscaped(elements, sb);
          default:
            sb.append(c);
            break;
        }
      }
    }
    return sb.toString();
  }

  /** Escapes content-key elements, see {@link Elements#toCanonicalString()}. */
  public static String toCanonicalString(List<String> elements) {
    StringBuilder sb = new StringBuilder();

    for (String element : elements) {
      if (sb.length() > 0) {
        sb.append(DOT);
      }
      int l = element.length();
      for (int i = 0; i < l; i++) {
        char c = element.charAt(i);
        if (c == DOT) {
          sb.setLength(0);
          return toPathStringCanonical(elements, sb);
        } else {
          sb.append(c);
        }
      }
    }
    return sb.toString();
  }

  private static String toPathStringEscaped(List<String> elements, StringBuilder sb) {
    for (String element : elements) {
      sb.append(DOT);
      int l = element.length();
      for (int i = 0; i < l; i++) {
        char c = element.charAt(i);
        switch (c) {
          case DOT:
            sb.append(ESCAPE_STRING_FOR_DOT);
            break;
          case SLASH:
            sb.append(ESCAPE_STRING_FOR_SLASH);
            break;
          case BACKSLASH:
            sb.append(ESCAPE_STRING_FOR_BACKSLASH);
            break;
          case PERCENT:
            sb.append(ESCAPE_STRING_FOR_PERCENT);
            break;
          case STAR:
            sb.append(ESCAPE_STRING_FOR_STAR);
            break;
          default:
            sb.append(c);
            break;
        }
      }
    }

    return sb.toString();
  }

  private static String toPathStringCanonical(List<String> elements, StringBuilder sb) {
    for (String element : elements) {
      sb.append(DOT);
      int l = element.length();
      for (int i = 0; i < l; i++) {
        char c = element.charAt(i);
        switch (c) {
          case DOT:
            sb.append(ESCAPE_STRING_FOR_DOT);
            break;
          case STAR:
            sb.append(ESCAPE_STRING_FOR_STAR);
            break;
          default:
            sb.append(c);
            break;
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
