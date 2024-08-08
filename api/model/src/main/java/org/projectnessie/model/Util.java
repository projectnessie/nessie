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

  public static final char DOT = '.';
  public static final char SLASH = '/';
  public static final char BACKSLASH = '\\';
  public static final char PERCENT = '%';
  public static final char ESCAPE_FOR_DOT = '_';
  public static final char ESCAPE_FOR_SLASH = '{';
  public static final char ESCAPE_FOR_BACKSLASH = '}';
  public static final char ESCAPE_FOR_PERCENT = '[';
  public static final String ESCAPE_STRING_FOR_DOT = "" + DOT + ESCAPE_FOR_DOT;
  public static final String ESCAPE_STRING_FOR_SLASH = "" + DOT + ESCAPE_FOR_SLASH;
  public static final String ESCAPE_STRING_FOR_BACKSLASH = "" + DOT + ESCAPE_FOR_BACKSLASH;
  public static final String ESCAPE_STRING_FOR_PERCENT = "" + DOT + ESCAPE_FOR_PERCENT;

  private Util() {}

  public static final int FIRST_ALLOWED_KEY_CHAR = 0x20;
  public static final char ZERO_BYTE = '\u0000';
  public static final char GROUP_SEPARATOR = '\u001D';
  public static final char URL_PATH_SEPARATOR = SLASH;
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
   * Servlet Specification 6, URI Path Canonicalization</a> and <a
   * href="https://datatracker.ietf.org/doc/html/rfc3986#section-3.3">RFC 3986, section 3.3</a>.
   * This is available with Nessie Spec version 2.2.0, as advertised via {@link
   * NessieConfiguration#getSpecVersion()}.
   *
   * <ul>
   *   <li>Fact: two consecutive dots ({@code ..}) would represent an <em>empty</em> namespace
   *       elements, which is illegal. It is correct to assume that this character sequence does not
   *       occur in encoded content keys.
   *   <li>The sequence {@code ..*} is the encoded representation for a single dot character {@code
   *       .}.
   *   <li>The sequence {@code ..^} is the encoded representation for a slash {@code /}, which is a
   *       URI path separator.
   *   <li>The sequence {@code ..=} is the encoded representation for a backslash {@code \}, which
   *       is a URI path separator.
   *   <li>The sequence {@code ..=} is the encoded representation for a hash {@code #}, which is a
   *       URI path separator.
   *   <li>The sequence {@code ..} followed by any character not mentioned in the sequences above,
   *       means that the first dot represents an element boundary, decoding should continue after
   *       the first {@code .} character.
   * </ul>
   *
   * <p>This function can decode the representations returned by {@link #toPathString(List)}, {@link
   * #toPathStringEscaped(List)} and {@link #toCanonicalString(List)}.
   *
   * @param encoded Path encoded string, as returned by {@link #toPathString(List)}, {@link
   *     #toPathStringEscaped(List)} and {@link #toCanonicalString(List)}.
   * @return Actual key.
   */
  public static List<String> fromPathString(String encoded) {
    List<String> elements = new ArrayList<>();
    int l = encoded.length();
    StringBuilder e = new StringBuilder();
    boolean escaped = false;
    for (int i = 0; i < l; i++) {
      char c = encoded.charAt(i);
      switch (c) {
        case DOT:
          if (!escaped) {
            if (e.length() == 0) {
              // Got a '.' at the beginning of an element. This and all following elements are
              // escaped.
              escaped = true;
            } else {
              elements.add(e.toString());
              e.setLength(0);
            }
          } else {
            c = encoded.charAt(++i);
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
              default:
                // Any other character, that character is the first character of the _next_ element;
                --i;
                elements.add(e.toString());
                e.setLength(0);
                break;
            }
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
   * Convert these elements to a URI path/query string, which <em>may violate</em> <a
   * href="https://jakarta.ee/specifications/servlet/6.0/jakarta-servlet-spec-6.0#uri-path-canonicalization">Jakarta
   * Servlet Spec 6, chapter 3.5.2 URI Path Canonicalization</a>, because it uses so-called control
   * characters and ambiguous path elements.
   *
   * <p>Users must prefer {@link #toPathStringEscaped(List)}, if the service support Nessie spec
   * version 2.2.0 or newer.
   *
   * @param elements The content-key or namespace elements to encode.
   * @return The URI path compatible representation of the given elements, possibly escaped. The
   *     returned value should be URL-encoded before added to a URI path or query.
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

  /**
   * Escapes content-key elements into a URI path/query compatible form that does not violate <a
   * href="https://jakarta.ee/specifications/servlet/6.0/jakarta-servlet-spec-6.0#uri-path-canonicalization">Jakarta
   * Servlet Spec 6, chapter 3.5.2 URI Path Canonicalization</a> by escaping the characters {@code
   * /}, {@code \}, {@code %}, and if escaping also the {@code .}.
   *
   * <p>Some examples:
   *
   * <ul>
   *   <li>{@code ["foo", "bar", "baz"]} returned as {@code "foo.bar.baz"} - no escaping needed.
   *   <li>{@code ["foo", ".bar", "baz"]} returned as {@code "foo..._bar.baz"} - escaping needed
   *       with the 2nd element. The first dot is the element separator. The 2nd dot is the first
   *       character of the second element, indicating that escaping starts at this element. {@code
   *       ._} is the escape sequence for the {@code .} character.
   *   <li>{@code ["foo.", ".bar", "a/\%aa"]} returned as {@code ".foo._.._bar.a.{.}.[aa"}
   * </ul>
   *
   * <p>The returned value is always processable by Nessie services announcing Nessie spec 2.2.0 or
   * newer.
   *
   * @param elements The content-key or namespace elements to encode.
   * @return The URI path compatible representation of the given elements, possibly escaped. The
   *     returned value should be URL-encoded before added to a URI path.
   */
  public static String toPathStringEscaped(List<String> elements) {
    StringBuilder sb = new StringBuilder();
    boolean escaped = false;
    for (String element : elements) {
      if (sb.length() > 0) {
        sb.append('.');
      }
      escaped = escapePathElement(element, sb, escaped);
    }
    return sb.toString();
  }

  /** Escapes all characters, that are "problematic" in URI paths. */
  static boolean escapePathElement(String element, StringBuilder sb, boolean escaped) {
    int sbl = sb.length();
    int l = element.length();
    for (int i = 0; i < l; i++) {
      char c = element.charAt(i);
      switch (c) {
        case DOT:
        case SLASH:
        case BACKSLASH:
        case PERCENT:
          if (!escaped) {
            // Not yet "escaped", let the first escaped element start with a '.'
            if (sb.length() > sbl) {
              sb.insert(sbl, DOT);
            } else {
              sb.append(DOT);
            }
            escaped = true;
          }
          for (; i < l; i++) {
            c = element.charAt(i);
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
              default:
                sb.append(c);
                break;
            }
          }
          break;
        default:
          sb.append(c);
          break;
      }
    }
    return escaped;
  }

  /**
   * Escapes content-key elements into a canonical form that escapes {@code .} characters. The
   * returned format is similar to {@linkplain #toPathStringEscaped(List)}, but does not escape
   * problematic URI characters.
   *
   * <p>Some examples:
   *
   * <ul>
   *   <li>{@code ["foo", "bar", "baz"]} returned as {@code "foo.bar.baz"} - no escaping needed.
   *   <li>{@code ["foo", ".bar", "baz"]} returned as {@code "foo..._bar.baz"} - escaping needed
   *       with the 2nd element. The first dot is the element separator. The 2nd dot is the first
   *       character of the second element, indicating that escaping starts at this element. {@code
   *       ._} is the escape sequence for the {@code .} character.
   *   <li>{@code ["foo.", ".bar", "a/\%aa"]} returned as {@code ".foo._.._bar.a/\%aa"}
   *   <li>{@code ["foo", "bar", "a/\%aa"]} returned as {@code "foo.bar..a/\%aa"}
   * </ul>
   *
   * @param elements The content-key or namespace elements to encode.
   * @return The canonical representation of the given elements, possibly escaped. The returned
   *     value should <em>not</em> be used in a URI path.
   */
  public static String toCanonicalString(List<String> elements) {
    StringBuilder sb = new StringBuilder();
    boolean escaped = false;
    for (String element : elements) {
      if (sb.length() > 0) {
        sb.append('.');
      }
      escaped = escapeCanonicalElement(element, sb, escaped);
    }
    return sb.toString();
  }

  /**
   * Similar to {@link #escapePathElement(String, StringBuilder, boolean)}, but escapes only the
   * {@value #DOT} character.
   */
  static boolean escapeCanonicalElement(String element, StringBuilder sb, boolean escaped) {
    int sbl = sb.length();
    int l = element.length();
    for (int i = 0; i < l; i++) {
      char c = element.charAt(i);
      if (c == DOT) {
        if (!escaped) {
          // Not yet "escaped", let the first escaped element start with a '.'
          if (sb.length() > sbl) {
            sb.insert(sbl, DOT);
          } else {
            sb.append(DOT);
          }
          escaped = true;
        }
        for (; i < l; i++) {
          c = element.charAt(i);
          if (c == DOT) {
            sb.append(ESCAPE_STRING_FOR_DOT);
          } else {
            sb.append(c);
          }
        }
      } else {
        sb.append(c);
      }
    }
    return escaped;
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
