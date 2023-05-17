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
package org.projectnessie.versioned.storage.common.indexes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Character.isSurrogate;
import static java.lang.Character.isSurrogatePair;
import static java.lang.Character.toCodePoint;

import com.google.common.annotations.VisibleForTesting;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Represents a key in a {@link StoreIndex}, optimized for performance (CPU + heap).
 *
 * <p>Note that this key does not map exactly to a (user facing) <em>Nessie content keys</em>. Keys
 * at the storage level are finer grained.
 */
public final class StoreKey implements Comparable<StoreKey> {

  /** Maximum number of characters in a key. Note: characters can take up to 3 bytes via UTF-8. */
  public static final int MAX_LENGTH = 500;

  /** Contains the {@code char 0} separated key representation. */
  private final String key;

  private StoreKey(String key) {
    this.key = key;
  }

  public String rawString() {
    return key;
  }

  @Override
  public int compareTo(StoreKey that) {
    return key.compareTo(that.key);
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof StoreKey)) {
      return false;
    }

    StoreKey that = (StoreKey) obj;
    return this.key.equals(that.key);
  }

  public static StoreKey keyFromString(String rawString) {
    return new StoreKey(rawString);
  }

  public static StoreKey key(String... elements) {
    for (String element : elements) {
      checkElement(element);
    }
    return new StoreKey(String.join("\0", elements));
  }

  public static StoreKey key(List<String> elements) {
    elements.forEach(StoreKey::checkElement);
    return new StoreKey(String.join("\0", elements));
  }

  private static void checkElement(String element) {
    checkArgument(element != null && !element.isEmpty(), "Empty key elements are not allowed");
  }

  public ByteBuffer serialize(ByteBuffer keySerializationBuffer) {
    keySerializationBuffer.clear();
    try {
      putString(keySerializationBuffer, key);
      keySerializationBuffer.put((byte) 0);
      keySerializationBuffer.put((byte) 0);
    } catch (BufferOverflowException e) {
      throw new IllegalArgumentException("Serialized key too big");
    }

    keySerializationBuffer.flip();
    return keySerializationBuffer;
  }

  /**
   * Custom implementation, less GC alloc and churn.
   *
   * <p>Uses the "modified" UTF-8 encoding, differences:
   *
   * <ul>
   *   <li>Serializes {@code char 0} as a single byte ({@code 0x00}.
   *   <li>Supports surrogates / code points.
   *   <li>Non-surrogates are take 1-3 bytes.
   *   <li>Surrogates take 4 bytes (from 2 {@code char}s)
   * </ul>
   */
  @VisibleForTesting
  static void putString(ByteBuffer buffer, String s) {
    int l = s.length();
    int i = 0;

    // "fast" loop
    for (; i < l; i++) {
      char c = s.charAt(i);
      // NOTE: The "modified UTF-8" encoding does _NOT_ encode 'ASCII 0' in a single byte!
      if (c <= 0x7f) {
        buffer.put((byte) c);
      } else {
        break;
      }
    }

    // "slow" loop
    for (; i < l; i++) {
      char c = s.charAt(i);
      // NOTE: The "modified UTF-8" encoding does _NOT_ encode 'ASCII 0' in a single byte!
      if (c <= 0x7f) {
        buffer.put((byte) c);
      } else if (c <= 0x07ff) {
        buffer.put((byte) (0xc0 | (0x1f & c >> 6)));
        buffer.put((byte) (0x80 | (0x3f & c)));
      } else if (isSurrogate(c)) {
        char c2;
        int codePoint =
            i < l - 1 && isSurrogatePair(c, c2 = s.charAt(i + 1)) ? toCodePoint(c, c2) : -1;
        if (codePoint < 0) {
          throw new IllegalArgumentException(
              "Unmappable surrogate character (0x"
                  + Integer.toString(c & 0xffff, 16)
                  + " 0c"
                  + Integer.toString(c & 0xffff, 16)
                  + ')');
        } else {
          buffer.put((byte) (0xf0 | (codePoint >> 18)));
          buffer.put((byte) (0x80 | (0x3f & codePoint >> 12)));
          buffer.put((byte) (0x80 | (0x3f & codePoint >> 6)));
          buffer.put((byte) (0x80 | (0x3f & codePoint)));
          i++; // 2 chars
        }
      } else {
        buffer.put((byte) (0xe0 | (0x0f & c >> 12)));
        buffer.put((byte) (0x80 | (0x3f & c >> 6)));
        buffer.put((byte) (0x80 | (0x3f & c)));
      }
    }
  }

  public static int findPositionAfterKey(ByteBuffer src) {
    int p = src.position();
    while (true) {
      for (int i = 0; ; i++) {
        if (src.get(p++) == 0) {
          if (i == 0) {
            // ignore the trailing 0 of the element and trailing 0 of the "end of key"
            return p;
          }
          break;
        }
      }
    }
  }

  public static StoreKey deserializeKey(ByteBuffer src) {
    int p0 = src.position();
    while (true) {
      for (int i = 0; ; i++) {
        if (src.get() == 0) {
          if (i == 0) {
            // ignore the trailing 0 of the element and trailing 0 of the "end of key"
            int end = src.position() - 2;

            String s;
            int len = end - p0;
            if (src.hasArray()) {
              s = new String(src.array(), src.arrayOffset() + p0, len, StandardCharsets.UTF_8);
            } else {
              byte[] array = new byte[len];
              src.position(p0);
              src.get(array);
              s = new String(array, StandardCharsets.UTF_8);
            }

            return new StoreKey(s);
          }
          break;
        }
      }
    }
  }

  /**
   * Checks the values of this instance.
   *
   * <p>This is not embedded into immutable's {@code .build()} check for performance reasons.
   */
  public StoreKey check() {
    checkState(key.length() <= MAX_LENGTH, "Key too long, max allowed length: %s", MAX_LENGTH);
    return this;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(key.length());
    for (int i = 0; i < key.length(); i++) {
      char c = key.charAt(i);
      switch (c) {
        case 0:
          c = '/';
          break;
        case 1:
          c = '.';
          break;
        default:
          break;
      }
      sb.append(c);
    }
    return sb.toString();
  }

  public boolean startsWith(StoreKey prefix) {
    return key.startsWith(prefix.key);
  }

  public boolean startsWithElementsOrParts(StoreKey prefix) {
    int m = CharBuffer.wrap(key).mismatch(CharBuffer.wrap(prefix.key));
    if (m == -1) {
      // equal
      return true;
    }
    if (m < prefix.key.length()) {
      // prefix does not match at all
      return false;
    }
    // check for element or part border
    char c = key.charAt(m);
    return c == (char) 0 || c == (char) 1;
  }

  /** Tests whether this store key ends with the given element. */
  public boolean endsWithElement(String element) {
    int elLen = element.length();
    int len = key.length();
    if (len < elLen + 1) {
      if (len == elLen) {
        return key.equals(element);
      }
      return false;
    }
    return key.charAt(len - elLen - 1) == (char) 0 && key.endsWith(element);
  }
}
