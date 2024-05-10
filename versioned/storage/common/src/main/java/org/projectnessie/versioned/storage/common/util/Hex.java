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
package org.projectnessie.versioned.storage.common.util;

public final class Hex {

  private static final char[] HEX = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  private Hex() {}

  public static char hexChar(byte b) {
    return HEX[b & 0xf];
  }

  public static long stringToLong(String s, int off) {
    long l = ((long) nibble(s.charAt(off++))) << 60L;
    l |= ((long) nibble(s.charAt(off++))) << 56L;
    l |= ((long) nibble(s.charAt(off++))) << 52L;
    l |= ((long) nibble(s.charAt(off++))) << 48L;
    l |= ((long) nibble(s.charAt(off++))) << 44L;
    l |= ((long) nibble(s.charAt(off++))) << 40L;
    l |= ((long) nibble(s.charAt(off++))) << 36L;
    l |= ((long) nibble(s.charAt(off++))) << 32L;
    l |= ((long) nibble(s.charAt(off++))) << 28L;
    l |= ((long) nibble(s.charAt(off++))) << 24L;
    l |= ((long) nibble(s.charAt(off++))) << 20L;
    l |= ((long) nibble(s.charAt(off++))) << 16L;
    l |= ((long) nibble(s.charAt(off++))) << 12L;
    l |= ((long) nibble(s.charAt(off++))) << 8L;
    l |= ((long) nibble(s.charAt(off++))) << 4L;
    l |= nibble(s.charAt(off));
    return l;
  }

  public static int nibbleFromLong(long v, int index) {
    return (int) (v >> (60 - index * 4L)) & 0xf;
  }

  public static byte byteFromLong(long v, int index) {
    return (byte) (v >> (56 - index * 8L));
  }

  public static byte nibble(char c) {
    if (c >= '0' && c <= '9') {
      return (byte) (c - '0');
    }
    if (c >= 'a' && c <= 'f') {
      return (byte) (c - 'a' + 10);
    }
    if (c >= 'A' && c <= 'F') {
      return (byte) (c - 'A' + 10);
    }
    throw new IllegalArgumentException("Illegal hex character '" + c + "'");
  }
}
