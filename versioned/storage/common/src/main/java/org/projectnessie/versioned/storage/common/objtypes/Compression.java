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
package org.projectnessie.versioned.storage.common.objtypes;

import java.util.Locale;

public enum Compression {
  NONE('N'),
  GZIP('G'),
  DEFLATE('D'),
  ZSTD('Z'),
  LZ4('L'),
  SNAPPY('S');

  public static Compression fromValue(String valueString) {
    if (valueString.length() == 1) {
      return fromValue(valueString.charAt(0));
    }
    return Compression.valueOf(valueString.toUpperCase(Locale.ROOT));
  }

  public static Compression fromValue(char value) {
    switch (value) {
      case 'N':
        return NONE;
      case 'G':
        return GZIP;
      case 'D':
        return DEFLATE;
      case 'Z':
        return ZSTD;
      case 'L':
        return LZ4;
      case 'S':
        return SNAPPY;
      default:
        throw new IllegalArgumentException("Illegal value '" + value + "' for Compression");
    }
  }

  private final char value;
  private final String valueString;

  Compression(char value) {
    this.value = value;
    this.valueString = "" + value;
  }

  public char value() {
    return value;
  }

  public String valueString() {
    return valueString;
  }
}
