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
package org.projectnessie.client.auth.oauth2;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

/** A secret that can be cleared once read. */
public final class Secret {

  // Visible for testing
  final char[] value;

  public Secret(char... value) {
    this.value = value;
  }

  public Secret(String value) {
    this.value = value.toCharArray();
  }

  public int length() {
    return value.length;
  }

  public char[] getCharsAndClear() {
    char[] v = value.clone();
    Arrays.fill(value, '\0');
    return v;
  }

  public String getStringAndClear() {
    String s = new String(value);
    Arrays.fill(value, '\0');
    return s;
  }

  public byte[] getBytesAndClear(Charset charset) {
    CharBuffer cb = CharBuffer.wrap(value);
    ByteBuffer bb = charset.encode(cb);
    byte[] bytes = new byte[bb.remaining()];
    bb.get(bytes);
    if (bb.hasArray()) {
      Arrays.fill(bb.array(), (byte) 0);
    }
    Arrays.fill(value, '\0');
    return bytes;
  }
}
