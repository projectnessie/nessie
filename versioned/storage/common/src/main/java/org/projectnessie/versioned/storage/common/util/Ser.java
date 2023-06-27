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
package org.projectnessie.versioned.storage.common.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;

public final class Ser {
  private Ser() {}

  public static final ObjectMapper SHARED_OBJECT_MAPPER = new ObjectMapper();

  public static int varIntLen(int v) {
    checkArgument(v >= 0);
    v &= 0x7fffffff;
    int l = 0;
    while (true) {
      l++;
      if (v <= 0x7f) {
        return l;
      }
      v >>= 7;
    }
  }

  public static ByteBuffer putVarInt(ByteBuffer b, int v) {
    checkArgument(v >= 0);
    v &= 0x7fffffff;

    while (true) {
      if (v <= 0x7f) {
        return b.put((byte) v);
      }

      b.put((byte) (v | 0x80));

      v >>= 7;
    }
  }

  public static int readVarInt(ByteBuffer b) {
    int r = 0;
    for (int shift = 0; ; shift += 7) {
      int v = b.get() & 0xff;
      r |= (v & 0x7f) << shift;
      if ((v & 0x80) == 0) {
        break;
      }
    }
    return r;
  }

  public static void skipVarInt(ByteBuffer b) {
    for (; ; ) {
      int v = b.get() & 0xff;
      if ((v & 0x80) == 0) {
        break;
      }
    }
  }
}
