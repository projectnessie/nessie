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

import static org.projectnessie.versioned.storage.common.util.Ser.putVarInt;
import static org.projectnessie.versioned.storage.common.util.Ser.varIntLen;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;

public final class Util {
  private Util() {}

  public static final String STRING_100 =
      "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";

  private static final char[] HEX = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  public static String asHex(ByteString b) {
    StringBuilder sb = new StringBuilder();
    int size = b.size();
    for (int p = 0; p < size; p++) {
      byte v = b.byteAt(p);
      sb.append(HEX[(v >> 4) & 0xf]);
      sb.append(HEX[v & 0xf]);
    }
    return sb.toString();
  }

  public static String asHex(ByteBuffer b) {
    StringBuilder sb = new StringBuilder();
    for (int p = b.position(); p < b.limit(); p++) {
      int v = b.get(p);
      sb.append(HEX[(v >> 4) & 0xf]);
      sb.append(HEX[v & 0xf]);
    }
    return sb.toString();
  }

  public static ByteBuffer generateObjIdBuffer(boolean directBuffer, int len) {
    int l = len + varIntLen(len);
    ByteBuffer buf = directBuffer ? ByteBuffer.allocateDirect(l) : ByteBuffer.allocate(l);
    putVarInt(buf, len);
    for (int p = 0; p < len; p++) {
      buf.put((byte) p);
    }
    buf.flip();
    return buf;
  }
}
