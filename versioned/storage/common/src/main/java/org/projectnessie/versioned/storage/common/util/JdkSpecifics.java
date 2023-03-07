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

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class JdkSpecifics {

  public static final JdkSpecific JDK_SPECIFIC;

  static {
    JdkSpecific m;
    try {
      m = new Java11();
      m.mismatch(
          ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)),
          ByteBuffer.wrap("world".getBytes(StandardCharsets.UTF_8)));
    } catch (Throwable t) {
      m = new Java8();
    }
    JDK_SPECIFIC = m;
  }

  public interface JdkSpecific {
    int mismatch(ByteBuffer b1, ByteBuffer b2);
  }

  @VisibleForTesting
  static final class Java11 implements JdkSpecific {
    @Override
    public int mismatch(ByteBuffer b1, ByteBuffer b2) {
      return b1.mismatch(b2);
    }

    @Override
    public String toString() {
      return "JdkSpecific.Java11";
    }
  }

  @VisibleForTesting
  static final class Java8 implements JdkSpecific {
    @Override
    public int mismatch(ByteBuffer b1, ByteBuffer b2) {
      int l1 = b1.limit();
      int l2 = b2.limit();
      int p1 = b1.position();
      int p2 = b2.position();
      for (int i = 0; ; i++, p1++, p2++) {
        if (p1 < l1 && p2 < l2) {
          byte v1 = b1.get(p1);
          byte v2 = b2.get(p2);
          if (v1 == v2) {
            continue;
          }
        }
        return p1 == l1 && p2 == l2 ? -1 : i;
      }
    }

    @Override
    public String toString() {
      return "JdkSpecific.Java8";
    }
  }
}
