/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.client.rest.io;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * Captures the first 2kB of the input in case the response is not parsable, to provide a better
 * error message to the user.
 */
public final class CapturingInputStream extends FilterInputStream {
  static final int CAPTURE_LEN = 2048;
  private final byte[] capture = new byte[CAPTURE_LEN];
  private int captured;

  public CapturingInputStream(InputStream delegate) {
    super(delegate);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int rd = super.read(b, off, len);
    int captureRemain = capture.length - captured;
    if (rd > 0 && captureRemain > 0) {
      int copy = Math.min(rd, captureRemain);
      System.arraycopy(b, off, capture, captured, copy);
      captured += copy;
    }
    return rd;
  }

  @Override
  public int read() throws IOException {
    int rd = super.read();
    if (rd >= 0 && captured < capture.length) {
      capture[captured++] = (byte) rd;
    }
    return rd;
  }

  /**
   * Consumes the input stream up to {@value #CAPTURE_LEN} bytes and returns the captured content as
   * a string. This method does not close the input stream.
   */
  public String capture() {
    for (int i = 0; i < CAPTURE_LEN; i++) {
      try {
        if (read() < 0) {
          break;
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return captured();
  }

  /**
   * Returns the captured content so far, as a string. This method does not consume and does not
   * close the input stream.
   */
  public String captured() {
    return new String(capture, 0, captured, UTF_8);
  }
}
