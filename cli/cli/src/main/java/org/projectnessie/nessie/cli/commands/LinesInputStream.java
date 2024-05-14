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
package org.projectnessie.nessie.cli.commands;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.InputStream;
import java.util.Spliterator;
import java.util.stream.Stream;

public class LinesInputStream extends InputStream {
  private final Spliterator<String> source;

  public LinesInputStream(Stream<String> source) {
    this(source.spliterator());
  }

  public LinesInputStream(Spliterator<String> source) {
    this.source = source;
  }

  private byte[] current = null;
  private int pos;
  private final byte[] single = new byte[1];

  @Override
  public int read(byte[] b, int off, int len) {
    int rd = 0;

    while (len > 0) {
      byte[] c = current;
      if (c == null) {
        if (!source.tryAdvance(
            line -> {
              current = (line + '\n').getBytes(UTF_8);
              pos = 0;
            })) {
          return rd > 0 ? rd : -1;
        }
        c = current;
      }
      int remaining = c.length - pos;
      int canPull = Math.min(remaining, len);
      System.arraycopy(c, pos, b, off, canPull);
      rd += canPull;
      pos += canPull;
      off += canPull;
      len -= canPull;
      if (pos == c.length) {
        current = null;
      }
    }

    return rd;
  }

  @Override
  public int read() {
    int rd = read(single, 0, 1);
    if (rd <= 0) {
      return -1;
    }
    return ((int) single[0]) & 0xff;
  }
}
