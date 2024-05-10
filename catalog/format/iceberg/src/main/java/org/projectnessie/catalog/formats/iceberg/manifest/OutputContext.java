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
package org.projectnessie.catalog.formats.iceberg.manifest;

import java.io.IOException;
import java.io.OutputStream;

final class OutputContext extends OutputStream {
  private final OutputStream output;
  private final boolean closeOutput;

  OutputContext(OutputStream output, boolean closeOutput) {
    this.output = output;
    this.closeOutput = closeOutput;
  }

  private long written;

  @Override
  public void write(int b) throws IOException {
    output.write(b);
    written++;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    output.write(b, off, len);
    written += len;
  }

  public long bytesWritten() {
    return written;
  }

  @Override
  public void flush() throws IOException {
    output.flush();
  }

  @Override
  public void close() throws IOException {
    if (closeOutput) {
      output.close();
    }
  }
}
