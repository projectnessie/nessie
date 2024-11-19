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
package org.projectnessie.versioned.transfer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.projectnessie.nessie.relocated.protobuf.AbstractMessage;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;

final class SizeLimitedOutput {

  private final ExportFileSupplier exportFiles;
  private final String fileNamePrefix;
  private final Consumer<String> newFileName;
  private final LongConsumer finalEntityCount;
  private final long maxFileSize;
  private final int outputBufferSize;
  private int fileNum;
  long entityCount;
  private long currentFileSize;

  private OutputStream output;

  SizeLimitedOutput(
      ExportFileSupplier exportFiles,
      NessieExporter exporter,
      String fileNamePrefix,
      Consumer<String> newFileName,
      LongConsumer finalEntityCount) {
    this.exportFiles = exportFiles;
    this.maxFileSize = exportFiles.fixMaxFileSize(exporter.maxFileSize());
    this.outputBufferSize = exporter.outputBufferSize();
    this.fileNamePrefix = fileNamePrefix;
    this.newFileName = newFileName;
    this.finalEntityCount = finalEntityCount;
  }

  void writeEntity(AbstractMessage message) {
    try {
      int size = message.getSerializedSize();
      currentFileSize += size;
      if (maxFileSize != Long.MAX_VALUE) {
        if (currentFileSize > maxFileSize) {
          currentFileSize = 0L;
          finishCurrentFile();
        }
      }
      if (output == null) {
        fileNum++;
        String fileName = String.format("%s-%08d", fileNamePrefix, fileNum);
        output = new BufferedOutputStream(exportFiles.newFileOutput(fileName), outputBufferSize);
        newFileName.accept(fileName);
      }
      message.writeDelimitedTo(output);
      entityCount++;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void finish() throws IOException {
    finishCurrentFile();
    finalEntityCount.accept(entityCount);
  }

  void finishCurrentFile() throws IOException {
    if (output != null) {
      try {
        output.flush();
        output.close();
      } finally {
        output = null;
      }
    }
  }

  public void closeSilently() {
    try {
      finishCurrentFile();
    } catch (IOException ignore) {
      // ... silent
    }
  }
}
