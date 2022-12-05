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
package org.projectnessie.versioned.transfer.files;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.write;

import org.junit.jupiter.api.Test;

public class TestFileExporterImporter
    extends AbstractTestExporterImporter<FileExporter, FileImporter> {

  @Override
  protected FileExporter newExportFileSupplier(String target) {
    return FileExporter.builder().targetDirectory(dir.resolve(target)).build();
  }

  @Override
  protected FileImporter newImportFileSupplier(FileExporter exporter) {
    return FileImporter.builder().sourceDirectory(exporter.targetDirectory()).build();
  }

  @Test
  public void nonEmptyTarget() throws Exception {
    String target = "target";
    createDirectories(dir.resolve(target));
    write(dir.resolve(target).resolve("foo"), "hello".getBytes(UTF_8));
    try (FileExporter exporter = newExportFileSupplier(target)) {
      soft.assertThatIllegalStateException()
          .isThrownBy(exporter::preValidate)
          .withMessageContaining(" must be empty, but is not");
    }
  }
}
