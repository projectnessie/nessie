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

import static org.projectnessie.versioned.transfer.ExportImportConstants.EXPORT_METADATA;
import static org.projectnessie.versioned.transfer.ExportImportConstants.HEADS_AND_FORKS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.files.FileExporter;
import org.projectnessie.versioned.transfer.files.FileImporter;
import org.projectnessie.versioned.transfer.files.ImportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;

public class TestFileExportImport extends AbstractExportImport {

  @Override
  protected ExportMeta readMeta(Path targetDir) throws IOException {
    return ExportMeta.parseFrom(Files.readAllBytes(targetDir.resolve(EXPORT_METADATA)));
  }

  @Override
  protected HeadsAndForks readHeadsAndForks(Path targetDir) throws IOException {
    return HeadsAndForks.parseFrom(Files.readAllBytes(targetDir.resolve(HEADS_AND_FORKS)));
  }

  @Override
  protected List<String> listFiles(Path targetDir) throws IOException {
    try (Stream<Path> listing = Files.list(targetDir)) {
      return listing.map(p -> p.getFileName().toString()).collect(Collectors.toList());
    }
  }

  @Override
  protected ExportFileSupplier prepareExporter(Path targetDir) {
    return FileExporter.builder().targetDirectory(targetDir).build();
  }

  @Override
  protected ImportFileSupplier prepareImporter(Path targetDir) {
    return FileImporter.builder().sourceDirectory(targetDir).build();
  }
}
