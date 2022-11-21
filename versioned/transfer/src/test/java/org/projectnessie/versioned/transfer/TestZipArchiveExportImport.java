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
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;

public class TestZipArchiveExportImport extends AbstractExportImport {

  @Override
  protected ExportMeta readMeta(Path targetDir) throws IOException {
    Path zipFile = targetDir.resolve("export.zip");
    try (ZipFile zip = new ZipFile(zipFile.toFile())) {
      try (InputStream metaInput = zip.getInputStream(zip.getEntry(EXPORT_METADATA))) {
        return ExportMeta.parseFrom(metaInput);
      }
    }
  }

  @Override
  protected HeadsAndForks readHeadsAndForks(Path targetDir) throws IOException {
    Path zipFile = targetDir.resolve("export.zip");
    try (ZipFile zip = new ZipFile(zipFile.toFile())) {
      try (InputStream metaInput = zip.getInputStream(zip.getEntry(HEADS_AND_FORKS))) {
        return HeadsAndForks.parseFrom(metaInput);
      }
    }
  }

  @Override
  protected List<String> listFiles(Path targetDir) throws IOException {
    Path zipFile = targetDir.resolve("export.zip");
    try (ZipFile zip = new ZipFile(zipFile.toFile())) {
      List<String> fileNames = new ArrayList<>();
      for (Enumeration<? extends ZipEntry> e = zip.entries(); e.hasMoreElements(); ) {
        fileNames.add(e.nextElement().getName());
      }
      return fileNames;
    }
  }

  @Override
  protected ExportFileSupplier prepareExporter(Path targetDir) {
    Path zipFile = targetDir.resolve("export.zip");
    return ZipArchiveExporter.builder().outputFile(zipFile).build();
  }

  @Override
  protected ImportFileSupplier prepareImporter(Path targetDir) {
    Path zipFile = targetDir.resolve("export.zip");
    return ZipArchiveImporter.builder().sourceZipFile(zipFile).build();
  }
}
