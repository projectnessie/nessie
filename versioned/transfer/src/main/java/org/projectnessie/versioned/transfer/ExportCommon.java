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
import static org.projectnessie.versioned.transfer.ExportImportConstants.REPOSITORY_DESCRIPTION;

import java.io.IOException;
import java.io.OutputStream;
import org.projectnessie.api.NessieVersion;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.RepositoryDescriptionProto;

abstract class ExportCommon {
  final ExportFileSupplier exportFiles;
  final NessieExporter exporter;

  ExportCommon(ExportFileSupplier exportFiles, NessieExporter exporter) {
    this.exportFiles = exportFiles;
    this.exporter = exporter;
  }

  ExportMeta exportRepo() throws IOException {
    ExportContext exportContext = createExportContext(getExportVersion());
    try {
      exporter.progressListener().progress(ProgressEvent.STARTED);

      exporter.progressListener().progress(ProgressEvent.START_COMMITS);
      HeadsAndForks headsAndForks = exportCommits(exportContext);
      exportContext.commitOutput.finishCurrentFile();
      writeHeadsAndForks(headsAndForks);
      exporter.progressListener().progress(ProgressEvent.END_COMMITS);

      exporter.progressListener().progress(ProgressEvent.START_NAMED_REFERENCES);
      exportReferences(exportContext);
      exportContext.namedReferenceOutput.finishCurrentFile();
      exporter.progressListener().progress(ProgressEvent.END_NAMED_REFERENCES);

      exporter.progressListener().progress(ProgressEvent.START_META);
      writeRepositoryDescription();
      ExportMeta meta = exportContext.finish();
      writeExportMeta(meta);
      exporter.progressListener().progress(ProgressEvent.END_META, meta);

      exporter.progressListener().progress(ProgressEvent.FINISHED);

      return meta;
    } finally {
      // Ensure that all output streams are closed.
      exportContext.closeSilently();
    }
  }

  final long currentTimestampMillis() {
    return exporter.clock().millis();
  }

  ExportContext createExportContext(ExportVersion exportVersion) {
    return new ExportContext(
        exportFiles,
        exporter,
        ExportMeta.newBuilder()
            .setNessieVersion(NessieVersion.NESSIE_VERSION)
            .setCreatedMillisEpoch(currentTimestampMillis())
            .setVersion(exportVersion));
  }

  abstract ExportVersion getExportVersion();

  abstract void exportReferences(ExportContext exportContext);

  abstract HeadsAndForks exportCommits(ExportContext exportContext);

  abstract void writeRepositoryDescription() throws IOException;

  void writeExportMeta(ExportMeta meta) throws IOException {
    try (OutputStream output = exportFiles.newFileOutput(EXPORT_METADATA)) {
      meta.writeTo(output);
    }
  }

  void writeHeadsAndForks(HeadsAndForks hf) throws IOException {
    try (OutputStream output = exportFiles.newFileOutput(HEADS_AND_FORKS)) {
      hf.writeTo(output);
    }
  }

  void writeRepositoryDescription(RepositoryDescriptionProto repositoryDescription)
      throws IOException {
    try (OutputStream output = exportFiles.newFileOutput(REPOSITORY_DESCRIPTION)) {
      repositoryDescription.writeTo(output);
    }
  }
}
