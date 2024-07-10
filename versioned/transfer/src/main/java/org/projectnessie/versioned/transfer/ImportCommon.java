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

import java.io.IOException;
import org.projectnessie.versioned.transfer.files.ImportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;

abstract class ImportCommon {

  final ImportFileSupplier importFiles;
  final ExportMeta exportMeta;
  final NessieImporter importer;

  ImportCommon(ExportMeta exportMeta, NessieImporter importer) {
    this.importFiles = importer.importFileSupplier();
    this.exportMeta = exportMeta;
    this.importer = importer;
  }

  ImportResult importRepo() throws IOException {
    HeadsAndForks headsAndForks = importer.loadHeadsAndForks();

    importer.progressListener().progress(ProgressEvent.START_PREPARE);
    prepareRepository();
    importer.progressListener().progress(ProgressEvent.END_PREPARE);

    importer.progressListener().progress(ProgressEvent.START_GENERIC);
    long genericCount = importGeneric();
    importer.progressListener().progress(ProgressEvent.END_GENERIC);

    importer.progressListener().progress(ProgressEvent.START_COMMITS);
    long commitCount = importCommits();
    importer.progressListener().progress(ProgressEvent.END_COMMITS);

    importer.progressListener().progress(ProgressEvent.START_NAMED_REFERENCES);
    long namedReferenceCount = importNamedReferences();
    importer.progressListener().progress(ProgressEvent.END_NAMED_REFERENCES);

    importer.progressListener().progress(ProgressEvent.START_FINALIZE);
    importFinalize(headsAndForks);
    importer.progressListener().progress(ProgressEvent.END_FINALIZE);

    markRepositoryImported();
    importer.progressListener().progress(ProgressEvent.FINISHED);

    return ImmutableImportResult.builder()
        .exportMeta(exportMeta)
        .headsAndForks(headsAndForks)
        .importedCommitCount(commitCount)
        .importedGenericCount(genericCount)
        .importedReferenceCount(namedReferenceCount)
        .build();
  }

  abstract void prepareRepository() throws IOException;

  abstract long importNamedReferences() throws IOException;

  abstract long importCommits() throws IOException;

  abstract long importGeneric() throws IOException;

  abstract void importFinalize(HeadsAndForks headsAndForks);

  abstract void markRepositoryImported();
}
