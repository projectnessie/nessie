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

import static org.projectnessie.versioned.transfer.ExportImportConstants.DEFAULT_ATTACHMENT_BATCH_SIZE;
import static org.projectnessie.versioned.transfer.ExportImportConstants.DEFAULT_COMMIT_BATCH_SIZE;
import static org.projectnessie.versioned.transfer.ExportImportConstants.EXPORT_METADATA;
import static org.projectnessie.versioned.transfer.ExportImportConstants.HEADS_AND_FORKS;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import org.immutables.value.Value;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;

@Value.Immutable
public abstract class NessieImporter {

  public static NessieImporter.Builder builder() {
    return ImmutableNessieImporter.builder();
  }

  @SuppressWarnings("UnusedReturnValue")
  public interface Builder {
    /** Mandatory, specify the {@code DatabaseAdapter} to use. */
    Builder databaseAdapter(DatabaseAdapter databaseAdapter);

    /** Optional, specify a custom {@link ObjectMapper}. */
    Builder objectMapper(ObjectMapper objectMapper);

    /** Optional, specify a custom {@link StoreWorker}. */
    Builder storeWorker(StoreWorker storeWorker);

    /**
     * Optional, specify the number of commit log entries to be written at once, defaults to {@value
     * ExportImportConstants#DEFAULT_COMMIT_BATCH_SIZE}.
     */
    Builder commitBatchSize(int commitBatchSize);

    /**
     * Optional, specify the number of content attachments to be written at once, defaults to
     * {@value ExportImportConstants#DEFAULT_ATTACHMENT_BATCH_SIZE}.
     */
    Builder attachmentBatchSize(int attachmentBatchSize);

    Builder progressListener(ProgressListener progressListener);

    Builder importFileSupplier(ImportFileSupplier importFileSupplier);

    NessieImporter build();
  }

  abstract DatabaseAdapter databaseAdapter();

  @Value.Default
  int commitBatchSize() {
    return DEFAULT_COMMIT_BATCH_SIZE;
  }

  @Value.Default
  int attachmentBatchSize() {
    return DEFAULT_ATTACHMENT_BATCH_SIZE;
  }

  @Value.Default
  StoreWorker storeWorker() {
    return DefaultStoreWorker.instance();
  }

  @Value.Default
  ObjectMapper objectMapper() {
    return new ObjectMapper();
  }

  @Value.Default
  ProgressListener progressListener() {
    return (x, y) -> {};
  }

  abstract ImportFileSupplier importFileSupplier();

  @SuppressWarnings("resource")
  public HeadsAndForks loadHeadsAndForks() throws IOException {
    try (InputStream input = importFileSupplier().newFileInput(HEADS_AND_FORKS)) {
      return HeadsAndForks.parseFrom(input);
    }
  }

  public ImportResult importNessieRepository() throws IOException {
    // Need to load the export metadata here and not in `ExportCommon`, because the export meta
    // is needed to choose the import implementation.

    progressListener().progress(ProgressEvent.STARTED);

    progressListener().progress(ProgressEvent.START_META);
    ExportMeta exportMeta = loadExportMeta();
    progressListener().progress(ProgressEvent.END_META, exportMeta);

    return new ImportDatabaseAdapter(exportMeta, this).importRepo();
  }

  @SuppressWarnings("resource")
  private ExportMeta loadExportMeta() throws IOException {
    ExportMeta exportMeta;
    try (InputStream input = importFileSupplier().newFileInput(EXPORT_METADATA)) {
      exportMeta = ExportMeta.parseFrom(input);
    }
    return exportMeta;
  }
}
