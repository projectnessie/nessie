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

import static com.google.common.base.Preconditions.checkState;
import static org.projectnessie.versioned.transfer.ExportImportConstants.DEFAULT_ATTACHMENT_BATCH_SIZE;
import static org.projectnessie.versioned.transfer.ExportImportConstants.DEFAULT_COMMIT_BATCH_SIZE;
import static org.projectnessie.versioned.transfer.ExportImportConstants.EXPORT_METADATA;
import static org.projectnessie.versioned.transfer.ExportImportConstants.HEADS_AND_FORKS;
import static org.projectnessie.versioned.transfer.ExportImportConstants.REPOSITORY_DESCRIPTION;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.transfer.files.ImportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.RepositoryDescriptionProto;

@Value.Immutable
public abstract class NessieImporter {

  public static NessieImporter.Builder builder() {
    return ImmutableNessieImporter.builder();
  }

  @SuppressWarnings("UnusedReturnValue")
  public interface Builder {
    /** Specify the {@code DatabaseAdapter} to use. */
    Builder databaseAdapter(DatabaseAdapter databaseAdapter);

    /** Specify the {@code Persist} to use. */
    Builder persist(Persist persist);

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

  @Nullable
  @jakarta.annotation.Nullable
  abstract DatabaseAdapter databaseAdapter();

  @Nullable
  @jakarta.annotation.Nullable
  abstract Persist persist();

  @Value.Check
  void check() {
    checkState(
        persist() == null ^ databaseAdapter() == null,
        "Must supply either persist() or databaseAdapter(), never both");
  }

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
  public RepositoryDescriptionProto loadRepositoryDescription() throws IOException {
    try (InputStream input = importFileSupplier().newFileInput(REPOSITORY_DESCRIPTION)) {
      return RepositoryDescriptionProto.parseFrom(input);
    }
  }

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

    if (databaseAdapter() != null) {
      return new ImportDatabaseAdapter(exportMeta, this).importRepo();
    }

    switch (exportMeta.getVersion()) {
      case V1:
        return new ImportPersistV1(exportMeta, this).importRepo();
      case V2:
        return new ImportPersistV2(exportMeta, this).importRepo();
      default:
        throw new IllegalStateException(
            String.format(
                "This Nessie version does not support importing a %s (%d) export",
                exportMeta.getVersion().name(), exportMeta.getVersionValue()));
    }
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
