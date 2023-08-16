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
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Optional;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.CommitMetaSerializer;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableRepoDescription;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Commit;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.NamedReference;

final class ImportDatabaseAdapter extends ImportCommon {

  ImportDatabaseAdapter(ExportMeta exportMeta, NessieImporter importer) {
    super(exportMeta, importer);

    checkState(
        exportMeta.getVersion() == ExportVersion.V1,
        "This Nessie-version version does not support importing a %s (%s) export",
        exportMeta.getVersion().name(),
        exportMeta.getVersionValue());
  }

  @Override
  void prepareRepository() {
    DatabaseAdapter databaseAdapter = requireNonNull(importer.databaseAdapter());
    databaseAdapter.eraseRepo();
    databaseAdapter.initializeRepo("main");
    try {
      databaseAdapter.delete(BranchName.of("main"), Optional.empty());
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  long importNamedReferences() throws IOException {
    long namedReferenceCount = 0L;
    for (String fileName : exportMeta.getNamedReferencesFilesList()) {
      try (InputStream input = importFiles.newFileInput(fileName)) {
        while (true) {
          NamedReference namedReference = NamedReference.parseDelimitedFrom(input);
          if (namedReference == null) {
            break;
          }

          NamedRef ref;
          switch (namedReference.getRefType()) {
            case Tag:
              ref = TagName.of(namedReference.getName());
              break;
            case Branch:
              ref = BranchName.of(namedReference.getName());
              break;
            default:
              throw new IllegalArgumentException("Unknown reference type " + namedReference);
          }

          try {
            requireNonNull(importer.databaseAdapter())
                .create(ref, Hash.of(namedReference.getCommitId()));
          } catch (ReferenceAlreadyExistsException | ReferenceNotFoundException e) {
            throw new RuntimeException(e);
          }

          namedReferenceCount++;
          importer.progressListener().progress(ProgressEvent.NAMED_REFERENCE_WRITTEN);
        }
      }
    }
    return namedReferenceCount;
  }

  @Override
  long importCommits() throws IOException {
    long commitCount = 0L;
    int keyListDistance =
        requireNonNull(importer.databaseAdapter()).getConfig().getKeyListDistance();

    try (Batcher<CommitLogEntry> commitBatcher =
        Batcher.commitBatchWriter(importer.commitBatchSize(), importer.databaseAdapter())) {

      for (String fileName : exportMeta.getCommitsFilesList()) {
        try (InputStream input = importFiles.newFileInput(fileName)) {
          while (true) {
            Commit commit = Commit.parseDelimitedFrom(input);
            if (commit == null) {
              break;
            }

            ByteString metadata;
            try (InputStream in = commit.getMetadata().newInput()) {
              metadata =
                  CommitMetaSerializer.METADATA_SERIALIZER.toBytes(
                      importer.objectMapper().readValue(in, CommitMeta.class));
            }

            ImmutableCommitLogEntry.Builder logEntry =
                ImmutableCommitLogEntry.builder()
                    .createdTime(commit.getCreatedTimeMicros())
                    .commitSeq(commit.getCommitSequence())
                    .hash(Hash.of(commit.getCommitId()))
                    .metadata(metadata)
                    .keyListDistance((int) (commit.getCommitSequence() % keyListDistance))
                    .addParents(Hash.of(commit.getParentCommitId()));
            commit
                .getAdditionalParentsList()
                .forEach(ap -> logEntry.addAdditionalParents(Hash.of(ap)));
            commit
                .getOperationsList()
                .forEach(
                    op -> {
                      ContentKey key = ContentKey.of(op.getContentKeyList());
                      switch (op.getOperationType()) {
                        case Delete:
                          logEntry.addDeletes(key);
                          break;
                        case Put:
                          try (InputStream inValue = op.getValue().newInput()) {
                            Content content =
                                importer.objectMapper().readValue(inValue, Content.class);
                            ByteString onRef =
                                importer.storeWorker().toStoreOnReferenceState(content);

                            logEntry.addPuts(
                                KeyWithBytes.of(
                                    key,
                                    ContentId.of(op.getContentId()),
                                    (byte) op.getPayload(),
                                    onRef));

                          } catch (IOException e) {
                            throw new RuntimeException(e);
                          }
                          break;
                        default:
                          throw new IllegalArgumentException("Unknown operation type " + op);
                      }
                    });

            commitBatcher.add(logEntry.build());
            commitCount++;

            importer.progressListener().progress(ProgressEvent.COMMIT_WRITTEN);
          }
        }
      }
    }
    return commitCount;
  }

  @Override
  void importFinalize(HeadsAndForks headsAndForks) {}

  @Override
  void markRepositoryImported() {
    DatabaseAdapter databaseAdapter = requireNonNull(importer.databaseAdapter());
    try {
      databaseAdapter.updateRepositoryDescription(
          initial ->
              ImmutableRepoDescription.builder()
                  .from(initial)
                  .putProperties(RepoDescription.IMPORTED_AT_KEY, Instant.now().toString())
                  .build());
    } catch (ReferenceConflictException e) {
      throw new RuntimeException(e);
    }
  }
}
