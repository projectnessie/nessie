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

import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.REMOVE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.contentIdMaybe;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;

import java.io.IOException;
import java.io.InputStream;
import org.projectnessie.model.Content;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.batching.BatchingPersist;
import org.projectnessie.versioned.storage.batching.WriteBatching;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.ImmutableRepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Commit;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Operation;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.RelatedObj;

abstract class ImportPersistCommon extends ImportCommon {
  protected final BatchingPersist persist;

  ImportPersistCommon(ExportMeta exportMeta, NessieImporter importer) {
    super(exportMeta, importer);
    this.persist =
        WriteBatching.builder()
            .persist(requireNonNull(importer.persist()))
            .batchSize(importer.commitBatchSize())
            .optimistic(true)
            .build()
            .create();
  }

  @Override
  ImportResult importRepo() throws IOException {
    try {
      return super.importRepo();
    } finally {
      persist.flush();
    }
  }

  @Override
  void importFinalize(HeadsAndForks headsAndForks) {
    try {
      for (ByteString head : headsAndForks.getHeadsList()) {
        try {
          importer
              .indexesLogic()
              .completeIndexesInCommitChain(
                  ObjId.objIdFromBytes(head),
                  () -> importer.progressListener().progress(ProgressEvent.FINALIZE_PROGRESS));
        } catch (ObjNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    } finally {
      persist.flush();
    }
  }

  @Override
  long importCommits() throws IOException {
    long commitCount = 0L;
    try {
      for (String fileName : exportMeta.getCommitsFilesList()) {
        try (InputStream input = importFiles.newFileInput(fileName)) {
          while (true) {
            Commit commit = Commit.parseDelimitedFrom(input);
            if (commit == null) {
              break;
            }
            processCommit(commit);
            commitCount++;
          }
        } catch (ObjTooLargeException e) {
          throw new RuntimeException(e);
        }
      }
    } finally {
      persist.flush();
    }
    return commitCount;
  }

  @Override
  long importGeneric() throws IOException {
    long genericCount = 0L;
    try {
      for (String fileName : exportMeta.getGenericObjFilesList()) {
        try (InputStream input = importFiles.newFileInput(fileName)) {
          while (true) {
            RelatedObj generic = RelatedObj.parseDelimitedFrom(input);
            if (generic == null) {
              break;
            }
            processGeneric(generic);
            genericCount++;
          }
        } catch (ObjTooLargeException e) {
          throw new RuntimeException(e);
        }
      }
    } finally {
      persist.flush();
    }
    return genericCount;
  }

  @Override
  void markRepositoryImported() {
    RepositoryDescription initialDescription =
        requireNonNull(importer.repositoryLogic().fetchRepositoryDescription());
    ImmutableRepositoryDescription updatedDescription =
        ImmutableRepositoryDescription.builder()
            .from(initialDescription)
            .repositoryImportedTime(importer.persist().config().clock().instant())
            .build();
    try {
      importer.repositoryLogic().updateRepositoryDescription(updatedDescription);
    } catch (RetryTimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  abstract void processCommit(Commit commit) throws IOException, ObjTooLargeException;

  abstract void processGeneric(RelatedObj genericObj) throws IOException, ObjTooLargeException;

  void processCommitOp(StoreIndex<CommitOp> index, Operation op, StoreKey storeKey) {
    byte payload = (byte) op.getPayload();
    switch (op.getOperationType()) {
      case Delete:
        index.add(
            indexElement(
                storeKey, commitOp(REMOVE, payload, null, contentIdMaybe(op.getContentId()))));
        break;
      case Put:
        try (InputStream inValue = op.getValue().newInput()) {
          Content content = importer.objectMapper().readValue(inValue, Content.class);
          ByteString onRef = importer.storeWorker().toStoreOnReferenceState(content);

          ContentValueObj value = contentValue(op.getContentId(), payload, onRef);
          persist.storeObj(value);
          index.add(
              indexElement(
                  storeKey, commitOp(ADD, payload, value.id(), contentIdMaybe(op.getContentId()))));
        } catch (ObjTooLargeException | IOException e) {
          throw new RuntimeException(e);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown operation type " + op);
    }
  }
}
