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
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.REMOVE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.contentIdMaybe;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;

import java.io.IOException;
import java.io.InputStream;
import org.projectnessie.model.Content;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Commit;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Operation;

abstract class ImportPersistCommon extends ImportCommon {
  ImportPersistCommon(ExportMeta exportMeta, NessieImporter importer) {
    super(exportMeta, importer);
  }

  @Override
  void importFinalize(HeadsAndForks headsAndForks) {
    IndexesLogic indexesLogic = indexesLogic(requireNonNull(importer.persist()));
    for (ByteString head : headsAndForks.getHeadsList()) {
      try {
        indexesLogic.completeIndexesInCommitChain(
            ObjId.objIdFromBytes(head),
            () -> importer.progressListener().progress(ProgressEvent.FINALIZE_PROGRESS));
      } catch (ObjNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  long importCommits() throws IOException {
    long commitCount = 0L;
    try (BatchWriter<Obj> batchWriter = BatchWriter.objWriter(requireNonNull(importer.persist()))) {
      for (String fileName : exportMeta.getCommitsFilesList()) {
        try (InputStream input = importFiles.newFileInput(fileName)) {
          while (true) {
            Commit commit = Commit.parseDelimitedFrom(input);
            if (commit == null) {
              break;
            }
            processCommit(batchWriter, commit);
            commitCount++;
          }
        }
      }
    }
    return commitCount;
  }

  abstract void processCommit(BatchWriter<Obj> batchWriter, Commit commit) throws IOException;

  void processCommitOp(
      BatchWriter<Obj> batchWriter, StoreIndex<CommitOp> index, Operation op, StoreKey storeKey) {
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
          ByteString onRef =
              importer
                  .storeWorker()
                  .toStoreOnReferenceState(
                      content,
                      att -> {
                        throw new UnsupportedOperationException();
                      });

          ContentValueObj value = contentValue(op.getContentId(), payload, onRef);
          batchWriter.add(value);
          index.add(
              indexElement(
                  storeKey, commitOp(ADD, payload, value.id(), contentIdMaybe(op.getContentId()))));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown operation type " + op);
    }
  }
}
