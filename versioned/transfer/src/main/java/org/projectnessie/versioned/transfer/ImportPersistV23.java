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

import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromBytes;
import static org.projectnessie.versioned.storage.common.persist.ObjTypes.objTypeByName;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;
import org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.serialize.SmileSerialization;
import org.projectnessie.versioned.transfer.serialize.TransferTypes;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Commit;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Ref;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.RelatedObj;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.RepositoryDescriptionProto;

final class ImportPersistV23 extends ImportPersistCommon {

  ImportPersistV23(ExportMeta exportMeta, NessieImporter importer) {
    super(exportMeta, importer);
  }

  @Override
  void prepareRepository() throws IOException {
    RepositoryDescriptionProto repositoryDescription = importer.loadRepositoryDescription();

    importer
        .repositoryLogic()
        .initialize(
            repositoryDescription.getDefaultBranchName(),
            false,
            b -> {
              if (repositoryDescription.hasOldestCommitTimestampMillis()) {
                b.oldestPossibleCommitTime(
                    Instant.ofEpochMilli(repositoryDescription.getOldestCommitTimestampMillis()));
              }
              b.putAllProperties(repositoryDescription.getPropertiesMap());
            });
  }

  @Override
  long importNamedReferences() throws IOException {
    try {
      long namedReferenceCount = 0L;
      for (String fileName : exportMeta.getNamedReferencesFilesList()) {
        try (InputStream input = importFiles.newFileInput(fileName)) {
          while (true) {
            Ref ref = Ref.parseDelimitedFrom(input);
            if (ref == null) {
              break;
            }

            try {
              ByteString ext = ref.getExtendedInfoObj();
              if (ref.hasCreatedAtMicros()) {
                // Export format V3
                importer
                    .referenceLogic()
                    .createReferenceForImport(
                        ref.getName(),
                        objIdFromBytes(ref.getPointer()),
                        ext == null ? null : objIdFromBytes(ext),
                        ref.getCreatedAtMicros());
              } else {
                // Export format V2
                importer
                    .referenceLogic()
                    .createReference(
                        ref.getName(),
                        objIdFromBytes(ref.getPointer()),
                        ext == null ? null : objIdFromBytes(ext));
              }
            } catch (RefAlreadyExistsException | RetryTimeoutException e) {
              throw new RuntimeException(e);
            }

            namedReferenceCount++;
            importer.progressListener().progress(ProgressEvent.NAMED_REFERENCE_WRITTEN);
          }
        }
      }
      return namedReferenceCount;
    } finally {
      persist.flush();
    }
  }

  @Override
  void processCommit(Commit commit) throws ObjTooLargeException {
    CommitHeaders.Builder headers = newCommitHeaders();
    commit
        .getHeadersList()
        .forEach(h -> h.getValuesList().forEach(v -> headers.add(h.getName(), v)));

    CommitObj.Builder c =
        commitBuilder()
            .id(objIdFromBytes(commit.getCommitId()))
            .addTail(objIdFromBytes(commit.getParentCommitId()))
            .created(commit.getCreatedTimeMicros())
            .seq(commit.getCommitSequence())
            .message(commit.getMessage())
            .headers(headers.build())
            .incompleteIndex(true);
    commit.getAdditionalParentsList().forEach(ap -> c.addSecondaryParents(objIdFromBytes(ap)));
    StoreIndex<CommitOp> index = newStoreIndex(CommitOp.COMMIT_OP_SERIALIZER);
    commit
        .getOperationsList()
        .forEach(
            op -> {
              StoreKey storeKey = keyFromString(op.getContentKey(0));
              processCommitOp(index, op, storeKey);
            });

    c.incrementalIndex(index.serialize());

    persist.storeObj(c.build());

    importer.progressListener().progress(ProgressEvent.COMMIT_WRITTEN);
  }

  @Override
  void processGeneric(RelatedObj genericObj) throws ObjTooLargeException {
    ObjType type = objTypeByName(genericObj.getTypeName());
    ObjId id = objIdFromBytes(genericObj.getId());

    Obj obj;
    if (type.equals(StandardObjType.UNIQUE)) {
      obj = UniqueIdObj.uniqueId(id, 0L, genericObj.getSpace(), genericObj.getData());
    } else {
      ByteBuffer data = genericObj.getData().asReadOnlyByteBuffer();
      String versionToken = genericObj.hasVersionToken() ? genericObj.getVersionToken() : null;

      if (genericObj.getEncodingValue() != TransferTypes.Encoding.Smile_VALUE) {
        throw new IllegalArgumentException(
            "Unsupported generic object encoding " + genericObj.getEncoding());
      }

      obj =
          SmileSerialization.deserializeObj(
              id, versionToken, data, type, 0L, Compression.fromValue(genericObj.getCompression()));
    }

    persist.storeObj(obj);

    importer.progressListener().progress(ProgressEvent.GENERIC_WRITTEN);
  }
}
