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
import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.CommitMetaSerializer;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.versionstore.RefMapping;
import org.projectnessie.versioned.storage.versionstore.TypeMapping;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.TransferTypes;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Commit;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Operation;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.OperationType;

/**
 * Creates export artifacts by generating artificial commits from the latest content object values
 * on one particular branch.
 */
final class ExportContents extends ExportCommon {
  private final VersionStore store;

  private ByteString lastCommitId = ObjId.EMPTY_OBJ_ID.asBytes();

  ExportContents(
      ExportFileSupplier exportFiles, NessieExporter exporter, ExportVersion exportVersion) {
    super(exportFiles, exporter, exportVersion);
    store = exporter.versionStore();
  }

  @Override
  void prepare(ExportContext exportContext) {
    // nop
  }

  private <T> List<T> take(int n, Iterator<T> it) {
    List<T> result = new ArrayList<>(n);
    while (n-- > 0 && it.hasNext()) {
      result.add(it.next());
    }
    return result;
  }

  @SuppressWarnings("UnstableApiUsage")
  @Override
  HeadsAndForks exportCommits(ExportContext exportContext) {
    ReferenceInfo<CommitMeta> ref;
    try {
      ref = store.getNamedRef(exporter.contentsFromBranch(), GetNamedRefsParams.DEFAULT);
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    try {
      CommitObj commit =
          exporter.persist().fetchTypedObj(hashToObjId(ref.getHash()), COMMIT, CommitObj.class);
      handleGenericObjs(transferRelatedObjects.commitRelatedObjects(commit));
    } catch (ObjNotFoundException e) {
      // ignore
    }

    long startMicros = TimeUnit.MILLISECONDS.toMicros(currentTimestampMillis());

    long seq = 0;
    try (PaginationIterator<KeyEntry> entries =
        store.getKeys(ref.getNamedRef(), null, false, NO_KEY_RESTRICTIONS)) {
      while (true) {
        List<KeyEntry> batch = take(exporter.contentsBatchSize(), entries);
        if (batch.isEmpty()) {
          break;
        }

        ByteString meta =
            CommitMetaSerializer.METADATA_SERIALIZER.toBytes(
                CommitMeta.fromMessage(
                    String.format(
                        "Single branch export from '%s', part %d",
                        ref.getNamedRef().getName(), seq + 1)));
        long micros = TimeUnit.MILLISECONDS.toMicros(currentTimestampMillis());

        Hasher hasher = Hashing.sha256().newHasher();
        hasher.putBytes(meta.asReadOnlyByteBuffer());
        hasher.putBytes(lastCommitId.asReadOnlyByteBuffer());
        hasher.putLong(seq);
        hasher.putLong(micros);

        Commit.Builder commitBuilder =
            Commit.newBuilder()
                .setMetadata(meta)
                .setCommitSequence(seq++)
                .setCreatedTimeMicros(micros)
                .setParentCommitId(lastCommitId);

        Map<ContentKey, ContentResult> values =
            store.getValues(
                ref.getHash(),
                batch.stream().map(e -> e.getKey().contentKey()).collect(Collectors.toList()),
                false);
        for (Map.Entry<ContentKey, ContentResult> entry : values.entrySet()) {
          ContentKey key = entry.getKey();
          Content content = entry.getValue().content();
          Operation op = putOperationFromCommit(key, requireNonNull(content, "content")).build();
          hasher.putBytes(op.toByteArray());
          commitBuilder.addOperations(op);

          handleGenericObjs(transferRelatedObjects.contentRelatedObjects(content));
        }

        ByteString commitId = ByteString.copyFrom(hasher.hash().asBytes());
        commitBuilder.setCommitId(commitId);

        lastCommitId = commitId;
        exportContext.writeCommit(commitBuilder.build());
        exporter.progressListener().progress(ProgressEvent.COMMIT_WRITTEN);
      }
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    return HeadsAndForks.newBuilder()
        .setScanStartedAtInMicros(startMicros)
        .addHeads(lastCommitId)
        .build();
  }

  @Override
  void exportReferences(ExportContext exportContext) {
    Reference reference =
        exporter.persist().fetchReference(RefMapping.asBranchName(exporter.contentsFromBranch()));
    handleGenericObjs(transferRelatedObjects.referenceRelatedObjects(reference));

    ObjId extendedInfoObj = reference.extendedInfoObj();
    TransferTypes.Ref.Builder refBuilder =
        TransferTypes.Ref.newBuilder().setName(reference.name()).setPointer(lastCommitId);
    if (extendedInfoObj != null) {
      refBuilder.setExtendedInfoObj(extendedInfoObj.asBytes());
    }
    refBuilder.setCreatedAtMicros(exporter.persist().config().currentTimeMicros());

    exportContext.writeRef(refBuilder.build());

    exporter.progressListener().progress(ProgressEvent.NAMED_REFERENCE_WRITTEN);
  }

  private Operation.Builder putOperationFromCommit(ContentKey key, Content value) {
    return Operation.newBuilder()
        .setOperationType(OperationType.Put)
        .addContentKey(TypeMapping.keyToStoreKey(key).rawString())
        .setContentId(value.getId())
        .setPayload(DefaultStoreWorker.payloadForContent(value))
        .setValue(contentToValue(value));
  }

  private ByteString contentToValue(Content content) {
    try {
      return ByteString.copyFromUtf8(exporter.objectMapper().writeValueAsString(content));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
