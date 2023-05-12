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
import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;
import static org.projectnessie.versioned.storage.common.persist.ObjType.VALUE;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;
import org.projectnessie.model.Content;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.HeadsAndForkPoints;
import org.projectnessie.versioned.storage.common.logic.IdentifyHeadsAndForkPoints;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.logic.PagedResult;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Commit;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Operation;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.OperationType;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Ref;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.RepositoryDescriptionProto;

final class ExportPersist extends ExportCommon {
  ExportPersist(ExportFileSupplier exportFiles, NessieExporter exporter) {
    super(exportFiles, exporter);
  }

  @Override
  long currentTimestampMillis() {
    return requireNonNull(exporter.persist()).config().clock().millis();
  }

  @Override
  ExportVersion getExportVersion() {
    return ExportVersion.V2;
  }

  @Override
  HeadsAndForks exportCommits(ExportContext exportContext) {
    IndexesLogic indexesLogic = indexesLogic(exporter.persist());

    Consumer<CommitObj> commitHandler =
        c -> {
          Commit commit = mapCommitObj(c, indexesLogic);
          exportContext.writeCommit(commit);
          exporter.progressListener().progress(ProgressEvent.COMMIT_WRITTEN);
        };

    HeadsAndForkPoints headsAndForkPoints =
        exporter.fullScan() ? scanDatabase(commitHandler) : scanAllReferences(commitHandler);

    HeadsAndForks.Builder hf =
        HeadsAndForks.newBuilder()
            .setScanStartedAtInMicros(headsAndForkPoints.getScanStartedAtInMicros());
    headsAndForkPoints.getHeads().forEach(h -> hf.addHeads(h.asBytes()));
    headsAndForkPoints.getForkPoints().forEach(h -> hf.addForkPoints(h.asBytes()));
    return hf.build();
  }

  private HeadsAndForkPoints scanAllReferences(Consumer<CommitObj> commitHandler) {
    Persist persist = requireNonNull(exporter.persist());

    IdentifyHeadsAndForkPoints identify =
        new IdentifyHeadsAndForkPoints(
            exporter.expectedCommitCount(), persist.config().currentTimeMicros());

    ReferenceLogic referenceLogic = referenceLogic(persist);
    CommitLogic commitLogic = commitLogic(persist);
    referenceLogic
        .queryReferences(referencesQuery())
        .forEachRemaining(
            ref -> {
              for (Iterator<CommitObj> commitIter =
                      commitLogic.commitLog(commitLogQuery(ref.pointer()));
                  commitIter.hasNext(); ) {
                CommitObj commit = commitIter.next();
                if (!identify.handleCommit(commit)) {
                  break;
                }
                commitHandler.accept(commit);
              }
            });

    return identify.finish();
  }

  private HeadsAndForkPoints scanDatabase(Consumer<CommitObj> commitHandler) {
    CommitLogic commitLogic = commitLogic(requireNonNull(exporter.persist()));
    return commitLogic.identifyAllHeadsAndForkPoints(exporter.expectedCommitCount(), commitHandler);
  }

  @Override
  void exportReferences(ExportContext exportContext) {
    ReferenceLogic referenceLogic = referenceLogic(requireNonNull(exporter.persist()));
    for (PagedResult<Reference, String> refs = referenceLogic.queryReferences(referencesQuery());
        refs.hasNext(); ) {
      Reference reference = refs.next();
      Ref ref =
          Ref.newBuilder()
              .setName(reference.name())
              .setPointer(reference.pointer().asBytes())
              .build();
      exportContext.writeRef(ref);
      exporter.progressListener().progress(ProgressEvent.NAMED_REFERENCE_WRITTEN);
    }
  }

  @Override
  void writeRepositoryDescription() throws IOException {
    RepositoryDescription repositoryDescription =
        repositoryLogic(exporter.persist()).fetchRepositoryDescription();
    if (repositoryDescription != null) {
      writeRepositoryDescription(
          RepositoryDescriptionProto.newBuilder()
              .putAllProperties(repositoryDescription.properties())
              .setRepositoryId(requireNonNull(exporter.persist()).config().repositoryId())
              .setRepositoryCreatedTimestampMillis(
                  repositoryDescription.repositoryCreatedTime().toEpochMilli())
              .setOldestCommitTimestampMillis(
                  repositoryDescription.oldestPossibleCommitTime().toEpochMilli())
              .setDefaultBranchName(repositoryDescription.defaultBranchName())
              .build());
    }
  }

  private Commit mapCommitObj(CommitObj c, IndexesLogic indexesLogic) {
    Commit.Builder b =
        Commit.newBuilder()
            .setCommitId(c.id().asBytes())
            .setParentCommitId(c.tail().get(0).asBytes())
            .setMessage(c.message())
            .setCommitSequence(c.seq())
            .setCreatedTimeMicros(c.created());
    c.headers()
        .keySet()
        .forEach(h -> b.addHeadersBuilder().setName(h).addAllValues(c.headers().getAll(h)));
    c.secondaryParents().forEach(p -> b.addAdditionalParents(p.asBytes()));
    indexesLogic
        .commitOperations(c)
        .forEach(
            op -> {
              CommitOp content = op.content();
              Operation.Builder opBuilder = b.addOperationsBuilder().setPayload(content.payload());

              opBuilder.addContentKey(op.key().rawString());

              ObjId valueId = content.value();
              if (valueId != null) {
                try {
                  ContentValueObj value =
                      requireNonNull(exporter.persist())
                          .fetchTypedObj(valueId, VALUE, ContentValueObj.class);
                  Content modelContent =
                      exporter.storeWorker().valueFromStore((byte) content.payload(), value.data());
                  byte[] modelContentBytes =
                      exporter.objectMapper().writeValueAsBytes(modelContent);
                  opBuilder
                      .setContentId(value.contentId())
                      .setValue(ByteString.copyFrom(modelContentBytes));
                } catch (ObjNotFoundException | JsonProcessingException e) {
                  throw new RuntimeException(e);
                }
              }
              if (content.action().exists()) {
                opBuilder.setOperationType(OperationType.Put);
              } else {
                opBuilder.setOperationType(OperationType.Delete);
              }
            });
    return b.build();
  }
}
