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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.errorprone.annotations.MustBeClosed;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.HeadsAndForkPoints;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.ReferencesUtil;
import org.projectnessie.versioned.persist.adapter.ReferencesUtil.IdentifyHeadsAndForkPoints;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Commit;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.NamedReference;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Operation;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.OperationType;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.RefType;

final class ExportDatabaseAdapter extends ExportCommon {
  ExportDatabaseAdapter(ExportFileSupplier exportFiles, NessieExporter exporter) {
    super(exportFiles, exporter);
  }

  @Override
  ExportVersion getExportVersion() {
    return ExportVersion.V1;
  }

  @Override
  void writeRepositoryDescription() {}

  @Override
  HeadsAndForks exportCommits(ExportContext exportContext) {
    Consumer<CommitLogEntry> commitHandler =
        c -> {
          Commit commit = mapCommitLogEntry(c);
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

  private HeadsAndForkPoints scanDatabase(Consumer<CommitLogEntry> commitHandler) {
    return ReferencesUtil.forDatabaseAdapter(exporter.databaseAdapter())
        .identifyAllHeadsAndForkPoints(exporter.expectedCommitCount(), commitHandler);
  }

  private HeadsAndForkPoints scanAllReferences(Consumer<CommitLogEntry> commitHandler) {
    DatabaseAdapter databaseAdapter = requireNonNull(exporter.databaseAdapter());
    IdentifyHeadsAndForkPoints identify =
        new IdentifyHeadsAndForkPoints(
            exporter.expectedCommitCount(), databaseAdapter.getConfig().currentTimeInMicros());

    try (Stream<ReferenceInfo<ByteString>> namedRefs =
        databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      namedRefs
          .map(ReferenceInfo::getHash)
          .forEach(
              head -> {
                try {
                  scanCommitLogChain(
                      databaseAdapter.commitLog(head), identify, commitHandler, databaseAdapter);
                } catch (ReferenceNotFoundException e) {
                  throw new RuntimeException(e);
                }
              });

    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }

    return identify.finish();
  }

  private void scanCommitLogChain(
      Stream<CommitLogEntry> commits,
      IdentifyHeadsAndForkPoints identify,
      Consumer<CommitLogEntry> commitHandler,
      DatabaseAdapter databaseAdapter) {
    commits.forEach(
        commit -> {
          if (identify.handleCommit(commit)) {
            commitHandler.accept(commit);
            for (Hash hash : commit.getAdditionalParents()) {
              try {
                scanCommitLogChain(
                    databaseAdapter.commitLog(hash), identify, commitHandler, databaseAdapter);
              } catch (ReferenceNotFoundException e) {
                throw new RuntimeException(e);
              }
            }
          }
        });
  }

  @Override
  void exportReferences(ExportContext exportContext) {
    try (Stream<NamedReference> namedRefs = exportNamedReferences()) {
      namedRefs.forEach(
          namedReference -> {
            exportContext.writeNamedReference(namedReference);
            exporter.progressListener().progress(ProgressEvent.NAMED_REFERENCE_WRITTEN);
          });
    }
  }

  @MustBeClosed
  Stream<NamedReference> exportNamedReferences() {
    try {
      return requireNonNull(exporter.databaseAdapter())
          .namedRefs(GetNamedRefsParams.DEFAULT)
          .map(
              refInfo ->
                  NamedReference.newBuilder()
                      .setRefType(refType(refInfo.getNamedRef()))
                      .setName(refInfo.getNamedRef().getName())
                      .setCommitId(refInfo.getHash().asBytes())
                      .build());
    } catch (ReferenceNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private RefType refType(NamedRef namedRef) {
    if (namedRef instanceof TagName) {
      return RefType.Tag;
    }
    if (namedRef instanceof BranchName) {
      return RefType.Branch;
    }
    throw new IllegalArgumentException("Unknown named reference type " + namedRef);
  }

  Commit mapCommitLogEntry(CommitLogEntry entry) {
    Commit.Builder commitBuilder =
        Commit.newBuilder()
            .setCommitId(entry.getHash().asBytes())
            .setMetadata(entry.getMetadata())
            .setCommitSequence(entry.getCommitSeq())
            .setCreatedTimeMicros(entry.getCreatedTime())
            .setParentCommitId(entry.getParents().get(0).asBytes());
    entry.getAdditionalParents().forEach(p -> commitBuilder.addAdditionalParents(p.asBytes()));
    entry.getDeletes().forEach(d -> commitBuilder.addOperations(deleteOperationFromCommit(d)));
    entry.getPuts().forEach(p -> commitBuilder.addOperations(putOperationFromCommit(p)));
    return commitBuilder.build();
  }

  private Operation.Builder deleteOperationFromCommit(ContentKey delete) {
    return Operation.newBuilder()
        .setOperationType(OperationType.Delete)
        .addAllContentKey(delete.getElements());
  }

  private Operation.Builder putOperationFromCommit(KeyWithBytes put) {
    return Operation.newBuilder()
        .setOperationType(OperationType.Put)
        .addAllContentKey(put.getKey().getElements())
        .setContentId(put.getContentId().getId())
        .setPayload(put.getPayload())
        .setValue(contentToValue(convertToContent(put)));
  }

  private Content convertToContent(KeyWithBytes p) {
    return exporter
        .storeWorker()
        .valueFromStore(
            p.getPayload(),
            p.getValue(),
            () ->
                requireNonNull(exporter.databaseAdapter())
                    .globalContent(p.getContentId())
                    .map(ContentIdAndBytes::getValue)
                    .orElse(null));
  }

  private ByteString contentToValue(Content content) {
    try {
      return ByteString.copyFromUtf8(exporter.objectMapper().writeValueAsString(content));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
