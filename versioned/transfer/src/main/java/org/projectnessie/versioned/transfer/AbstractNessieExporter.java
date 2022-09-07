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

import static org.projectnessie.versioned.transfer.ExportImportConstants.DEFAULT_BUFFER_SIZE;
import static org.projectnessie.versioned.transfer.ExportImportConstants.EXPORT_METADATA;
import static org.projectnessie.versioned.transfer.ExportImportConstants.HEADS_AND_FORKS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.errorprone.annotations.MustBeClosed;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentIdAndBytes;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.HeadsAndForkPoints;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.ReferencesUtil;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Commit;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.NamedReference;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.Operation;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.OperationType;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.RefType;

public abstract class AbstractNessieExporter {

  public static final String NAMED_REFS_PREFIX = "named-refs";
  public static final String COMMITS_PREFIX = "commits";

  @SuppressWarnings("UnusedReturnValue")
  public interface Builder<B extends Builder<B, T>, T extends AbstractNessieExporter> {
    /** Mandatory, specify the {@code DatabaseAdapter} to use. */
    B databaseAdapter(DatabaseAdapter databaseAdapter);

    /** Optional, specify a custom {@link ObjectMapper}. */
    B objectMapper(ObjectMapper objectMapper);

    /** Optional, specify a custom {@link StoreWorker}. */
    B storeWorker(StoreWorker storeWorker);

    /**
     * Optional, specify a different buffer size than the default value of {@value
     * ExportImportConstants#DEFAULT_BUFFER_SIZE}.
     */
    B outputBufferSize(int outputBufferSize);

    /**
     * Maximum size of a file containing commits or named references. Default is to write everything
     * into a single file.
     */
    B maxFileSize(long maxFileSize);

    /**
     * The expected number of commits in the Nessie repository, default is {@value
     * ExportImportConstants#DEFAULT_EXPECTED_COMMIT_COUNT}.
     */
    B expectedCommitCount(int expectedCommitCount);

    B progressListener(ProgressListener progressListener);

    T build();
  }

  abstract DatabaseAdapter databaseAdapter();

  @Value.Default
  ObjectMapper objectMapper() {
    return new ObjectMapper();
  }

  @Value.Default
  StoreWorker storeWorker() {
    return DefaultStoreWorker.instance();
  }

  @Value.Default
  int outputBufferSize() {
    return DEFAULT_BUFFER_SIZE;
  }

  @Value.Default
  long maxFileSize() {
    return Long.MAX_VALUE;
  }

  @Value.Default
  int expectedCommitCount() {
    return ExportImportConstants.DEFAULT_EXPECTED_COMMIT_COUNT;
  }

  protected abstract void preValidate() throws IOException;

  protected abstract OutputStream newFileOutput(String fileName) throws IOException;

  @Value.Default
  ProgressListener progressListener() {
    return (x, y) -> {};
  }

  public ExportMeta exportNessieRepository() throws IOException {
    preValidate();

    ExportContext exportContext =
        new ExportContext(
            ExportMeta.newBuilder()
                .setCreatedMillisEpoch(databaseAdapter().getConfig().getClock().millis())
                .setVersion(ExportVersion.V1));
    try {
      progressListener().progress(ProgressEvent.STARTED);

      progressListener().progress(ProgressEvent.START_NAMED_REFERENCES);
      try (Stream<NamedReference> namedRefs = exportNamedReferences()) {
        namedRefs.forEach(
            namedReference -> {
              exportContext.writeNamedReference(namedReference);
              progressListener().progress(ProgressEvent.NAMED_REFERENCE_WRITTEN);
            });
      }
      exportContext.namedReferenceOutput.finishCurrentFile();
      progressListener().progress(ProgressEvent.END_NAMED_REFERENCES);

      progressListener().progress(ProgressEvent.START_COMMITS);
      Consumer<CommitLogEntry> commitHandler =
          c -> {
            Commit commit = mapCommitLogEntry(c);
            exportContext.writeCommit(commit);
            progressListener().progress(ProgressEvent.COMMIT_WRITTEN);
          };

      HeadsAndForkPoints headsAndForks =
          ReferencesUtil.forDatabaseAdapter(databaseAdapter())
              .identifyAllHeadsAndForkPoints(expectedCommitCount(), commitHandler);
      exportContext.commitOutput.finishCurrentFile();
      progressListener().progress(ProgressEvent.END_COMMITS);

      HeadsAndForks.Builder hf =
          HeadsAndForks.newBuilder()
              .setScanStartedAtInMicros(headsAndForks.getScanStartedAtInMicros());
      headsAndForks.getHeads().forEach(h -> hf.addHeads(h.asBytes()));
      headsAndForks.getForkPoints().forEach(h -> hf.addForkPoints(h.asBytes()));
      try (OutputStream output = newFileOutput(HEADS_AND_FORKS)) {
        hf.build().writeTo(output);
      }

      progressListener().progress(ProgressEvent.START_META);
      ExportMeta meta = exportContext.finish();
      try (OutputStream output = newFileOutput(EXPORT_METADATA)) {
        meta.writeTo(output);
      }
      progressListener().progress(ProgressEvent.END_META, meta);

      progressListener().progress(ProgressEvent.FINISHED);

      return meta;
    } finally {
      // Ensure that all output streams are closed.
      exportContext.closeSilently();
    }
  }

  @MustBeClosed
  Stream<NamedReference> exportNamedReferences() {
    try {
      return databaseAdapter()
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

  protected Operation.Builder deleteOperationFromCommit(Key delete) {
    return Operation.newBuilder()
        .setOperationType(OperationType.Delete)
        .addAllContentKey(delete.getElements());
  }

  protected Operation.Builder putOperationFromCommit(KeyWithBytes put) {
    return Operation.newBuilder()
        .setOperationType(OperationType.Put)
        .addAllContentKey(put.getKey().getElements())
        .setContentId(put.getContentId().getId())
        .setPayload(put.getPayload())
        .setValue(contentToValue(convertToContent(put)));
  }

  protected Content convertToContent(KeyWithBytes p) {
    return storeWorker()
        .valueFromStore(
            p.getPayload(),
            p.getValue(),
            () ->
                databaseAdapter()
                    .globalContent(p.getContentId())
                    .map(ContentIdAndBytes::getValue)
                    .orElse(null),
            databaseAdapter()::mapToAttachment);
  }

  protected ByteString contentToValue(Content content) {
    try {
      return ByteString.copyFromUtf8(objectMapper().writeValueAsString(content));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private final class ExportContext {

    private final ExportMeta.Builder exportMeta;

    private final SizeLimitedOutput namedReferenceOutput;
    private final SizeLimitedOutput commitOutput;

    ExportContext(ExportMeta.Builder exportMeta) {
      this.exportMeta = exportMeta;
      namedReferenceOutput =
          new SizeLimitedOutput(
              NAMED_REFS_PREFIX,
              exportMeta::addNamedReferencesFiles,
              exportMeta::setNamedReferencesCount);
      commitOutput =
          new SizeLimitedOutput(
              COMMITS_PREFIX, exportMeta::addCommitsFiles, exportMeta::setCommitCount);
    }

    void writeNamedReference(NamedReference namedReference) {
      namedReferenceOutput.writeEntity(namedReference);
    }

    void writeCommit(Commit commit) {
      commitOutput.writeEntity(commit);
    }

    ExportMeta finish() throws IOException {
      namedReferenceOutput.finish();
      commitOutput.finish();
      return exportMeta.build();
    }

    void closeSilently() {
      namedReferenceOutput.closeSilently();
      commitOutput.closeSilently();
    }
  }

  private final class SizeLimitedOutput {
    private final String fileNamePrefix;
    private final Consumer<String> newFileName;
    private final LongConsumer finalEntityCount;
    private int fileNum;
    long entityCount;
    private long currentFileSize;

    private OutputStream output;

    SizeLimitedOutput(
        String fileNamePrefix, Consumer<String> newFileName, LongConsumer finalEntityCount) {
      this.fileNamePrefix = fileNamePrefix;
      this.newFileName = newFileName;
      this.finalEntityCount = finalEntityCount;
    }

    void writeEntity(AbstractMessage message) {
      try {
        int size = message.getSerializedSize();
        currentFileSize += size;
        if (maxFileSize() != Long.MAX_VALUE) {
          if (currentFileSize > maxFileSize()) {
            currentFileSize = 0L;
            finishCurrentFile();
          }
        }
        if (output == null) {
          fileNum++;
          String fileName = String.format("%s-%08d", fileNamePrefix, fileNum);
          output = new BufferedOutputStream(newFileOutput(fileName), outputBufferSize());
          newFileName.accept(fileName);
        }
        message.writeDelimitedTo(output);
        entityCount++;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    void finish() throws IOException {
      finishCurrentFile();
      finalEntityCount.accept(entityCount);
    }

    private void finishCurrentFile() throws IOException {
      if (output != null) {
        try {
          output.flush();
          output.close();
        } finally {
          output = null;
        }
      }
    }

    public void closeSilently() {
      try {
        finishCurrentFile();
      } catch (IOException ignore) {
        // ... silent
      }
    }
  }
}
