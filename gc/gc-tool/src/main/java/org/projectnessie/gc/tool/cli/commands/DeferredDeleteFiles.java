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
package org.projectnessie.gc.tool.cli.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.gc.tool.cli.options.EnvironmentDefaultProvider;
import org.projectnessie.gc.tool.cli.options.IcebergOptions;
import org.projectnessie.storage.uri.StorageUri;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Help.Ansi;

@CommandLine.Command(
    name = "deferred-deletes",
    mixinStandardHelpOptions = true,
    defaultValueProvider = EnvironmentDefaultProvider.class,
    description =
        "Delete files collected as deferred deletes, "
            + "must not be used with the in-memory contents-storage.")
public class DeferredDeleteFiles extends BaseLiveSetCommand {

  @CommandLine.Mixin IcebergOptions icebergOptions;

  @Override
  protected Integer call(
      LiveContentSet liveContentSet, LiveContentSetsRepository liveContentSetsRepository) {
    if (liveContentSet.status() != LiveContentSet.Status.EXPIRY_SUCCESS) {
      throw new ExecutionException(
          commandSpec.commandLine(),
          "Expected live-set to have status EXPIRY_SUCCESS, but status is "
              + liveContentSet.status());
    }

    out.println("About to execute deferred-file-deletions of live content set:");
    listLiveContentSets(commandSpec, Stream.of(liveContentSet));

    out.println();

    DeleteSummary total;
    try (IcebergFiles icebergFiles = createIcebergFiles(icebergOptions);
        BatchDelete batchDelete =
            new BatchDelete(
                100,
                icebergFiles,
                (path, summary) ->
                    out.println(
                        Ansi.AUTO.text(
                            "@|italic,yellow "
                                + String.format(
                                    "Deleted %d files from %s.", summary.deleted(), path)
                                + "|@")))) {
      try (Stream<FileReference> deferredDeletions = liveContentSet.fetchFileDeletions()) {
        deferredDeletions.forEach(batchDelete::add);
      } finally {
        batchDelete.flush();
      }

      total = batchDelete.getSummary();
    }

    out.println();
    out.println(
        Ansi.AUTO.text(
            "@|bold,green "
                + String.format(
                    "Deferred deletion finished: %d files deleted, %d delete failures.%n",
                    total.deleted(), total.failures())
                + "|@"));

    return total.failures() == 0L ? 0 : 1;
  }

  static final class BatchDelete implements AutoCloseable {
    private final int bufferSize;
    private final FileDeleter deleter;
    private final List<FileReference> buffer;
    private final BiConsumer<StorageUri, DeleteSummary> progress;
    private StorageUri currentBase = StorageUri.of("nope://nope");
    private DeleteSummary summary = DeleteSummary.EMPTY;

    BatchDelete(
        int bufferSize, FileDeleter deleter, BiConsumer<StorageUri, DeleteSummary> progress) {
      this.bufferSize = bufferSize;
      this.deleter = deleter;
      this.progress = progress;
      buffer = new ArrayList<>(bufferSize);
    }

    void add(FileReference f) {
      if (!f.base().equals(currentBase)) {
        flush();
        currentBase = f.base();
      }
      buffer.add(f);
      if (buffer.size() == bufferSize) {
        flush();
      }
    }

    private void flush() {
      if (!buffer.isEmpty()) {
        DeleteSummary delete = deleter.deleteMultiple(currentBase, buffer.stream());
        progress.accept(currentBase, delete);
        summary = summary.add(delete);
        buffer.clear();
      }
    }

    DeleteSummary getSummary() {
      return summary;
    }

    @Override
    public void close() {
      flush();
    }
  }
}
