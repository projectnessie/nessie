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
package org.projectnessie.quarkus.cli;

import com.google.protobuf.ByteString;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.transfer.AbstractNessieImporter;
import org.projectnessie.versioned.transfer.CommitLogOptimization;
import org.projectnessie.versioned.transfer.ExportImportConstants;
import org.projectnessie.versioned.transfer.FileImporter;
import org.projectnessie.versioned.transfer.ImportResult;
import org.projectnessie.versioned.transfer.ProgressEvent;
import org.projectnessie.versioned.transfer.ProgressListener;
import org.projectnessie.versioned.transfer.ZipArchiveImporter;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import picocli.CommandLine;
import picocli.CommandLine.PicocliException;

@CommandLine.Command(
    name = "import",
    mixinStandardHelpOptions = true,
    description = "Imports a Nessie repository from the local file system.")
public class ImportRepository extends BaseCommand {

  static final String PATH = "--path";

  static final String ERASE_BEFORE_IMPORT = "--erase-before-import";
  static final String NO_OPTIMIZE = "--no-optimize";
  static final String INPUT_BUFFER_SIZE = "--input-buffer-size";
  static final String COMMIT_BATCH_SIZE = "--commit-batch-size";

  @CommandLine.Option(
      names = {"-p", PATH},
      paramLabel = "<import-from>",
      required = true,
      description = {
        "The ZIP file or directory to read the export from.",
        "If this parameter refers to a file, the import will assume that it is a ZIP file, otherwise a directory."
      })
  private Path path;

  @CommandLine.Option(
      names = COMMIT_BATCH_SIZE,
      description =
          "Batch size when writing commits, defaults to "
              + ExportImportConstants.DEFAULT_COMMIT_BATCH_SIZE
              + ".")
  private Integer commitBatchSize;

  @CommandLine.Option(
      names = INPUT_BUFFER_SIZE,
      description =
          "Input buffer size, defaults to " + ExportImportConstants.DEFAULT_BUFFER_SIZE + ".")
  private Integer inputBufferSize;

  @CommandLine.Option(
      names = NO_OPTIMIZE,
      description = "Do not run commit log optimization after importing the repository.")
  private boolean noOptimize;

  @CommandLine.Option(
      names = {"-e", ERASE_BEFORE_IMPORT},
      description = "Do not run commit log optimization after importing the repository.")
  private boolean erase;

  @Override
  public Integer call() throws Exception {
    warnOnInMemory();

    @SuppressWarnings("rawtypes")
    AbstractNessieImporter.Builder builder;
    if (Files.isRegularFile(path)) {
      builder = ZipArchiveImporter.builder().sourceZipFile(path);
    } else if (Files.isDirectory(path)) {
      builder = FileImporter.builder().sourceDirectory(path);
    } else {
      throw new PicocliException(String.format("No such file or directory %s", path));
    }

    builder.databaseAdapter(databaseAdapter);
    if (commitBatchSize != null) {
      builder.commitBatchSize(commitBatchSize);
    }
    if (inputBufferSize != null) {
      builder.inputBufferSize(inputBufferSize);
    }

    PrintWriter out = spec.commandLine().getOut();

    if (!erase) {
      try (Stream<ReferenceInfo<ByteString>> refs =
              databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT);
          Stream<CommitLogEntry> commits = databaseAdapter.scanAllCommitLogEntries()) {
        AtomicReference<ReferenceInfo<ByteString>> ref = new AtomicReference<>();
        long refCount = refs.peek(ref::set).count();
        boolean hasCommit = commits.findAny().isPresent();

        if (hasCommit
            || refCount > 1
            || (refCount == 1 && !ref.get().getHash().equals(databaseAdapter.noAncestorHash()))) {
          spec.commandLine()
              .getErr()
              .println(
                  "The Nessie repository already exists and is not empty, aborting. "
                      + "Provide the "
                      + ERASE_BEFORE_IMPORT
                      + " option if you want to erase the repository.");
          return 100;
        }

        erase = true;
      }
    }

    // Perform erase + initialize to reset any non-obvious contents (global log, global
    // state, ref-log, repository description, etc).
    out.println("Erasing potentially existing repository...");
    databaseAdapter.eraseRepo();
    databaseAdapter.initializeRepo("main");
    try {
      databaseAdapter.delete(BranchName.of("main"), Optional.empty());
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw new RuntimeException(e);
    }

    ImportResult importResult =
        builder.progressListener(new ImportProgressListener(out)).build().importNessieRepository();

    out.printf(
        "Imported Nessie repository, %d commits, %d named references.%n",
        importResult.importedCommitCount(), importResult.importedReferenceCount());

    if (!noOptimize) {
      out.println("Optimizing...");

      CommitLogOptimization.builder()
          .headsAndForks(importResult.headsAndForkPoints())
          .databaseAdapter(databaseAdapter)
          .build()
          .optimize();

      out.println("Finished commit log optimization.");
    }

    return 0;
  }

  /** Mostly paints dots - but also some numbers about the progress. */
  private static final class ImportProgressListener implements ProgressListener {

    private final PrintWriter out;
    private int count;
    private boolean dot;
    private ExportMeta exportMeta;

    public ImportProgressListener(PrintWriter out) {
      this.out = out;
    }

    @Override
    public void progress(@Nonnull ProgressEvent progress, ExportMeta meta) {
      switch (progress) {
        case END_META:
          this.exportMeta = meta;
          break;
        case START_COMMITS:
          out.printf("Importing %d commits...%n", exportMeta.getCommitCount());
          count = 0;
          dot = false;
          break;
        case END_COMMITS:
          if (dot) {
            out.println();
          }
          out.printf("%d commits imported.%n%n", count);
          break;
        case START_NAMED_REFERENCES:
          out.printf("Importing %d named references...%n", exportMeta.getNamedReferencesCount());
          count = 0;
          dot = false;
          break;
        case COMMIT_WRITTEN:
        case NAMED_REFERENCE_WRITTEN:
          count++;
          if ((count % 10) == 0) {
            out.print('.');
            dot = true;
          }
          if ((count % 500) == 0) {
            out.printf(" %d%n", count);
            dot = false;
          }
          break;
        case END_NAMED_REFERENCES:
          if (dot) {
            out.println();
          }
          out.printf("%d named references imported.%n%n", count);
          break;
        default:
          break;
      }
    }
  }
}
