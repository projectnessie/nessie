/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.tools.admin.cli;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import jakarta.annotation.Nonnull;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.projectnessie.versioned.transfer.ExportImportConstants;
import org.projectnessie.versioned.transfer.ImportResult;
import org.projectnessie.versioned.transfer.NessieImporter;
import org.projectnessie.versioned.transfer.ProgressEvent;
import org.projectnessie.versioned.transfer.ProgressListener;
import org.projectnessie.versioned.transfer.files.FileImporter;
import org.projectnessie.versioned.transfer.files.ImportFileSupplier;
import org.projectnessie.versioned.transfer.files.ZipArchiveImporter;
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
      names = {"-e", ERASE_BEFORE_IMPORT},
      description = {
        "Erase an existing repository before the import is started.",
        "This will delete all previously existing Nessie data.",
        "Using this option has no effect, if the Nessie repository does not already exist."
      })
  private boolean erase;

  @Override
  public Integer call() throws Exception {
    warnOnInMemory();

    PrintWriter out = spec.commandLine().getOut();

    out.printf("Importing into a %s version store...%n", versionStoreConfig.getVersionStoreType());

    long t0 = System.nanoTime();
    try (ImportFileSupplier importFileSupplier = createImportFileSupplier()) {

      NessieImporter.Builder builder =
          NessieImporter.builder().importFileSupplier(importFileSupplier).persist(persist);
      if (commitBatchSize != null) {
        builder.commitBatchSize(commitBatchSize);
      }

      if (erase) {
        spec.commandLine().getOut().println("Erasing repository...");
        persist.erase();
      } else if (repositoryLogic(persist).repositoryExists()) {
        spec.commandLine()
            .getErr()
            .println(
                "The Nessie repository already exists and is not empty, aborting. "
                    + "Provide the "
                    + ERASE_BEFORE_IMPORT
                    + " option if you want to erase the repository.");
        return EXIT_CODE_REPO_ALREADY_EXISTS;
      }

      NessieImporter importer = builder.progressListener(new ImportProgressListener(out)).build();

      ImportResult importResult = importer.importNessieRepository();

      out.printf(
          "Imported Nessie repository, %d commits, %d named references, %d generic objects.%n",
          importResult.importedCommitCount(),
          importResult.importedReferenceCount(),
          importResult.importedGenericCount());

      return 0;
    } finally {
      out.printf("Total duration: %s%n", Duration.ofNanos(System.nanoTime() - t0));
    }
  }

  private ImportFileSupplier createImportFileSupplier() {
    ImportFileSupplier importFileSupplier;
    if (Files.isRegularFile(path)) {
      importFileSupplier = ZipArchiveImporter.builder().sourceZipFile(path).build();
    } else if (Files.isDirectory(path)) {
      FileImporter.Builder b = FileImporter.builder().sourceDirectory(path);
      if (inputBufferSize != null) {
        b.inputBufferSize(inputBufferSize);
      }
      importFileSupplier = b.build();
    } else {
      throw new PicocliException(String.format("No such file or directory %s", path));
    }
    return importFileSupplier;
  }

  /** Mostly paints dots - but also some numbers about the progress. */
  private static final class ImportProgressListener implements ProgressListener {

    private final PrintWriter out;
    private int count;
    private boolean dot;
    private ExportMeta exportMeta;
    private long timeOffset;
    private long timeLast;

    public ImportProgressListener(PrintWriter out) {
      this.out = out;
    }

    @Override
    public void progress(@Nonnull ProgressEvent progress, ExportMeta meta) {
      switch (progress) {
        case START_PREPARE:
          out.printf("Preparing repository...%n");
          dot = false;
          break;
        case END_PREPARE:
          break;
        case END_META:
          this.exportMeta = meta;
          String nessieVersion = meta.getNessieVersion();
          if (nessieVersion.isEmpty()) {
            nessieVersion = "(unknown, before 0.46)";
          }
          out.printf(
              "Export using format %s was created by Nessie version %s on %s, "
                  + "containing %d named references (in %d files), %d commits (in %d files), %d generic objects (in %d files).%n",
              meta.getVersion(),
              nessieVersion,
              Instant.ofEpochMilli(meta.getCreatedMillisEpoch()),
              meta.getNamedReferencesCount(),
              meta.getNamedReferencesFilesCount(),
              meta.getCommitCount(),
              meta.getCommitsFilesCount(),
              meta.getGenericObjCount(),
              meta.getGenericObjFilesCount());
          break;
        case START_COMMITS:
          out.printf("Importing %d commits...%n", exportMeta.getCommitCount());
          startPhase();
          break;
        case END_COMMITS:
          endPhase();
          out.printf("%d commits imported, total duration: %s.%n%n", count, totalDuration());
          break;
        case START_GENERIC:
          out.printf("Importing %d generic objects...%n", exportMeta.getGenericObjCount());
          startPhase();
          break;
        case END_GENERIC:
          endPhase();
          out.printf(
              "%d generic objects imported, total duration: %s.%n%n", count, totalDuration());
          break;
        case START_NAMED_REFERENCES:
          out.printf("Importing %d named references...%n", exportMeta.getNamedReferencesCount());
          startPhase();
          break;
        case COMMIT_WRITTEN:
        case GENERIC_WRITTEN:
        case NAMED_REFERENCE_WRITTEN:
        case FINALIZE_PROGRESS:
          count++;
          if ((count % 10) == 0) {
            out.print('.');
            out.flush();
            dot = true;
          }
          if ((count % 1000) == 0) {
            long last = timeLast;
            long now = System.nanoTime();
            timeLast = now;
            out.printf(" %d - duration: %s%n", count, Duration.ofNanos(now - last));
            dot = false;
          }
          break;
        case END_NAMED_REFERENCES:
          endPhase();
          out.printf(
              "%d named references imported, total duration: %s.%n%n", count, totalDuration());
          break;
        case START_FINALIZE:
          out.printf("Finalizing import...%n");
          startPhase();
          break;
        case END_FINALIZE:
          endPhase();
          out.printf("Import finalization finished, total duration: %s.%n%n", totalDuration());
          break;
        default:
          break;
      }
    }

    private Duration totalDuration() {
      return Duration.ofNanos(System.nanoTime() - timeOffset);
    }

    private void startPhase() {
      count = 0;
      timeLast = timeOffset = System.nanoTime();
      dot = false;
    }

    private void endPhase() {
      if (dot) {
        out.println();
      }
    }
  }
}
