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

import java.io.PrintWriter;
import java.nio.file.Path;
import javax.annotation.Nonnull;
import org.projectnessie.versioned.transfer.AbstractNessieExporter;
import org.projectnessie.versioned.transfer.ExportImportConstants;
import org.projectnessie.versioned.transfer.FileExporter;
import org.projectnessie.versioned.transfer.ProgressEvent;
import org.projectnessie.versioned.transfer.ProgressListener;
import org.projectnessie.versioned.transfer.ZipArchiveExporter;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import picocli.CommandLine;

@CommandLine.Command(
    name = "export",
    mixinStandardHelpOptions = true,
    description = "Exports a Nessie repository to the local file system.")
public class ExportRepository extends BaseCommand {

  static final String ZIP_FILE = "--zip-file";
  static final String TARGET_DIRECTORY = "--target-directory";
  static final String MAX_FILE_SIZE = "--max-file-size";
  static final String EXPECTED_COMMIT_COUNT = "--expected-commit-count";
  static final String OUTPUT_BUFFER_SIZE = "--output-buffer-size";

  @CommandLine.ArgGroup(multiplicity = "1")
  private Target target;

  static class Target {

    @CommandLine.Option(
        names = {"-f", ZIP_FILE},
        description =
            "The ZIP file to create with the export contents, "
                + "mutually exclusive with --target-directory.")
    private Path zipFile;

    @CommandLine.Option(
        names = {"-d", TARGET_DIRECTORY},
        description =
            "The empty (or non existing) target directory to populate with the export contents, "
                + "mutually exclusive with --zip-file.")
    private Path targetDirectory;
  }

  @CommandLine.Option(
      names = {MAX_FILE_SIZE},
      description = "Maximum size of a file in bytes inside the export.")
  private Long maxFileSize;

  @CommandLine.Option(
      names = {"-C", EXPECTED_COMMIT_COUNT},
      description =
          "Expected number of commits in the repository, defaults to "
              + ExportImportConstants.DEFAULT_EXPECTED_COMMIT_COUNT
              + ".")
  private Integer expectedCommitCount;

  @CommandLine.Option(
      names = {OUTPUT_BUFFER_SIZE},
      description =
          "Output buffer size, defaults to " + ExportImportConstants.DEFAULT_BUFFER_SIZE + ".")
  private Integer outputBufferSize;

  @Override
  public Integer call() throws Exception {
    warnOnInMemory();

    @SuppressWarnings("rawtypes")
    AbstractNessieExporter.Builder builder;
    if (target.zipFile != null) {
      builder = ZipArchiveExporter.builder().outputFile(target.zipFile);
    } else {
      builder = FileExporter.builder().targetDirectory(target.targetDirectory);
    }

    builder.databaseAdapter(databaseAdapter);
    if (maxFileSize != null) {
      builder.maxFileSize(maxFileSize);
    }
    if (expectedCommitCount != null) {
      builder.expectedCommitCount(expectedCommitCount);
    }
    if (outputBufferSize != null) {
      builder.outputBufferSize(outputBufferSize);
    }

    PrintWriter out = spec.commandLine().getOut();

    builder.progressListener(new ExportProgressListener(out)).build().exportNessieRepository();

    return 0;
  }

  /** Mostly paints dots - but also some numbers about the progress. */
  private static final class ExportProgressListener implements ProgressListener {
    private final PrintWriter out;
    private int count;
    private boolean dot;
    private ExportMeta exportMeta;

    private ExportProgressListener(PrintWriter out) {
      this.out = out;
    }

    @Override
    public void progress(@Nonnull ProgressEvent progress, ExportMeta meta) {
      switch (progress) {
        case FINISHED:
          out.printf(
              "Exported Nessie repository, %d commits into %d files, %d named references into %d files.%n",
              exportMeta.getCommitCount(),
              exportMeta.getCommitsFilesCount(),
              exportMeta.getNamedReferencesCount(),
              exportMeta.getNamedReferencesFilesCount());
          break;
        case END_META:
          this.exportMeta = meta;
          break;
        case START_COMMITS:
          out.println("Exporting commits...");
          count = 0;
          dot = false;
          break;
        case END_COMMITS:
          if (dot) {
            out.println();
          }
          out.printf("%d commits exported.%n%n", count);
          break;
        case START_NAMED_REFERENCES:
          out.println("Exporting named references...");
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
          out.printf("%d named references exported.%n%n", count);
          break;
        default:
          break;
      }
    }
  }
}
