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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;
import org.projectnessie.versioned.transfer.ExportImportConstants;
import org.projectnessie.versioned.transfer.NessieExporter;
import org.projectnessie.versioned.transfer.ProgressEvent;
import org.projectnessie.versioned.transfer.ProgressListener;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.files.FileExporter;
import org.projectnessie.versioned.transfer.files.ZipArchiveExporter;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import picocli.CommandLine;
import picocli.CommandLine.PicocliException;

@CommandLine.Command(
    name = "export",
    mixinStandardHelpOptions = true,
    description = "Exports a Nessie repository to the local file system.")
public class ExportRepository extends BaseCommand {

  static final String PATH = "--path";
  static final String OUTPUT_FORMAT = "--output-format";
  static final String MAX_FILE_SIZE = "--max-file-size";
  static final String EXPECTED_COMMIT_COUNT = "--expected-commit-count";
  static final String OUTPUT_BUFFER_SIZE = "--output-buffer-size";
  static final String SINGLE_BRANCH = "--single-branch-current-content";
  static final String CONTENT_BATCH_SIZE = "--content-batch-size";
  static final String COMMIT_BATCH_SIZE = "--commit-batch-size";
  static final String EXPORT_VERSION = "--export-version";
  static final String GENERIC_OBJECT_RESOLVERS = "--object-resolvers";

  enum Format {
    ZIP,
    DIRECTORY
  }

  @CommandLine.Option(
      names = {"-p", PATH},
      required = true,
      paramLabel = "<export-to>",
      description = "The ZIP file or directory to create with the export contents.")
  private Path path;

  @CommandLine.Option(
      names = {"-F", OUTPUT_FORMAT},
      paramLabel = "<output-format>",
      description = {
        "Explicitly define the output format to use to the export.",
        "If not specified, the implementation chooses the ZIP export, if "
            + PATH
            + " ends in .zip, otherwise will use the directory output format.",
        "Possible values: ${COMPLETION-CANDIDATES}"
      })
  private Format outputFormat;

  @CommandLine.Option(
      names = MAX_FILE_SIZE,
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
      names = OUTPUT_BUFFER_SIZE,
      description =
          "Output buffer size, defaults to " + ExportImportConstants.DEFAULT_BUFFER_SIZE + ".")
  private Integer outputBufferSize;

  @CommandLine.Option(
      names = {"--full-scan"},
      description = {
        "Export all commits, including those that are no longer reachable any named reference."
            + "Using this option is _not_ recommended."
      })
  private boolean fullScan;

  @CommandLine.Option(
      names = {SINGLE_BRANCH},
      paramLabel = "<branch-name>",
      description = {"Export only the most recent contents from the specified branch."})
  private String contentsFromBranch;

  @CommandLine.Option(
      names = {CONTENT_BATCH_SIZE},
      paramLabel = "<number>",
      description = {
        "Group the specified number of content objects into each commit at export time. "
            + "This option is ignored unless "
            + SINGLE_BRANCH
            + " is set. The default value is 100."
      })
  private Integer contentsBatchSize;

  @CommandLine.Option(
      names = COMMIT_BATCH_SIZE,
      description =
          "Batch size when reading commits and their associated contents, defaults to "
              + ExportImportConstants.DEFAULT_COMMIT_BATCH_SIZE
              + ".")
  private Integer commitBatchSize;

  @CommandLine.Option(
      names = EXPORT_VERSION,
      defaultValue = "" + ExportImportConstants.DEFAULT_EXPORT_VERSION,
      description =
          "The export version, defaults to " + ExportImportConstants.DEFAULT_EXPORT_VERSION + ".")
  private int exportVersion;

  @CommandLine.Option(
      names = GENERIC_OBJECT_RESOLVERS,
      description = {
        "Additional jars that provide `TransferRelatedObjects` implementations.",
        "Jars can be provided as file paths or as URLs."
      })
  private List<String> genericObjectResolvers = new ArrayList<>();

  @Override
  public Integer call() throws Exception {
    if (!repositoryLogic(persist).repositoryExists()) {
      spec.commandLine().getErr().println("Nessie repository does not exist");
      return EXIT_CODE_REPO_DOES_NOT_EXIST;
    }

    return export(
        b -> {
          b.persist(persist);
          b.versionStore(new VersionStoreImpl(persist));
        });
  }

  Integer export(Consumer<NessieExporter.Builder> builderConsumer) throws Exception {
    warnOnInMemory();

    spec.commandLine()
        .getOut()
        .printf(
            "Exporting from a %s version store using export version %s...%n",
            versionStoreConfig.getVersionStoreType(), exportVersion);

    try (ExportFileSupplier exportFileSupplier = createExportFileSupplier()) {
      NessieExporter.Builder builder =
          NessieExporter.builder()
              .exportFileSupplier(exportFileSupplier)
              .fullScan(fullScan)
              .contentsFromBranch(contentsFromBranch)
              .exportVersion(exportVersion);
      builderConsumer.accept(builder);
      if (maxFileSize != null) {
        builder.maxFileSize(maxFileSize);
      }
      if (expectedCommitCount != null) {
        builder.expectedCommitCount(expectedCommitCount);
      }
      if (outputBufferSize != null) {
        builder.outputBufferSize(outputBufferSize);
      }
      if (contentsBatchSize != null) {
        builder.contentsBatchSize(contentsBatchSize);
      }
      if (commitBatchSize != null) {
        builder.commitBatchSize(commitBatchSize);
      }

      for (String resolver : genericObjectResolvers) {
        URL url;
        try {
          url = URI.create(resolver).toURL();
        } catch (MalformedURLException e) {
          url = Paths.get(resolver).toUri().toURL();
        }
        builder.addGenericObjectResolvers(url);
      }

      PrintWriter out = spec.commandLine().getOut();

      builder.progressListener(new ExportProgressListener(out)).build().exportNessieRepository();

      return 0;
    }
  }

  private ExportFileSupplier createExportFileSupplier() {
    return switch (exportFormat()) {
      case ZIP -> {
        if (Files.isRegularFile(path)) {
          throw new PicocliException(
              String.format(
                  "Export file %s already exists, please delete it first, if you want to overwrite it.",
                  path));
        }
        yield ZipArchiveExporter.builder().outputFile(path).build();
      }
      case DIRECTORY -> {
        if (Files.isRegularFile(path)) {
          throw new PicocliException(
              String.format("%s refers to a file, but export type is %s.", path, Format.DIRECTORY));
        }
        yield FileExporter.builder().targetDirectory(path).build();
      }
      default -> throw new IllegalStateException(exportFormat().toString());
    };
  }

  private Format exportFormat() {
    if (outputFormat != null) {
      return outputFormat;
    }

    String fileName = path.getFileName().toString().toLowerCase(Locale.ROOT);
    return fileName.endsWith(".zip") ? Format.ZIP : Format.DIRECTORY;
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
              "Exported Nessie repository, %d commits into %d files, %d named references into %d files, %d generic objects into %d files.%n",
              exportMeta.getCommitCount(),
              exportMeta.getCommitsFilesCount(),
              exportMeta.getNamedReferencesCount(),
              exportMeta.getNamedReferencesFilesCount(),
              exportMeta.getGenericObjCount(),
              exportMeta.getGenericObjFilesCount());
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
        case START_GENERIC:
          out.println("Exporting generic objects...");
          count = 0;
          dot = false;
          break;
        case END_GENERIC:
          if (dot) {
            out.println();
          }
          out.printf("%d generic objects exported.%n%n", count);
          break;
        case START_NAMED_REFERENCES:
          out.println("Exporting named references...");
          count = 0;
          dot = false;
          break;
        case COMMIT_WRITTEN:
        // GENERIC_WRITTEN isn't actually "produced", but adding the value here for posterity.
        case GENERIC_WRITTEN:
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
        case START_FINALIZE:
          out.printf("Finalizing export...%n");
          dot = false;
          break;
        case END_FINALIZE:
          if (dot) {
            out.println();
          }
          out.printf("Export finalization finished.%n%n");
          break;
        default:
          break;
      }
    }
  }
}
