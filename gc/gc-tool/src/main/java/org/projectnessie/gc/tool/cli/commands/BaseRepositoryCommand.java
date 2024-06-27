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

import static java.lang.String.format;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetNotFoundException;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.expire.Expire;
import org.projectnessie.gc.expire.ExpireParameters;
import org.projectnessie.gc.expire.local.DefaultLocalExpire;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.iceberg.IcebergContentToContentReference;
import org.projectnessie.gc.iceberg.IcebergContentToFiles;
import org.projectnessie.gc.iceberg.IcebergContentTypeFilter;
import org.projectnessie.gc.iceberg.files.IcebergFiles;
import org.projectnessie.gc.identify.IdentifyLiveContents;
import org.projectnessie.gc.identify.PerRefCutoffPolicySupplier;
import org.projectnessie.gc.repository.RepositoryConnector;
import org.projectnessie.gc.tool.cli.Closeables;
import org.projectnessie.gc.tool.cli.options.IcebergOptions;
import org.projectnessie.gc.tool.cli.options.LiveContentSetsStorageOptions;
import org.projectnessie.gc.tool.cli.options.MarkOptions;
import org.projectnessie.gc.tool.cli.options.SweepOptions;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Model.CommandSpec;

public abstract class BaseRepositoryCommand extends BaseCommand {

  @CommandLine.Mixin LiveContentSetsStorageOptions liveContentSetsStorageOptions;

  @CommandLine.Spec CommandSpec commandSpec;

  @CommandLine.Option(
      names = "--time-zone",
      description = {"Time zone ID used to show timestamps.", "Defaults to system time zone."})
  ZoneId zoneId;

  protected PrintWriter out;

  protected ZoneId zoneId() {
    return zoneId != null ? zoneId : ZoneId.systemDefault();
  }

  @Override
  protected Integer call(Closeables closeables) throws Exception {
    this.out = commandSpec.commandLine().getOut();

    preValidate();

    LiveContentSetsRepository liveContentSetsRepository =
        liveContentSetsStorageOptions.createLiveContentSetsRepository(closeables);

    return call(closeables, liveContentSetsRepository);
  }

  protected void preValidate() {
    liveContentSetsStorageOptions.assertNotInMemory(commandSpec);
  }

  protected abstract Integer call(
      Closeables closeables, LiveContentSetsRepository liveContentSetsRepository);

  protected String instantAsString(Instant instant) {
    return instant != null
        ? DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
            instant.atZone(zoneId()).truncatedTo(ChronoUnit.SECONDS))
        : "";
  }

  protected static String asString(Object o) {
    return o != null ? o.toString() : "";
  }

  protected void listLiveContentSets(
      CommandSpec commandSpec, Stream<LiveContentSet> liveContentSetStream) {
    String pattern = "%-36s  %-22s  %-25s %-25s %-25s %-25s %s";

    out.println(Ansi.AUTO.text("@|italic Timestamps shown in " + zoneId() + "|@"));

    out.println(
        Ansi.AUTO.text(
            format(
                "@|bold,underline " + pattern + "|@",
                "ID",
                "Status",
                "Created",
                "Identify complete",
                "Expiry started",
                "Expiry finished",
                "Error")));

    liveContentSetStream.forEach(
        liveContentSet ->
            out.println(
                format(
                    pattern,
                    liveContentSet.id(),
                    liveContentSet.status(),
                    instantAsString(liveContentSet.created()),
                    instantAsString(liveContentSet.identifyCompleted()),
                    instantAsString(liveContentSet.expiryStarted()),
                    instantAsString(liveContentSet.expiryCompleted()),
                    asString(liveContentSet.errorMessage()))));
  }

  protected LiveContentSet identify(
      Closeables closeables,
      LiveContentSetsRepository liveContentSetsRepository,
      MarkOptions markOptions,
      CommandSpec commandSpec) {

    // Sanity check, whether that file can be written
    Path liveSetIdFile = markOptions.getLiveSetIdFile();
    if (liveSetIdFile != null) {
      try {
        Files.createDirectories(liveSetIdFile.getParent());
      } catch (IOException e) {
        throw new ExecutionException(
            commandSpec.commandLine(),
            "Cannot create parent directory for live-set-id file " + liveSetIdFile,
            e);
      }
      try {
        Files.write(liveSetIdFile, new byte[0]);
      } catch (IOException e) {
        throw new ExecutionException(
            commandSpec.commandLine(), "Cannot write live-set-id to " + liveSetIdFile, e);
      }
    }

    PerRefCutoffPolicySupplier perRefCutoffPolicySupplier =
        markOptions.createPerRefCutoffPolicySupplier();
    RepositoryConnector repositoryConnector =
        markOptions.getNessie().createRepositoryConnector(closeables);

    IdentifyLiveContents identify =
        IdentifyLiveContents.builder()
            .liveContentSetsRepository(liveContentSetsRepository)
            .contentTypeFilter(IcebergContentTypeFilter.INSTANCE)
            .cutOffPolicySupplier(perRefCutoffPolicySupplier)
            .repositoryConnector(repositoryConnector)
            .contentToContentReference(IcebergContentToContentReference.INSTANCE)
            .parallelism(markOptions.getParallelism())
            .build();

    UUID liveContentSetId = identify.identifyLiveContents();

    LiveContentSet liveContentSet;
    try {
      liveContentSet = liveContentSetsRepository.getLiveContentSet(liveContentSetId);
    } catch (LiveContentSetNotFoundException e) {
      throw new RuntimeException(e);
    }

    String msg =
        format(
            "Finished Nessie-GC identify phase finished with status %s after %s, live-content-set ID is %s.",
            liveContentSet.status(),
            Duration.between(liveContentSet.created(), liveContentSet.identifyCompleted()),
            liveContentSetId);

    if (liveSetIdFile != null) {
      try {
        Files.write(liveSetIdFile, liveContentSetId.toString().getBytes(StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new ExecutionException(
            commandSpec.commandLine(), "Failed to write live-set-id to " + liveSetIdFile, e);
      }
    }

    if (liveContentSet.status() != LiveContentSet.Status.IDENTIFY_SUCCESS) {
      throw new ExecutionException(commandSpec.commandLine(), msg);
    }

    out.println(Ansi.AUTO.text("@|bold,green " + msg + "|@"));
    return liveContentSet;
  }

  protected int expire(
      LiveContentSetsRepository liveContentSetsRepository,
      LiveContentSet liveContentSet,
      SweepOptions sweepOptions,
      IcebergOptions icebergOptions,
      CommandSpec commandSpec) {
    if (liveContentSet.status() != LiveContentSet.Status.IDENTIFY_SUCCESS) {
      throw new ExecutionException(
          commandSpec.commandLine(),
          "Expected live-set to have status IDENTIFY_SUCCESS, but status is "
              + liveContentSet.status());
    }

    try (IcebergFiles icebergFiles = createIcebergFiles(icebergOptions)) {
      Instant maxFileModificationTime = sweepOptions.getMaxFileModificationTime();
      if (maxFileModificationTime == null) {
        maxFileModificationTime = liveContentSet.created();
      }

      FileDeleter fileDeleter =
          sweepOptions.isDeferDeletes() ? liveContentSet.fileDeleter() : icebergFiles;

      ExpireParameters expireParameters =
          ExpireParameters.builder()
              .fileDeleter(fileDeleter)
              .filesLister(icebergFiles)
              .contentToFiles(
                  IcebergContentToFiles.builder().io(icebergFiles.resolvingFileIO()).build())
              .liveContentSet(liveContentSet)
              .maxFileModificationTime(maxFileModificationTime)
              .falsePositiveProbability(sweepOptions.getFalsePositiveProbability())
              .expectedFileCount(sweepOptions.getExpectedFileCount())
              .allowedFalsePositiveProbability(sweepOptions.getAllowedFalsePositiveProbability())
              .build();

      Expire expire =
          DefaultLocalExpire.builder()
              .parallelism(sweepOptions.getParallelism())
              .expireParameters(expireParameters)
              .build();
      DeleteSummary summary = expire.expire();

      // refresh liveContentSet to get current status
      try {
        liveContentSet = liveContentSetsRepository.getLiveContentSet(liveContentSet.id());
      } catch (LiveContentSetNotFoundException e) {
        throw new RuntimeException(e);
      }

      String msg =
          format(
              "Nessie-GC sweep phase for live-content-set %s finished with status %s after %s, deleted %d files, %d files could not be deleted.",
              liveContentSet.id(),
              liveContentSet.status(),
              Duration.between(liveContentSet.created(), liveContentSet.identifyCompleted()),
              summary.deleted(),
              summary.failures());

      if (liveContentSet.status() != LiveContentSet.Status.EXPIRY_SUCCESS) {
        throw new ExecutionException(commandSpec.commandLine(), msg);
      }

      commandSpec.commandLine().getOut().println(Ansi.AUTO.text("@|bold,green " + msg + "|@"));

      return summary.failures() == 0L ? 0 : 1;
    }
  }

  protected IcebergFiles createIcebergFiles(IcebergOptions icebergOptions) {
    Configuration conf = new Configuration();
    icebergOptions.getHadoopConf().forEach(conf::set);

    return IcebergFiles.builder()
        .properties(icebergOptions.getIcebergProperties())
        .hadoopConfiguration(conf)
        .build();
  }
}
