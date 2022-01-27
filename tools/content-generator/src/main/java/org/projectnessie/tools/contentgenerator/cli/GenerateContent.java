/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.tools.contentgenerator.cli;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.validation.constraints.Min;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Tag;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

@Command(name = "generate", mixinStandardHelpOptions = true, description = "Generate commits")
public class GenerateContent extends AbstractCommand {

  @Option(
      names = {"-b", "--num-branches"},
      defaultValue = "1",
      description = "Number of branches to use.")
  private int branchCount;

  @Option(
      names = {"-T", "--tag-probability"},
      defaultValue = "0",
      description = "Probability to create a new tag off the last commit.")
  private double newTagProbability;

  @Option(
      names = {"-D", "--default-branch"},
      description =
          "Name of the default branch, uses the server's default branch if not specified.")
  private String defaultBranchName;

  @Min(value = 1, message = "Must create at least one commit.")
  @Option(
      names = {"-n", "--num-commits"},
      required = true,
      defaultValue = "100",
      description = "Number of commits to create.")
  private int numCommits;

  @Option(
      names = {"-d", "--duration"},
      description =
          "Runtime duration, equally distributed among the number of commits to create. "
              + "See java.time.Duration for argument format details.")
  private Duration runtimeDuration;

  @Min(value = 1, message = "Must use at least one table (content-key).")
  @Option(
      names = {"-t", "--num-tables"},
      defaultValue = "1",
      description = "Number of table names, each commit chooses a random table (content-key).")
  private int numTables;

  @Option(
      names = "--type",
      defaultValue = "ICEBERG_TABLE",
      description =
          "Content-types to generate. Defaults to ICEBERG_TABLE. Possible values: "
              + "ICEBERG_TABLE, ICEBERG_VIEW, DELTA_LAKE_TABLE")
  private Content.Type contentType;

  @Spec private CommandSpec spec;

  @Override
  public void execute() throws BaseNessieClientServerException {
    NessieApiV1 api = createNessieApiInstance();

    if (runtimeDuration != null) {
      if (runtimeDuration.isZero() || runtimeDuration.isNegative()) {
        throw new ParameterException(
            spec.commandLine(), "Duration must be absent to greater than zero.");
      }
    }

    Duration perCommitDuration =
        Optional.ofNullable(runtimeDuration).orElse(Duration.ZERO).dividedBy(numCommits);

    ThreadLocalRandom random = ThreadLocalRandom.current();

    String runStartTime =
        DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").format(LocalDateTime.now());

    List<ContentKey> tableNames =
        IntStream.range(0, numTables)
            .mapToObj(
                i ->
                    ContentKey.of(
                        String.format("create-contents-%s", runStartTime),
                        "contents",
                        Integer.toString(i)))
            .collect(Collectors.toList());

    Branch defaultBranch;
    if (defaultBranchName == null) {
      // Use the server's default branch.
      defaultBranch = api.getDefaultBranch();
    } else {
      // Use the specified default branch.
      try {
        defaultBranch = (Branch) api.getReference().refName(defaultBranchName).get();
      } catch (NessieReferenceNotFoundException e) {
        // Create branch if it does not exist.
        defaultBranch = api.getDefaultBranch();
        defaultBranch =
            (Branch)
                api.createReference()
                    .reference(Branch.of(defaultBranchName, defaultBranch.getHash()))
                    .sourceRefName(defaultBranch.getName())
                    .create();
      }
    }

    List<String> branches = new ArrayList<>();
    branches.add(defaultBranch.getName());
    while (branches.size() < branchCount) {
      // Create a new branch
      String newBranchName = "branch-" + runStartTime + "_" + (branches.size() - 1);
      Branch branch = Branch.of(newBranchName, defaultBranch.getHash());
      spec.commandLine()
          .getOut()
          .printf(
              "Creating branch '%s' from '%s' at %s%n",
              branch.getName(), defaultBranch.getName(), branch.getHash());
      api.createReference().reference(branch).sourceRefName(defaultBranch.getName()).create();
      branches.add(newBranchName);
    }

    spec.commandLine().getOut().printf("Starting contents generation, %d commits...%n", numCommits);

    for (int i = 0; i < numCommits; i++) {
      // Choose a random branch to commit to
      String branchName = branches.get(random.nextInt(branches.size()));

      Branch commitToBranch = (Branch) api.getReference().refName(branchName).get();

      ContentKey tableName = tableNames.get(random.nextInt(tableNames.size()));

      Content tableContents =
          api.getContent().refName(branchName).key(tableName).get().get(tableName);
      Content newContents = createContents(tableContents, random);

      spec.commandLine()
          .getOut()
          .printf(
              "Committing content-key '%s' to branch '%s' at %s%n",
              tableName, commitToBranch.getName(), commitToBranch.getHash());

      CommitMultipleOperationsBuilder commit =
          api.commitMultipleOperations()
              .branch(commitToBranch)
              .commitMeta(
                  CommitMeta.builder()
                      .message(
                          String.format(
                              "%s table %s on %s, commit #%d of %d",
                              tableContents != null ? "Update" : "Create",
                              tableName,
                              branchName,
                              i,
                              numCommits))
                      .author(System.getProperty("user.name"))
                      .authorTime(Instant.now())
                      .build());
      if (newContents instanceof IcebergTable || newContents instanceof IcebergView) {
        commit.operation(Put.of(tableName, newContents, tableContents));
      } else {
        commit.operation(Put.of(tableName, newContents));
      }
      Branch newHead = commit.commit();

      if (random.nextDouble() < newTagProbability) {
        Tag tag = Tag.of("new-tag-" + random.nextLong(), newHead.getHash());
        spec.commandLine()
            .getOut()
            .printf(
                "Creating tag '%s' from '%s' at %s%n", tag.getName(), branchName, tag.getHash());
        api.createReference().reference(tag).sourceRefName(branchName).create();
      }

      try {
        TimeUnit.NANOSECONDS.sleep(perCommitDuration.toNanos());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    spec.commandLine().getOut().printf("Done creating contents.%n");
  }

  private Content createContents(Content currentContents, ThreadLocalRandom random) {
    switch (contentType) {
      case ICEBERG_TABLE:
        ImmutableIcebergTable.Builder icebergBuilder =
            ImmutableIcebergTable.builder()
                .snapshotId(random.nextLong())
                .schemaId(random.nextInt())
                .specId(random.nextInt())
                .sortOrderId(random.nextInt())
                .metadataLocation("metadata " + random.nextLong());
        if (currentContents != null) {
          icebergBuilder.id(currentContents.getId());
        }
        return icebergBuilder.build();
      case DELTA_LAKE_TABLE:
        ImmutableDeltaLakeTable.Builder deltaBuilder =
            ImmutableDeltaLakeTable.builder()
                .lastCheckpoint("Last checkpoint foo bar " + random.nextLong())
                .addMetadataLocationHistory("metadata location history " + random.nextLong());
        for (int i = 0; i < random.nextInt(4); i++) {
          deltaBuilder
              .lastCheckpoint("Another checkpoint " + random.nextLong())
              .addMetadataLocationHistory("Another metadata location " + random.nextLong());
        }
        if (currentContents != null) {
          deltaBuilder.id(currentContents.getId());
        }
        return deltaBuilder.build();
      case ICEBERG_VIEW:
        ImmutableIcebergView.Builder viewBuilder =
            ImmutableIcebergView.builder()
                .metadataLocation("metadata " + random.nextLong())
                .versionId(random.nextInt())
                .schemaId(random.nextInt())
                .dialect("Spark-" + random.nextInt())
                .sqlText("SELECT blah FROM meh;");
        if (currentContents != null) {
          viewBuilder.id(currentContents.getId());
        }
        return viewBuilder.build();
      default:
        throw new UnsupportedOperationException(
            String.format("Content type %s not supported", contentType.name()));
    }
  }
}
