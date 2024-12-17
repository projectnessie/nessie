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

import static org.projectnessie.versioned.storage.cleanup.Cleanup.createCleanup;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;

import java.time.Duration;
import java.time.Instant;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.storage.cleanup.CleanupParams;
import picocli.CommandLine;

@CommandLine.Command(
    name = "cut-history",
    mixinStandardHelpOptions = true,
    description = {
      "Advanced commit log manipulation command that removes parents from the specified commit. "
          + "Read the full help message before using!",
      "* Make sure to perform an export of the Nessie repository before running this command!",
      "* References (branches, tags) may lose common ancestors as a result of running this command. "
          + "If that happens, the affected references will lose be ability to merge into each other.",
      "* This command will make Nessie caches inconsistent with persisted data. "
          + "Therefore, it is preferable to run this command when Nessie servers are shut down. At the very least, "
          + "all Nessie servers should be restarted as soon as possible after this command completes. "
          + "Subsequently, it may be worth running the `cleanup-repository` command.",
    })
public class CutHistory extends BaseCommand {
  @CommandLine.Option(
      names = {"-c", "--commit"},
      required = true,
      description = "The hash of the commit that gets detached from its parents.")
  private String cutPoint;

  @CommandLine.Option(
      names = {"--dry-run"},
      description = "Perform all operations, but do not modify any objects.")
  private boolean dryRun;

  @Override
  public Integer call() throws Exception {
    if (!repositoryLogic(persist).repositoryExists()) {
      spec.commandLine().getErr().println("Nessie repository does not exist");
      return EXIT_CODE_REPO_DOES_NOT_EXIST;
    }

    CleanupParams cleanupParams = CleanupParams.builder().dryRun(dryRun).build();

    var cleanup = createCleanup(cleanupParams);
    var params = cleanup.buildCutHistoryParams(persist, hashToObjId(Hash.of(cutPoint)));
    var cutHistory = cleanup.createCutHistory(params);

    var start = Instant.now();
    var identifyResult = cutHistory.identifyAffectedCommits();
    spec.commandLine()
        .getOut()
        .printf("Identified %d related commits.%n", identifyResult.affectedCommitIds().size());

    var result = cutHistory.cutHistory(identifyResult);

    result
        .failures()
        .forEach(
            (id, e) ->
                spec.commandLine()
                    .getErr()
                    .printf("Unable to rewrite parents for %s: %s.%n", id, e));

    if (!result.rewrittenCommitIds().isEmpty()) {
      spec.commandLine()
          .getOut()
          .printf("Rewrote %d commits.%n", result.rewrittenCommitIds().size());
    }

    // wasHistoryCut() == true implies that it was _not_ a dry run
    if (result.wasHistoryCut()) {
      spec.commandLine().getOut().printf("Removed parents from commit %s.%n", cutPoint);
    }

    Duration duration = Duration.between(start, Instant.now());
    spec.commandLine().getOut().printf("Completed in %s.%n", duration);
    return result.failures().isEmpty() ? 0 : EXIT_CODE_GENERIC_ERROR;
  }
}
