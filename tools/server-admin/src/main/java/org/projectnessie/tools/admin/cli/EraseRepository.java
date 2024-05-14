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

import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.persist.Persist;
import picocli.CommandLine;

@CommandLine.Command(
    name = "erase-repository",
    mixinStandardHelpOptions = true,
    description =
        "Erase current Nessie repository (all data will be lost) and optionally re-initialize it.")
public class EraseRepository extends BaseCommand {

  @CommandLine.Option(
      names = {"-r", "--re-initialize"},
      description =
          "Re-initialize the repository after erasure. If set, provides the default branch name for the new repository.")
  private String newDefaultBranch;

  @CommandLine.Option(
      names = {"--confirmation-code"},
      description =
          "Confirmation code for erasing the repository (will be emitted by this command if not set).")
  private String confirmationCode;

  @Override
  public Integer call() {
    warnOnInMemory();

    if (!repositoryLogic(persist).repositoryExists()) {
      spec.commandLine().getErr().println("Nessie repository does not exist");
      return EXIT_CODE_REPO_DOES_NOT_EXIST;
    }

    String code = getConfirmationCode(persist);
    if (!code.equals(confirmationCode)) {
      spec.commandLine()
          .getErr()
          .printf(
              "Please use the '--confirmation-code=%s' option to indicate that the"
                  + " repository erasure operation is intentional.%nAll Nessie data will be lost!%n",
              code);
      return EXIT_CODE_GENERIC_ERROR;
    }

    persist.erase();
    spec.commandLine().getOut().println("Repository erased.");

    if (newDefaultBranch != null) {
      repositoryLogic(persist).initialize(newDefaultBranch);
      spec.commandLine().getOut().println("Repository initialized.");
    }

    return 0;
  }

  static String getConfirmationCode(Persist persist) {
    StoreConfig config = persist.config();

    // Derive some stable number from configuration
    long code = config.repositoryId().hashCode();
    code += 1; // avoid zero for an empty repo ID

    RepositoryLogic repositoryLogic = repositoryLogic(persist);
    RepositoryDescription repoDesc = repositoryLogic.fetchRepositoryDescription();
    if (repoDesc != null) {
      code = code * 31 + repoDesc.repositoryCreatedTime().toEpochMilli();
      code = code * 31 + repoDesc.oldestPossibleCommitTime().toEpochMilli();
    }
    code = 31L * code + config.parentsPerCommit();
    code = 31L * code + config.maxIncrementalIndexSize();
    code = 31L * code + config.maxSerializedIndexSize();
    code = Math.abs(code);
    // Format the code using MAX_RADIX to reduce the resultant string length
    return Long.toString(code, Character.MAX_RADIX);
  }
}
