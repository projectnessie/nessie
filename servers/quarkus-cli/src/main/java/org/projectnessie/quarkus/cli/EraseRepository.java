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

import javax.inject.Inject;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
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

  @Inject DatabaseAdapterConfig adapterConfig;

  @Override
  public Integer call() {
    warnOnInMemory();

    String code = getConfirmationCode();
    if (!code.equals(confirmationCode)) {
      spec.commandLine()
          .getErr()
          .printf(
              "Please use the '--confirmation-code=%s' option to indicate that the"
                  + " repository erasure operation is intentional.%nAll Nessie data will be lost!%n",
              code);
      return 1;
    }

    databaseAdapter.eraseRepo();

    if (newDefaultBranch != null) {
      databaseAdapter.initializeRepo(newDefaultBranch);
    }

    return 0;
  }

  private String getConfirmationCode() {
    // Derive some stable number from configuration
    int code = adapterConfig.getRepositoryId().hashCode();
    code += 1; // avoid zero for an empty repo ID
    code *= adapterConfig.getParentsPerCommit();
    code *= adapterConfig.getKeyListDistance();
    code *= adapterConfig.getMaxKeyListSize();
    // Format the code using MAX_RADIX to reduce the resultant string length
    return Long.toString(code, Character.MAX_RADIX);
  }
}
