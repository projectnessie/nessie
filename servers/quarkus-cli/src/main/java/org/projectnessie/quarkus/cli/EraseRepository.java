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

import picocli.CommandLine;

@CommandLine.Command(
    name = "erase-repo",
    mixinStandardHelpOptions = true,
    description = "Erase current Nessie repository and optionally re-initialize it.")
public class EraseRepository extends BaseCommand {

  @CommandLine.Option(
      names = {"-r", "--re-initialize"},
      description =
          "Re-initialize the repository after erasure. If set, provides the default branch name for the new repository.")
  private String newDefaultBranch;

  @Override
  public Integer call() {
    warnOnInMemory();

    databaseAdapter.eraseRepo();

    if (newDefaultBranch != null) {
      databaseAdapter.initializeRepo(newDefaultBranch);
    }

    return 0;
  }
}
