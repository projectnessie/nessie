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

import io.quarkus.picocli.runtime.annotations.TopCommand;
import java.io.PrintWriter;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@TopCommand
@Command(
    name = "nessie-server-admin-tool-runner.jar",
    mixinStandardHelpOptions = true,
    versionProvider = NessieVersionProvider.class,
    description = "Nessie Server Admin Tool",
    subcommands = {
      NessieInfo.class,
      HelpCommand.class,
      CleanupRepository.class,
      CutHistory.class,
      CheckContent.class,
      DeleteCatalogTasks.class,
      EraseRepository.class,
      ExportRepository.class,
      ImportRepository.class,
      ThirdPartyLicenses.class
    })
public class NessieServerAdminTool extends BaseCommand {

  @Override
  public Integer call() {
    return info();
  }

  private int info() {
    warnOnInMemory();

    PrintWriter out = spec.commandLine().getOut();

    out.println("Nessie server information & maintenance tool.");
    out.println("Use the 'help' command.");
    out.println();
    return 0;
  }
}
