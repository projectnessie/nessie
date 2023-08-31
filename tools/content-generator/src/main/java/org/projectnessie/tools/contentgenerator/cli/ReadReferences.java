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
package org.projectnessie.tools.contentgenerator.cli;

import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNotFoundException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Implementation to read all references. */
@Command(name = "refs", mixinStandardHelpOptions = true, description = "Read references")
public class ReadReferences extends AbstractCommand {

  @Option(
      names = {"-L", "--limit"},
      description =
          "Limit the number of references to read. A value <= 0 means unlimited. Defaults to 0.")
  private long limit = 0;

  @Override
  public void execute() throws NessieNotFoundException {
    try (NessieApiV2 api = createNessieApiInstance()) {
      spec.commandLine()
          .getOut()
          .printf(
              "Reading %s references...%n%n",
              limit > 0 ? "up to " + String.format("%,d", limit) : "all");
      long count =
          api.getAllReferences().stream()
              .limit(limit > 0 ? limit : Long.MAX_VALUE)
              .peek(reference -> spec.commandLine().getOut().println(reference))
              .count();
      spec.commandLine().getOut().printf("%nDone reading %,d references.%n%n", count);
    }
  }
}
