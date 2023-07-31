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

import java.util.List;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNotFoundException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "entries",
    mixinStandardHelpOptions = true,
    description =
        "List entries (keys) with the associated object type, ID and (optionally) content.")
public class ReadEntries extends AbstractCommand {

  @Option(
      names = {"-r", "--ref"},
      description = "Name of the branch/tag to read content from, defaults to 'main'")
  private String ref = "main";

  @Option(
      names = {"-C", "--with-content"},
      description = "Include content for each entry.")
  private boolean withContent;

  @Override
  public void execute() throws NessieNotFoundException {
    try (NessieApiV2 api = createNessieApiInstance()) {
      spec.commandLine().getOut().printf("Listing entries for reference '%s'.%n%n", ref);
      api.getEntries().refName(ref).withContent(withContent).stream()
          .forEach(
              entry -> {
                spec.commandLine().getOut().printf("Key: %s%n", entry.getName());
                if (isVerbose()) {
                  List<String> key = entry.getName().getElements();
                  for (int i = 0; i < key.size(); i++) {
                    spec.commandLine().getOut().printf("  key[%d]: %s%n", i, key.get(i));
                  }
                }

                spec.commandLine().getOut().printf("Type: %s%n", entry.getType());
                spec.commandLine().getOut().printf("Content ID: %s%n", entry.getContentId());

                if (entry.getContent() != null) {
                  spec.commandLine().getOut().printf("Value: %s%n", entry.getContent());
                }

                spec.commandLine().getOut().println();
              });
      spec.commandLine().getOut().printf("Done listing entries.%n");
    }
  }
}
