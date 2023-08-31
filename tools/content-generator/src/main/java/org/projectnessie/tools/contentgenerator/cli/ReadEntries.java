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

import java.io.PrintWriter;
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
      names = {"-H", "--hash"},
      description =
          "Hash of the commit to read content from, defaults to HEAD. Relative lookups are accepted.")
  private String hash;

  @Option(
      names = {"-C", "--with-content"},
      description = "Include content for each entry.")
  private boolean withContent;

  @Option(
      names = {"-L", "--limit"},
      description =
          "Limit the number of entries to read. A value <= 0 means unlimited. Defaults to 0.")
  private long limit = 0;

  @Override
  public void execute() throws NessieNotFoundException {
    PrintWriter out = spec.commandLine().getOut();

    try (NessieApiV2 api = createNessieApiInstance()) {
      out.printf(
          "Listing %s entries for reference '%s' @ %s...%n%n",
          limit > 0 ? "up to " + String.format("%,d", limit) : "all",
          ref,
          hash == null ? "HEAD" : hash);
      long count =
          api.getEntries().refName(ref).hashOnRef(hash).withContent(withContent).stream()
              .limit(limit > 0 ? limit : Long.MAX_VALUE)
              .peek(
                  entry -> {
                    out.printf("Key: %s%n", entry.getName());
                    if (isVerbose()) {
                      List<String> key = entry.getName().getElements();
                      for (int i = 0; i < key.size(); i++) {
                        out.printf("  key[%d]: %s%n", i, key.get(i));
                      }
                    }

                    out.printf("Type: %s%n", entry.getType());
                    out.printf("Content ID: %s%n", entry.getContentId());

                    if (entry.getContent() != null) {
                      out.printf("Value: %s%n", entry.getContent());
                    }

                    out.println();
                  })
              .count();
      out.printf(
          "%nDone listing %,d entries for reference '%s' @ %s.%n%n",
          count, ref, hash == null ? "HEAD" : hash);
    }
  }
}
