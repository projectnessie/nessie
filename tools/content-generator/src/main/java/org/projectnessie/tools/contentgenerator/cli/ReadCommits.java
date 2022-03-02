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
import java.util.Objects;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/** Implementation to read commit log. */
@Command(name = "commits", mixinStandardHelpOptions = true, description = "Read commits")
public class ReadCommits extends AbstractCommand {

  @Option(
      names = {"-r", "--ref"},
      description = "Name of the branch/tag to read the commit log from, defaults to 'main'")
  private String ref = "main";

  @Spec private CommandSpec spec;

  @Override
  public void execute() throws NessieNotFoundException {
    try (NessieApiV1 api = createNessieApiInstance()) {
      spec.commandLine().getOut().printf("Reading commits for ref '%s'\n\n", ref);
      FetchOption fetchOption = isVerbose() ? FetchOption.ALL : FetchOption.MINIMAL;
      LogResponse logResponse = api.getCommitLog().refName(ref).fetch(fetchOption).get();
      for (LogResponse.LogEntry logEntry : logResponse.getLogEntries()) {
        CommitMeta commitMeta = logEntry.getCommitMeta();
        spec.commandLine()
            .getOut()
            .printf(
                "%s\t%s\t%s [%s]\n",
                Objects.requireNonNull(commitMeta.getHash()).substring(0, 8),
                commitMeta.getAuthorTime(),
                commitMeta.getMessage(),
                commitMeta.getAuthor());

        List<Operation> operations = logEntry.getOperations();
        if (operations != null) {
          for (Operation op : operations) {
            spec.commandLine().getOut().printf("  %s\n", op);
            if (isVerbose()) {
              List<String> key = op.getKey().getElements();
              for (int i = 0; i < key.size(); i++) {
                spec.commandLine().getOut().printf("    key[%d]: %s\n", i, key.get(i));
              }
            }
          }
        }
      }
      spec.commandLine().getOut().printf("\nDone reading commits for ref '%s'\n\n", ref);
    }
  }
}
