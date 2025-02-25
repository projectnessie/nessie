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

import jakarta.validation.constraints.Min;
import java.util.ArrayList;
import java.util.List;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "copy", mixinStandardHelpOptions = true, description = "Repeatedly copy content")
public class CopyContent extends CommittingCommand {

  @Option(
      names = {"-r", "--ref", "--branch"},
      description = "Branch name for making changes (defaults to the default branch if not set).")
  private String ref;

  @Min(value = 1, message = "At least one copy is to be made.")
  @Option(
      names = {"-n", "--num-copies"},
      required = true,
      defaultValue = "10000",
      description = "Number of copies to make.")
  private int numCopies;

  @Option(
      names = {"-k", "--from"},
      description = "Content key to copy",
      required = true)
  private List<String> fromKey;

  @Option(
      names = {"-t", "--to"},
      description =
          "Content key pattern for the copies. Each element of the pattern may contain one instance of '%d',"
              + " which will be replaced with the copy counter.",
      required = true)
  private List<String> keyPattern;

  @Override
  public void execute() throws BaseNessieClientServerException {
    try (NessieApiV2 api = createNessieApiInstance()) {
      Reference branch;
      if (ref == null) {
        branch = api.getDefaultBranch();
      } else {
        branch = api.getReference().refName(ref).get();
      }

      ContentKey key = ContentKey.of(fromKey);
      Content content = api.getContent().reference(branch).getSingle(key).getContent();

      for (int i = 0; i < numCopies; i++) {
        List<String> to = new ArrayList<>(keyPattern.size());
        for (String pattern : keyPattern) {
          to.add(String.format(pattern, i));
        }

        ContentKey copyKey = ContentKey.of(to);
        branch =
            api.commitMultipleOperations()
                .branchName(branch.getName())
                .hash(branch.getHash())
                .commitMeta(commitMetaFromMessage("Copy " + i))
                .operation(Put.of(copyKey, content.withId(null)))
                .commit();

        spec.commandLine()
            .getOut()
            .printf("Created %s at %s%n", copyKey.toCanonicalString(), branch.getHash());
      }
    }
  }
}
