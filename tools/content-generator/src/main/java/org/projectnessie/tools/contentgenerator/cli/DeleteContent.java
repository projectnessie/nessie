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
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Deletes content objects. */
@Command(name = "delete", mixinStandardHelpOptions = true, description = "Delete content objects")
public class DeleteContent extends CommittingCommand {

  @Option(
      names = {"-r", "--branch"},
      description = "Name of the branch where to make changes, defaults to 'main'")
  private String ref = "main";

  @Option(
      names = {"-k", "--key"},
      description = "Content key to delete",
      required = true)
  private List<String> key;

  @Option(
      names = {"-m", "--message"},
      description = "Commit message (auto-generated if not set)")
  private String message;

  @Override
  public void execute() throws NessieNotFoundException, NessieConflictException {
    try (NessieApiV2 api = createNessieApiInstance()) {
      ContentKey contentKey = ContentKey.of(key);

      Reference refInfo = api.getReference().refName(ref).get();

      if (message == null) {
        message = "Delete: " + contentKey;
      }

      Branch head =
          api.commitMultipleOperations()
              .commitMeta(commitMetaFromMessage(message))
              .branch((Branch) refInfo)
              .operation(Operation.Delete.of(contentKey))
              .commit();

      spec.commandLine().getOut().printf("Deleted %s in %s%n", contentKey, head);
    }
  }
}
