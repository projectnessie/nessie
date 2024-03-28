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

import static org.projectnessie.model.Content.Type.NAMESPACE;

import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Reference;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/** Deletes a namespace recursively. */
@Command(
    name = "delete-recursive",
    mixinStandardHelpOptions = true,
    description = "Delete selected namespace and its nested objects (recursively)")
public class DeleteRecursive extends CommittingCommand {

  @CommandLine.Option(
      names = {"-r", "--ref", "--branch"},
      description = "Name of the branch where to make changes (default branch if not set).")
  private String branchName;

  @CommandLine.Option(
      names = {"-k", "--key"},
      paramLabel = "<key element>",
      description = "Elements (one or more) of a single namespace key to delete.")
  private List<String> keyElements;

  @Override
  public void execute() throws NessieNotFoundException, NessieConflictException {
    try (NessieApiV2 api = createNessieApiInstance()) {
      if (isVerbose()) {
        spec.commandLine().getOut().println("Fetching existing entries...");
      }

      Branch branch;
      if (branchName == null) {
        branch = api.getDefaultBranch();
      } else {
        Reference reference = api.getReference().refName(branchName).get();
        Preconditions.checkArgument(
            reference instanceof Branch, "Reference %s is not a branch.", branchName);
        branch = (Branch) reference;
      }

      ContentKey ns = ContentKey.of(keyElements);

      int count = 0;
      CommitMultipleOperationsBuilder commit = api.commitMultipleOperations();
      try (Stream<Entry> entries = api.getEntries().reference(branch).prefixKey(ns).stream()) {
        Iterator<Entry> it = entries.iterator();
        while (it.hasNext()) {
          Entry entry = it.next();
          if (ns.equals(entry.getName())) {
            Preconditions.checkArgument(
                NAMESPACE.equals(entry.getType()),
                "Key %s does not point to a Namespace, it points to %s",
                entry.getName(),
                entry.getType());
          }

          commit.operation(Delete.of(entry.getName()));
          count++;
        }

        if (count == 0) {
          spec.commandLine().getOut().println("Nothing to delete.");
          return;
        }

        if (isVerbose()) {
          spec.commandLine().getOut().printf("Deleting %d entries...%n", count);
        }

        commit.commitMeta(
            commitMetaFromMessage(
                String.format("Recursively deleted %d entries at %s", count, ns)));

        commit.branch(branch);
        Branch head = commit.commit();

        spec.commandLine().getOut().printf("Deleted %d entries at %s%n", count, head);
      }
    }
  }
}
