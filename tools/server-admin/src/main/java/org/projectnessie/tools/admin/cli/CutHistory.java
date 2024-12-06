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

import static org.projectnessie.versioned.storage.common.logic.CommitLogQuery.commitLogQuery;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.objIdToHash;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.Logics;
import org.projectnessie.versioned.storage.common.logic.PagedResult;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.versionstore.RefMapping;
import picocli.CommandLine;

@CommandLine.Command(
    name = "cut-history",
    mixinStandardHelpOptions = true,
    description = {
      "Advanced commit log manipulation command that detaches a number of most recent "
          + "commits from their parent commit trail. Use with extreme caution. See also the 'cleanup-repository' "
          + "command."
    })
public class CutHistory extends BaseCommand {
  @CommandLine.Option(
      names = {"-r", "--ref"},
      description = "Reference name to use (default branch, if not set).")
  private String ref;

  @CommandLine.Option(
      names = {"--depth", "-D"},
      required = true,
      description = "The number of commits to preserve intact starting from the ref HEAD.")
  private int depth;

  @Override
  public Integer call() throws Exception {
    if (!repositoryLogic(persist).repositoryExists()) {
      spec.commandLine().getErr().println("Nessie repository does not exist");
      return EXIT_CODE_REPO_DOES_NOT_EXIST;
    }

    CommitLogic commitLogic = Logics.commitLogic(persist);

    Hash head = resolveRefHead();
    List<CommitObj> toStore = new ArrayList<>();
    Set<ObjId> danglingTail = new HashSet<>(); // commit IDs referenced from direct children
    int count = depth;
    PagedResult<CommitObj, ObjId> it = commitLogic.commitLog(commitLogQuery(hashToObjId(head)));
    while (it.hasNext()) {
      CommitObj commitObj = it.next();

      if (count > 0) { // Processing a preserved commit
        danglingTail.addAll(commitObj.tail());
        danglingTail.remove(commitObj.id());
        danglingTail.remove(EMPTY_OBJ_ID);

        count--;
        continue;
      }

      if (!commitObj.secondaryParents().isEmpty()) {
        spec.commandLine()
            .getErr()
            .printf("ERROR: Unable to reset parents in merge commit '%s'%n", commitObj.id());
        return EXIT_CODE_GENERIC_ERROR;
      }

      if (EMPTY_OBJ_ID.equals(commitObj.directParent())) {
        spec.commandLine()
            .getErr()
            .printf(
                "Encountered root commit '%s'. Full commit history is preserved.%n",
                commitObj.id());
        return 0;
      }

      // Rewriting commit to fixup parents
      danglingTail.remove(commitObj.id());
      CommitObj.Builder builder = CommitObj.commitBuilder().from(commitObj);
      if (danglingTail.isEmpty()) {
        spec.commandLine().getOut().printf("New root commit '%s'%n", commitObj.id());
        builder.tail(List.of());
      } else {
        spec.commandLine()
            .getOut()
            .printf(
                "Rewriting commit '%s' to update the list of indirect parents.%n", commitObj.id());
        builder.tail(commitObj.tail().subList(0, 1)); // one direct parent for simplicity
      }

      toStore.add(builder.build());

      if (danglingTail.isEmpty()) {
        break;
      }
    }

    persist.upsertObjs(toStore.toArray(new Obj[0]));

    spec.commandLine().getOut().printf("Updated %d commits.%n", toStore.size());
    return 0;
  }

  private Hash resolveRefHead() throws ReferenceNotFoundException {
    String effectiveRef = ref == null ? serverConfig.getDefaultBranch() : ref;
    return objIdToHash(new RefMapping(persist).resolveNamedRef(effectiveRef).pointer());
  }
}
