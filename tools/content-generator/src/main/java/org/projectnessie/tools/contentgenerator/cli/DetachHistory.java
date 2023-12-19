/*
 * Copyright (C) 2023 Dremio
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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "detach-history",
    mixinStandardHelpOptions = true,
    description =
        "Get the latest contents from one or all branches, commit it directly on top of the root commit (no history), "
            + "then reassign the original branch(es) to the new commit.")
public class DetachHistory extends RefreshContent {

  @CommandLine.Option(
      names = {"--src-suffix"},
      description =
          "Name suffix for tagging the original (source) branches, defaults to '-original'.")
  private String origSuffix = "-original";

  @CommandLine.Option(
      names = {"--final-suffix"},
      description =
          "Name suffix for tagging the the final stat of source branches (equivalent to the tag generated "
              + "from the --src-suffix option). This suffix is used during retries, defaults to '-completed'.")
  private String completedSuffix = "-completed";

  @CommandLine.Option(
      names = {"--max-attempts"},
      description =
          "Max attempts (in case of concurrent commits from other actors), defaults to 100.")
  private int maxAttempts = 100;

  private final Map<String, Branch> sources = new HashMap<>();
  private final Map<String, Branch> targets = new HashMap<>();
  private String tmpSuffix;
  private String rootHash;

  @Override
  public void execute() throws BaseNessieClientServerException {
    // Overview:
    // 1) Tag old source branch HEADs (in setup()).
    // 2) Copy the latest content to temporary branches.
    // 3) Reassign source branches to HEADs of corresponding temporary branches.
    // The last step may fail if there are concurrent commits to source branch, in which case
    // the whole process is re-tried.
    try (NessieApiV2 api = createNessieApiInstance()) {
      rootHash = api.getConfig().getNoAncestorHash();
      for (int attempt = 0; attempt < maxAttempts; attempt++) {
        spec.commandLine().getOut().printf("Running attempt %d...%n", attempt);

        sources.clear();
        targets.clear();
        tmpSuffix = "-tmp-" + UUID.randomUUID();

        super.execute(); // copy content into temporary branches

        try {
          // reassign original branch names to HEADs of corresponding temporary branches
          reassign(api);

          spec.commandLine().getOut().printf("Completed successfully%n");
          break;
        } catch (NessieReferenceConflictException e) {
          spec.commandLine()
              .getOut()
              .printf("Unable to complete attempt %d, retrying...%n", attempt);

          // Delete any remaining temporary references
          for (Branch target : targets.values()) {
            try {
              Reference tmpRef = api.getReference().refName(target.getName()).get();
              api.deleteReference().reference(tmpRef).delete();
            } catch (NessieNotFoundException ex) {
              // ignore, the target reference must have been reassigned and deleted
            }
          }
        }
      }
    }
  }

  @Override
  protected void commitSameContent(
      NessieApiV2 api, Branch source, Map<ContentKey, Content> contentMap)
      throws BaseNessieClientServerException {
    Branch target = setup(api, source);
    if (target == null) {
      return; // already processed
    }

    Map<ContentKey, Content> detachedContents = new HashMap<>();
    // Remove content IDs because these entries will be treated as new entities
    // on the target branch due to empty commit history there.
    contentMap.forEach((k, v) -> detachedContents.put(k, v.withId(null)));
    super.commitSameContent(api, target, detachedContents);
  }

  private Branch setup(NessieApiV2 api, Branch source)
      throws NessieConflictException, NessieNotFoundException {
    Branch old = sources.putIfAbsent(source.getName(), source);
    if (old != null && !Objects.equals(old.getHash(), source.getHash())) {
      throw new IllegalArgumentException("Hash mismatch");
    }

    try {
      api.getReference().refName(source.getName() + completedSuffix).get();
      spec.commandLine().getOut().printf("Ignoring previously migrated reference '%s'%n", source);
      return null;
    } catch (NessieNotFoundException e) {
      // expected
    }

    spec.commandLine().getOut().printf("Migrating '%s'%n", source);
    String origName = source.getName() + origSuffix;
    try {
      Reference origRef = api.getReference().refName(origName).get();
      api.assignReference().reference(origRef).assignTo(source).assign();
    } catch (NessieNotFoundException e) {
      api.createReference()
          .sourceRefName(source.getName())
          .reference(Tag.of(origName, source.getHash()))
          .create();
    }

    return targets.computeIfAbsent(
        source.getName(),
        sourceName -> {
          String targetName = sourceName + tmpSuffix;
          try {
            // create a target branch without any commit history
            return (Branch)
                api.createReference().reference(Branch.of(targetName, rootHash)).create();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  private void reassign(NessieApiV2 api) throws NessieNotFoundException, NessieConflictException {
    spec.commandLine().getOut().println("Reassigning references...");

    for (Map.Entry<String, Branch> e : targets.entrySet()) {
      String name = e.getKey();
      Reference target = e.getValue();
      Branch source = sources.get(name);

      // get the latest hash
      target = api.getReference().refName(target.getName()).get();

      api.assignReference().reference(source).assignTo(target).assign();

      api.createReference()
          .reference(Tag.of(source.getName() + completedSuffix, source.getHash()))
          .create();

      spec.commandLine().getOut().printf("Completed migration for '%s'%n", source.getName());

      api.deleteReference().reference(target).delete();
    }
  }
}
