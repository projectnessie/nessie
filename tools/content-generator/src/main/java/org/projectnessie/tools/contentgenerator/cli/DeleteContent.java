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
import java.util.stream.Collectors;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import picocli.CommandLine.Command;

/** Deletes content objects. */
@Command(
    name = "delete",
    mixinStandardHelpOptions = true,
    description = "Delete selected content objects")
public class DeleteContent extends BulkCommittingCommand {

  @Override
  protected void processBatch(NessieApiV2 api, Reference ref, List<ContentKey> keys) {
    String defaultMsg =
        keys.size() == 1 ? "Delete " + keys.get(0) : "Delete " + keys.size() + " keys.";

    try {
      Branch head =
          api.commitMultipleOperations()
              .commitMeta(commitMetaFromMessage(defaultMsg))
              .branch((Branch) ref)
              .operations(keys.stream().map(Operation.Delete::of).collect(Collectors.toList()))
              .commit();

      spec.commandLine().getOut().printf("Deleted %s keys in %s%n", keys.size(), head);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
