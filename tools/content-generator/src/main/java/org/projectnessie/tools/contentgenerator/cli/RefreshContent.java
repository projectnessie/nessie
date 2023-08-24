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
import java.util.Map;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation;
import picocli.CommandLine.Command;

@Command(
    name = "content-refresh",
    mixinStandardHelpOptions = true,
    description = "Get and Put content objects without changes to refresh their storage model")
public class RefreshContent extends BulkCommittingCommand {

  @Override
  protected void processBatch(NessieApiV2 api, Branch ref, List<ContentKey> keys) {
    GetContentBuilder request = api.getContent().reference(ref);
    keys.forEach(request::key);

    try {
      Map<ContentKey, Content> contentMap = request.get();

      commitSameContent(api, ref, contentMap);
    } catch (BaseNessieClientServerException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void commitSameContent(
      NessieApiV2 api, Branch branch, Map<ContentKey, Content> contentMap)
      throws BaseNessieClientServerException {

    if (contentMap.isEmpty()) {
      return;
    }

    String defaultMsg = "Refresh " + contentMap.size() + " key(s)";

    CommitMultipleOperationsBuilder request =
        api.commitMultipleOperations().branch(branch).commitMeta(commitMetaFromMessage(defaultMsg));

    for (Map.Entry<ContentKey, Content> entry : contentMap.entrySet()) {
      Content content = entry.getValue();
      request.operation(Operation.Put.of(entry.getKey(), content));
    }

    Branch head = request.commit();

    spec.commandLine()
        .getOut()
        .printf(
            "Refreshed %d keys in %s at commit %s%n",
            contentMap.size(), branch.getName(), head.getHash());
  }
}
