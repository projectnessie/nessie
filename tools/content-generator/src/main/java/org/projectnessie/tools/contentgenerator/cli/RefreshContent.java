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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import org.projectnessie.model.Reference;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "content-refresh",
    mixinStandardHelpOptions = true,
    description = "Get and Put content objects without changes to refresh their storage model")
public class RefreshContent extends CommittingCommand {

  @Option(
      names = {"--input"},
      description =
          "Input file name containing a JSON array of 'ContentInfoEntry' objects from 'content-info' CLI command. "
              + "(if not set, use --key and --ref).")
  private String input;

  @Option(
      names = {"-k", "--key"},
      description =
          "Key elements to use for loading and refreshing a content object (ignored if --input is set).")
  private List<String> keyElements;

  @Option(
      names = {"-r", "--ref"},
      description =
          "Branch name for committing refreshed content objects (ignored if --input is set).")
  private String ref;

  @Option(
      names = {"-B", "--batch"},
      defaultValue = "100",
      description = "The number of keys to process in each batched read operation.")
  private int batchSize;

  @Option(
      names = {"-m", "--message"},
      description = "Commit message to use for each refresh operation (auto-generated if not set).")
  private String message;

  @Option(
      names = {"--storage-model"},
      description =
          "If set, only those entries that have the specified storage model will be refreshed "
              + "(ignored if --input is not set).")
  private String storageModel;

  @Option(
      names = {"--skip-tags"},
      description =
          "If set, input references that are not branches will be ignored (as opposed to reported as errors).")
  private boolean skipTags;

  @Override
  public void execute() throws BaseNessieClientServerException {
    try (NessieApiV2 api = createNessieApiInstance()) {
      if (input == null) {
        Reference reference = api.getReference().refName(ref).get();
        refresh(api, reference, Collections.singletonList(ContentKey.of(keyElements)));
      } else {
        try (FileInputStream inputStream = new FileInputStream(input)) {
          refresh(api, inputStream);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
    }
  }

  private void refresh(NessieApiV2 api, InputStream input) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonParser parser = mapper.createParser(input);

    JsonToken token = parser.nextToken();
    if (!JsonToken.START_ARRAY.equals(token)) {
      throw new IllegalArgumentException(
          "Input data should be a JSON array of 'ContentInfoEntry' objects.");
    }

    List<JsonNode> batch = new ArrayList<>(batchSize);
    while (JsonToken.START_OBJECT.equals(parser.nextToken())) {
      batch.add(parser.readValueAs(JsonNode.class));
      if (batch.size() >= batchSize) {
        refresh(api, batch);
        batch.clear();
      }
    }

    if (!batch.isEmpty()) {
      refresh(api, batch);
    }
  }

  private void refresh(NessieApiV2 api, List<JsonNode> batch)
      throws BaseNessieClientServerException {
    Map<String, List<ContentKey>> perRef = new HashMap<>();
    for (JsonNode node : batch) {
      // Parse according to the structure of org.projectnessie.quarkus.cli.ContentInfoEntry
      List<String> keyElements = new ArrayList<>();
      node.required("key")
          .required("elements")
          .elements()
          .forEachRemaining(n -> keyElements.add(n.asText()));

      String refName = node.required("reference").asText();

      String model = node.get("storageModel").asText();
      if (storageModel != null && !storageModel.equals(model)) {
        continue;
      }

      perRef.computeIfAbsent(refName, key -> new ArrayList<>()).add(ContentKey.of(keyElements));
    }

    for (Map.Entry<String, List<ContentKey>> entry : perRef.entrySet()) {
      Reference reference = api.getReference().refName(entry.getKey()).get();
      refresh(api, reference, entry.getValue());
    }
  }

  private void refresh(NessieApiV2 api, Reference ref, List<ContentKey> keys)
      throws BaseNessieClientServerException {
    if (!(ref instanceof Branch)) {
      if (skipTags) {
        spec.commandLine()
            .getOut()
            .printf("Skipped %d keys because %s is not a branch.%n", keys.size(), ref);
        return;
      } else {
        throw new IllegalArgumentException("Content can only be refreshed on branches: " + ref);
      }
    }

    GetContentBuilder request = api.getContent().reference(ref);
    keys.forEach(request::key);

    Map<ContentKey, Content> contentMap = request.get();

    commitSameContent(api, (Branch) ref, contentMap);
  }

  private void commitSameContent(
      NessieApiV2 api, Branch branch, Map<ContentKey, Content> contentMap)
      throws BaseNessieClientServerException {

    if (contentMap.isEmpty()) {
      return;
    }

    String msg = message == null ? ("Refresh " + contentMap.size() + " key(s)") : message;

    CommitMultipleOperationsBuilder request =
        api.commitMultipleOperations().branch(branch).commitMeta(commitMetaFromMessage(msg));

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
