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

import static java.util.Collections.singletonList;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Reference;
import picocli.CommandLine.Option;

public abstract class BulkCommittingCommand extends CommittingCommand {

  @Option(
      names = {"--all"},
      description =
          "Process all active keys in all branches (supersedes --input and all other selector options).")
  private boolean all;

  @Option(
      names = {"-R", "--recursive"},
      description =
          "Process keys equal to or nested under the value of the key specified by --key options"
              + " (supersedes --input; may be used together with --all).")
  private boolean nested;

  @Option(
      names = {"-r", "--ref", "--branch"},
      description = "Branch name for making changes (default branch if not set).")
  private String ref;

  @Option(
      names = {"-f", "--input"},
      description = "Input file name. See the --format option for details (if not set, use --key).")
  private String input;

  @Option(
      names = {"-F", "--format"},
      description =
          "The format of the input file. CSV_KEYS means one content key per line (separated key elements or "
              + "URL path encoded whole key). CONTENT_INFO_JSON means a JSON array of objects having a 'key' "
              + "attribute (with an 'elements' string array inside defining the content key) and a 'reference' "
              + "attribute defining the name of reference holding the object (tags are automatically "
              + "ignored). If --branch is set, it overrides the 'reference' attribute.")
  private InputFormat format;

  @Option(
      names = {"-S", "--separator"},
      description =
          "The fields separator for CVS input files (if not set, each line is interpreted as a URL path "
              + "encoded key string).")
  private String separator;

  @Option(
      names = {"-k", "--key"},
      paramLabel = "<key element>",
      description =
          "Elements (one or more) of a single content key to process (ignored if --input is set).")
  private List<String> keyElements;

  @Option(
      names = {"-B", "--batch"},
      defaultValue = "100",
      description = "The number of keys to group for processing (read and/or write operations).")
  private int batchSize;

  protected abstract void processBatch(NessieApiV2 api, Branch ref, List<ContentKey> keys);

  private Branch branchFromOptions(NessieApiV2 api) throws NessieNotFoundException {
    if (ref == null) {
      return api.getDefaultBranch();
    } else {
      Reference reference = api.getReference().refName(ref).get();
      if (!(reference instanceof Branch)) {
        throw new IllegalArgumentException("Content can only be committed to branches: " + ref);
      }
      return (Branch) reference;
    }
  }

  @Override
  public void execute() throws BaseNessieClientServerException {
    try (NessieApiV2 api = createNessieApiInstance()) {
      if (all) {
        processAll(api);
      } else if (nested) {
        processAll(api, branchFromOptions(api));
      } else if (input == null) {
        processBatch(api, branchFromOptions(api), singletonList(ContentKey.of(keyElements)));
      } else {
        try (FileInputStream inputStream = new FileInputStream(input)) {
          if (format == null) {
            throw new IllegalArgumentException(
                "The --format option must be set when --input is set.");
          }

          switch (format) {
            case CSV_KEYS:
              processLines(api, branchFromOptions(api), inputStream);
              break;
            case CONTENT_INFO_JSON:
              processJson(api, inputStream);
              break;
            default:
              throw new IllegalArgumentException("Unsupported format: " + format);
          }
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
    }
  }

  private void processAll(NessieApiV2 api) throws NessieNotFoundException {
    api.getAllReferences().stream().forEach(r -> processAll(api, r));
  }

  private Namespace rootNamespace() {
    return nested ? Namespace.of(keyElements) : Namespace.EMPTY;
  }

  protected Iterator<List<ContentKey>> partitionKeys(Stream<ContentKey> input, int batchSize) {
    return Iterators.partition(input.iterator(), batchSize);
  }

  private void processAll(NessieApiV2 api, Reference ref) {
    if (!(ref instanceof Branch)) {
      return;
    }

    try {
      Namespace root = rootNamespace();
      GetEntriesBuilder listRequest = api.getEntries().reference(ref).withContent(false);
      if (!root.isEmpty()) {
        listRequest.prefixKey(root.toContentKey());
      }

      partitionKeys(listRequest.stream().map(EntriesResponse.Entry::getName), batchSize)
          .forEachRemaining(batch -> processBatch(api, (Branch) ref, batch));
    } catch (NessieNotFoundException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void processLines(NessieApiV2 api, Branch ref, InputStream input) throws IOException {
    Function<String, ContentKey> parseKey;
    if (separator == null) {
      parseKey = ContentKey::fromPathString;
    } else {
      String sep = Pattern.quote(separator);
      parseKey = (line) -> ContentKey.of(line.split(sep));
    }

    Iterators.partition(
            new BufferedReader(new InputStreamReader(input)).lines().map(parseKey).iterator(),
            batchSize)
        .forEachRemaining(keys -> processBatch(api, ref, keys));
  }

  private void processJson(NessieApiV2 api, InputStream input) throws IOException {
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
        process(api, batch);
        batch.clear();
      }
    }

    if (!batch.isEmpty()) {
      process(api, batch);
    }
  }

  private void process(NessieApiV2 api, List<JsonNode> batch)
      throws BaseNessieClientServerException {
    Map<String, List<ContentKey>> perRef = new HashMap<>();
    for (JsonNode node : batch) {
      // Parse according to the structure of org.projectnessie.quarkus.cli.ContentInfoEntry
      List<String> keyElements = new ArrayList<>();
      node.required("key")
          .required("elements")
          .elements()
          .forEachRemaining(n -> keyElements.add(n.asText()));

      String refName = ref == null ? node.required("reference").asText() : ref;

      perRef.computeIfAbsent(refName, key -> new ArrayList<>()).add(ContentKey.of(keyElements));
    }

    for (Map.Entry<String, List<ContentKey>> entry : perRef.entrySet()) {
      Reference reference = api.getReference().refName(entry.getKey()).get();
      if (!(reference instanceof Branch)) {
        continue; // automatically skip tags when processing JSON input
      }
      processBatch(api, (Branch) reference, entry.getValue());
    }
  }

  public enum InputFormat {
    CONTENT_INFO_JSON,
    CSV_KEYS,
  }
}
