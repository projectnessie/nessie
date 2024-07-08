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
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.Reference;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class BulkCommittingCommand extends CommittingCommand {

  @ArgGroup BranchSelector branchSelector = new BranchSelector();

  static class BranchSelector {

    @Option(
        names = {"--all"},
        description = "Process all branches (ignored when --input is set).")
    private boolean all;

    @Option(
        names = {"-r", "--ref", "--branch"},
        description = "Branch name for making changes (defaults to the default branch if not set).")
    private String ref;
  }

  @ArgGroup KeySelector keySelector = new KeySelector();

  static class KeySelector {

    @ArgGroup(exclusive = false)
    RegularKeySelector regularKeySelector = new RegularKeySelector();

    boolean isFileBased() {
      return fileKeySelector.input != null;
    }

    static class RegularKeySelector {

      @Option(
          names = {"-k", "--key"},
          paramLabel = "<key element>",
          required = true,
          description =
              "Elements (one or more) of a single content key to process. "
                  + "If not specified, all keys in the selected branches will be processed.")
      private List<String> keyElements = new ArrayList<>();

      @Option(
          names = {"-R", "--recursive"},
          defaultValue = "false",
          required = true,
          description =
              "Recursively process all child keys nested under the value of the key specified by --key options.")
      private boolean recursive;

      boolean isSingleKey() {
        return !keyElements.isEmpty() && !recursive;
      }

      ContentKey asContentKey() {
        return ContentKey.of(keyElements);
      }
    }

    @ArgGroup(exclusive = false)
    FileKeySelector fileKeySelector = new FileKeySelector();

    static class FileKeySelector {

      @Option(
          names = {"-f", "--input"},
          required = true,
          description =
              "Input file name. See the --format option for details (if not set, use --ref and --key to select "
                  + "respectively the branch and the key to process).")
      private String input;

      @Option(
          names = {"-F", "--format"},
          required = true,
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

      Function<String, ContentKey> keyParser() {
        return separator == null
            ? ContentKey::fromPathString
            : line -> ContentKey.of(line.split(Pattern.quote(separator)));
      }
    }
  }

  @Option(
      names = {"-B", "--batch"},
      defaultValue = "100",
      description = "The number of keys to group for processing (read and/or write operations).")
  private int batchSize;

  protected abstract void processBatch(NessieApiV2 api, Branch ref, List<ContentKey> keys);

  private Branch branchFromOptions(NessieApiV2 api) throws NessieNotFoundException {
    if (branchSelector.ref == null) {
      return api.getDefaultBranch();
    } else {
      Reference reference = api.getReference().refName(branchSelector.ref).get();
      if (!(reference instanceof Branch)) {
        throw new IllegalArgumentException(
            "Content can only be committed to branches: " + branchSelector.ref);
      }
      return (Branch) reference;
    }
  }

  /**
   * Process selected branches and keys according to the below rules.
   *
   * <ol>
   *   <li>If {@code --input} is set, then process the branches and keys in the file;
   *   <li>If not, then:
   *       <ol>
   *         <li>Select the branches to process:
   *             <ul>
   *               <li>If {@code --all} is set, select all branches;
   *               <li>Else if {@code --ref} is set, select only the given branch;
   *               <li>Else, select the default branch.
   *             </ul>
   *         <li>For each selected branch, select the keys to process:
   *             <ul>
   *               <li>If {@code --key} is set, select only that key;
   *               <li>If {@code --nested} is also set, also select child keys of {@code --key};
   *               <li>Else, select all keys.
   *             </ul>
   *       </ol>
   * </ol>
   */
  @Override
  public void execute() throws BaseNessieClientServerException {
    try (NessieApiV2 api = createNessieApiInstance()) {
      if (keySelector.isFileBased()) {
        processFile(api);
      } else if (branchSelector.all) {
        api.getAllReferences().stream().forEach(r -> processBranch(api, r));
      } else {
        processBranch(api, branchFromOptions(api));
      }
    }
  }

  private void processFile(NessieApiV2 api) {
    var selector = keySelector.fileKeySelector;
    try (FileInputStream inputStream = new FileInputStream(selector.input)) {
      switch (selector.format) {
        case CSV_KEYS:
          processLines(api, branchFromOptions(api), inputStream);
          break;
        case CONTENT_INFO_JSON:
          processJson(api, inputStream);
          break;
        default:
          throw new IllegalArgumentException("Unsupported format: " + selector.format);
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private void processBranch(NessieApiV2 api, Reference ref) {
    if (!(ref instanceof Branch)) {
      return;
    }
    var selector = keySelector.regularKeySelector;
    if (selector.isSingleKey()) {
      processSingleKey(api, ref, selector.asContentKey());
    } else {
      processManyKeys(api, ref);
    }
  }

  private void processManyKeys(NessieApiV2 api, Reference ref) {
    try {
      var selector = keySelector.regularKeySelector;
      var listRequest = api.getEntries().reference(ref).withContent(false);
      if (!selector.keyElements.isEmpty()) {
        listRequest.prefixKey(selector.asContentKey());
      }
      var batches = partitionKeys(listRequest.stream().map(Entry::getName), batchSize);
      batches.forEachRemaining(batch -> processBatch(api, (Branch) ref, batch));
    } catch (NessieNotFoundException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void processSingleKey(NessieApiV2 api, Reference ref, ContentKey key) {
    processBatch(api, (Branch) ref, singletonList(key));
  }

  protected Iterator<List<ContentKey>> partitionKeys(Stream<ContentKey> input, int batchSize) {
    return Iterators.partition(input.iterator(), batchSize);
  }

  private void processLines(NessieApiV2 api, Branch ref, InputStream input) {
    var parser = keySelector.fileKeySelector.keyParser();
    var batches =
        Iterators.partition(
            new BufferedReader(new InputStreamReader(input)).lines().map(parser).iterator(),
            batchSize);
    batches.forEachRemaining(keys -> processBatch(api, ref, keys));
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

      String refName =
          branchSelector.ref == null ? node.required("reference").asText() : branchSelector.ref;

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
