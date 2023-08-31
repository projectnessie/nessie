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
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsResponse;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Reads content objects. */
@Command(name = "content", mixinStandardHelpOptions = true, description = "Read content objects")
public class ReadContent extends AbstractCommand {

  @Option(
      names = {"-r", "--ref"},
      description = "Name of the branch/tag to read content from, defaults to 'main'")
  private String ref = "main";

  @Option(
      names = {"-H", "--hash"},
      description =
          "Hash of the commit to read content from, defaults to HEAD. Relative lookups are accepted.")
  private String hash;

  @Option(
      names = {"-k", "--key"},
      description = "Content key to use",
      required = true)
  private List<String> key;

  @Override
  public void execute() throws NessieNotFoundException {
    try (NessieApiV2 api = createNessieApiInstance()) {
      ContentKey contentKey = ContentKey.of(key);
      spec.commandLine()
          .getOut()
          .printf(
              "Reading content for key '%s' on reference '%s' @ %s...%n%n",
              contentKey, ref, hash == null ? "HEAD" : hash);
      GetMultipleContentsResponse contents =
          api.getContent().refName(ref).hashOnRef(hash).key(contentKey).getWithResponse();
      Map<ContentKey, Content> contentMap = contents.toContentsMap();
      spec.commandLine().getOut().printf("Content at '%s'\n", contents.getEffectiveReference());
      for (Map.Entry<ContentKey, Content> entry : contentMap.entrySet()) {
        spec.commandLine().getOut().printf("Key: %s\n", entry.getKey());
        if (isVerbose()) {
          List<String> key = entry.getKey().getElements();
          for (int i = 0; i < key.size(); i++) {
            spec.commandLine().getOut().printf("  key[%d]: %s\n", i, key.get(i));
          }
        }
        spec.commandLine().getOut().printf("Value: %s\n", entry.getValue());
      }
      spec.commandLine()
          .getOut()
          .printf(
              "%nDone reading content for key '%s' on reference '%s' @ %s.%n%n",
              contentKey, ref, hash == null ? "HEAD" : hash);
    }
  }
}
