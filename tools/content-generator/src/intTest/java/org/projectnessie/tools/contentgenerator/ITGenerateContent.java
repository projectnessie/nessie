/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.tools.contentgenerator;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.model.Content.Type.DELTA_LAKE_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;
import static org.projectnessie.tools.contentgenerator.RunContentGenerator.runGeneratorCmd;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.LogResponse;
import org.projectnessie.tools.contentgenerator.RunContentGenerator.ProcessResult;

class ITGenerateContent extends AbstractContentGeneratorTest {

  static List<Content.Type> basicGenerateContentTest() {
    return asList(ICEBERG_TABLE, ICEBERG_VIEW, DELTA_LAKE_TABLE);
  }

  @ParameterizedTest
  @MethodSource("basicGenerateContentTest")
  void basicGenerateContentTest(Content.Type contentType) throws Exception {
    int numCommits = 20;

    try (NessieApiV2 api = buildNessieApi()) {

      String testCaseBranch = "type_" + contentType.name();

      ProcessResult proc =
          runGeneratorCmd(
              "generate",
              "--author",
              "Test Author 123",
              "-n",
              Integer.toString(numCommits),
              "-u",
              NESSIE_API_URI,
              "-D",
              testCaseBranch,
              "--type=" + contentType.name());

      assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
      // 1 commit to create the namespaces
      assertThat(api.getCommitLog().refName(testCaseBranch).stream())
          .hasSize(numCommits + 1)
          .first(type(LogResponse.LogEntry.class))
          .extracting(LogResponse.LogEntry::getCommitMeta)
          .extracting(CommitMeta::getAuthor)
          .isEqualTo("Test Author 123");
    }
  }
}
