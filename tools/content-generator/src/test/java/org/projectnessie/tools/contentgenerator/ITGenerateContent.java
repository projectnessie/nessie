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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.tools.contentgenerator.cli.NessieContentGenerator;

class ITGenerateContent extends AbstractContentGeneratorTest {

  @ParameterizedTest
  @EnumSource(Content.Type.class)
  void basicGenerateContentTest(Content.Type contentType) throws Exception {
    Assumptions.assumeTrue(contentType != Content.Type.UNKNOWN && contentType != Type.NAMESPACE);

    int numCommits = 20;

    try (NessieApiV1 api = buildNessieApi()) {

      String testCaseBranch = "type_" + contentType.name();

      assertThat(
              NessieContentGenerator.runMain(
                  new String[] {
                    "generate",
                    "-n",
                    Integer.toString(numCommits),
                    "-u",
                    NESSIE_API_URI,
                    "-D",
                    testCaseBranch,
                    "--type=" + contentType.name()
                  }))
          .isEqualTo(0);

      assertThat(api.getCommitLog().refName(testCaseBranch).get().getLogEntries())
          .hasSize(numCommits);
    }
  }
}
