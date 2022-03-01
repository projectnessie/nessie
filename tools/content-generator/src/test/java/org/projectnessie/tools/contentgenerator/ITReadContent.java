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
package org.projectnessie.tools.contentgenerator;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.tools.contentgenerator.cli.NessieContentGenerator;

class ITReadContent extends AbstractContentGeneratorTest {

  private final String contentId = "testContentId-" + UUID.randomUUID();
  private Branch branch;

  @BeforeEach
  void setup() throws NessieConflictException, NessieNotFoundException {
    try (NessieApiV1 api = buildNessieApi()) {
      branch = makeCommit(api, contentId);
    }
  }

  @Test
  void readContent() throws UnsupportedEncodingException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (PrintWriter out = new PrintWriter(new PrintStream(baos), true)) {
      assertThat(
              NessieContentGenerator.runMain(
                  out,
                  new String[] {
                    "content",
                    "--uri",
                    NESSIE_API_URI,
                    "--ref",
                    branch.getName(),
                    "--key",
                    CONTENT_KEY.getElements().get(0),
                    "--key",
                    CONTENT_KEY.getElements().get(1)
                  }))
          .isEqualTo(0);

      String[] output = baos.toString(StandardCharsets.UTF_8.toString()).split("\n");
      assertThat(output).anySatisfy(s -> assertThat(s).contains(contentId));
      assertThat(output).anySatisfy(s -> assertThat(s).contains(CONTENT_KEY.toString()));
    }
  }

  @Test
  void readContentVerbose() throws UnsupportedEncodingException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (PrintWriter out = new PrintWriter(new PrintStream(baos), true)) {
      assertThat(
              NessieContentGenerator.runMain(
                  out,
                  new String[] {
                    "content",
                    "--uri",
                    NESSIE_API_URI,
                    "--ref",
                    branch.getName(),
                    "--verbose",
                    "--key",
                    CONTENT_KEY.getElements().get(0),
                    "--key",
                    CONTENT_KEY.getElements().get(1)
                  }))
          .isEqualTo(0);

      String[] output = baos.toString(StandardCharsets.UTF_8.toString()).split("\n");
      assertThat(output).anySatisfy(s -> assertThat(s).contains(contentId));
      assertThat(output).anySatisfy(s -> assertThat(s).contains(CONTENT_KEY.toString()));
      assertThat(output)
          .anySatisfy(s -> assertThat(s).contains("key[0]: " + CONTENT_KEY.getElements().get(0)));
      assertThat(output)
          .anySatisfy(s -> assertThat(s).contains("key[1]: " + CONTENT_KEY.getElements().get(1)));
    }
  }
}
