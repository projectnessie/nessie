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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.tools.contentgenerator.cli.NessieContentGenerator;

/** Base class for content generator tests. */
public class AbstractContentGeneratorTest {

  static final Integer NESSIE_HTTP_PORT = Integer.getInteger("quarkus.http.test-port");

  static final String NESSIE_API_URI =
      String.format("http://localhost:%d/api/v1", NESSIE_HTTP_PORT);

  protected static final String COMMIT_MSG = "testMessage";
  protected static final ContentKey CONTENT_KEY = ContentKey.of("first", "second");

  protected Branch makeCommit(NessieApiV1 api, String contentId)
      throws NessieConflictException, NessieNotFoundException {
    String branchName = "test-" + UUID.randomUUID();
    Branch main = api.getDefaultBranch();
    Reference branch =
        api.createReference()
            .sourceRefName(main.getName())
            .reference(Branch.of(branchName, main.getHash()))
            .create();

    return api.commitMultipleOperations()
        .branchName(branch.getName())
        .hash(branch.getHash())
        .commitMeta(CommitMeta.fromMessage(COMMIT_MSG))
        .operation(
            Operation.Put.of(
                CONTENT_KEY, IcebergTable.of("testMeta", 123, 456, 789, 321, contentId)))
        .commit();
  }

  protected NessieApiV1 buildNessieApi() {
    return HttpClientBuilder.builder()
        .fromSystemProperties()
        .withUri(NESSIE_API_URI)
        .build(NessieApiV1.class);
  }

  protected static final class ProcessResult {

    private final int exitCode;
    private final String stdOut;

    ProcessResult(int exitCode, String stdOut) {
      this.exitCode = exitCode;
      this.stdOut = stdOut;
    }

    int getExitCode() {
      return exitCode;
    }

    private String getStdOut() {
      return stdOut;
    }

    List<String> getStdOutLines() {
      return Arrays.asList(stdOut.split("\n"));
    }
  }

  protected ProcessResult runGeneratorCmd(String... params) {
    try (StringWriter stringOut = new StringWriter();
        PrintWriter out = new PrintWriter(stringOut)) {
      int exitCode = NessieContentGenerator.runMain(out, params);
      String output = stringOut.toString();
      return new ProcessResult(exitCode, output);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
