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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.client.NessieClientBuilder.createClientBuilderFromSystemSettings;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

/** Base class for content generator tests. */
public class AbstractContentGeneratorTest {

  public static final String NO_ANCESTOR =
      "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d";

  static final String NESSIE_API_URI =
      format(
          "%s/api/v2",
          requireNonNull(
              System.getProperty("quarkus.http.test-url"),
              "Required system property quarkus.http.test-url is not set"));

  protected static final String COMMIT_MSG = "testMessage";
  protected static final ContentKey CONTENT_KEY = ContentKey.of("first", "second");

  @BeforeEach
  void emptyRepo() throws Exception {
    try (NessieApiV2 api = buildNessieApi()) {
      Branch defaultBranch = api.getDefaultBranch();
      api.assignReference().reference(defaultBranch).assignTo(Detached.of(NO_ANCESTOR)).assign();
      api.getAllReferences().stream()
          .forEach(
              ref -> {
                try {
                  if (ref instanceof Tag
                      || (ref instanceof Branch
                          && !ref.getName().equals(defaultBranch.getName()))) {
                    api.deleteReference().reference(ref).delete();
                  }
                } catch (NessieConflictException | NessieNotFoundException e) {
                  throw new RuntimeException(e);
                }
              });
      api.createNamespace()
          .namespace(CONTENT_KEY.getNamespace())
          .refName(defaultBranch.getName())
          .create();
    }
  }

  protected Branch makeCommit(NessieApiV2 api)
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
        .operation(Operation.Put.of(CONTENT_KEY, IcebergTable.of("testMeta", 123, 456, 789, 321)))
        .commit();
  }

  protected NessieApiV2 buildNessieApi() {
    return createClientBuilderFromSystemSettings().withUri(NESSIE_API_URI).build(NessieApiV2.class);
  }
}
