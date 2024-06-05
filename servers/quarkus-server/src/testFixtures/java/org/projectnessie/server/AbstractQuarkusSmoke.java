/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.server;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.hash.Hashing;
import java.util.Locale;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Tag;

@SuppressWarnings("resource")
@ExtendWith(QuarkusNessieClientResolver.class)
@NessieApiVersions(versions = NessieApiVersion.V2) // one version is fine
public abstract class AbstractQuarkusSmoke {
  public static final String EMPTY = Hashing.sha256().hashString("empty", UTF_8).toString();

  private NessieApiV1 api;

  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  protected final SoftAssertions soft = new SoftAssertions();

  static {
    // Note: REST tests validate some locale-specific error messages, but expect on the messages to
    // be in ENGLISH. However, the JRE's startup classes (in particular class loaders) may cause the
    // default Locale to be initialized before Maven is able to override the user.language system
    // property. Therefore, we explicitly set the default Locale to ENGLISH here to match tests'
    // expectations.
    Locale.setDefault(Locale.ENGLISH);
  }

  @BeforeEach
  void initApi(NessieClientFactory clientFactory) {
    this.api = clientFactory.make();
  }

  public NessieApiV1 api() {
    return api;
  }

  @AfterEach
  public void tearDown() throws Exception {
    try {
      // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
      // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
      soft.assertAll();
    } finally {
      Branch defaultBranch = api().getDefaultBranch();
      api()
          .assignBranch()
          .branch(defaultBranch)
          .assignTo(Branch.of(defaultBranch.getName(), EMPTY))
          .assign();
      api().getAllReferences().stream()
          .forEach(
              ref -> {
                try {
                  if (ref instanceof Branch && !ref.getName().equals(defaultBranch.getName())) {
                    api().deleteBranch().branch((Branch) ref).delete();
                  } else if (ref instanceof Tag) {
                    api().deleteTag().tag((Tag) ref).delete();
                  }
                } catch (NessieConflictException | NessieNotFoundException e) {
                  throw new RuntimeException(e);
                }
              });
      api().close();
      api = null;
    }
  }

  @Test
  public void smoke() throws Exception {
    Branch main = api().getDefaultBranch();

    Branch branch =
        (Branch)
            api()
                .createReference()
                .reference(Branch.of("branch", main.getHash()))
                .sourceRefName(main.getName())
                .create();

    ContentKey key = ContentKey.of("a-table");

    CommitResponse commitResp =
        api()
            .commitMultipleOperations()
            .branch(branch)
            .commitMeta(CommitMeta.fromMessage("commit"))
            .operation(Put.of(key, IcebergTable.of("foo", 1, 2, 3, 4)))
            .commitWithResponse();
    branch = commitResp.getTargetBranch();

    soft.assertThat(api().getCommitLog().reference(branch).get().getLogEntries()).hasSize(1);

    soft.assertThat(api().getContent().reference(branch).key(key).get())
        .containsKeys(key)
        .hasSize(1);

    soft.assertThat(api().getEntries().reference(branch).get().getEntries())
        .extracting(EntriesResponse.Entry::getName)
        .containsExactly(key);
  }
}
