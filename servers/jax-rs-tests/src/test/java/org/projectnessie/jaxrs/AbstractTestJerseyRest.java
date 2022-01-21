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
package org.projectnessie.jaxrs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import java.net.URI;
import java.security.Principal;
import java.util.Arrays;
import java.util.function.Consumer;
import javax.ws.rs.core.SecurityContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.jaxrs.ext.NessieSecurityContext;
import org.projectnessie.jaxrs.ext.NessieUri;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;

@ExtendWith(DatabaseAdapterExtension.class)
abstract class AbstractTestJerseyRest extends AbstractTestRest {

  @NessieDbAdapter static DatabaseAdapter databaseAdapter;

  @RegisterExtension
  static org.projectnessie.jaxrs.ext.NessieJaxRsExtension server =
      new NessieJaxRsExtension(() -> databaseAdapter);

  private static URI nessieUri;

  @BeforeAll
  static void setNessieUri(@NessieUri URI uri) {
    nessieUri = uri;
  }

  @Override
  @BeforeEach
  public void setUp() {
    init(nessieUri);
  }

  static SecurityContext securityContext(String principalName) {
    Principal principal = () -> principalName;

    return new SecurityContext() {
      @Override
      public Principal getUserPrincipal() {
        return principal;
      }

      @Override
      public boolean isUserInRole(String role) {
        return false;
      }

      @Override
      public boolean isSecure() {
        return false;
      }

      @Override
      public String getAuthenticationScheme() {
        return null;
      }
    };
  }

  @Test
  public void committerAndAuthor(
      @NessieSecurityContext Consumer<SecurityContext> securityContextConsumer) throws Exception {
    Branch main = createBranch("committerAndAuthor");
    Branch merge = createBranch("committerAndAuthorMerge");
    Branch transplant = createBranch("committerAndAuthorTransplant");

    IcebergTable meta1 = IcebergTable.of("meep", 42, 42, 42, 42);
    IcebergTable meta2 = IcebergTable.of("meep_meep", 42, 42, 42, 42);
    Branch noSecurityContext =
        getApi()
            .commitMultipleOperations()
            .branchName(main.getName())
            .hash(main.getHash())
            .commitMeta(CommitMeta.builder().message("no security context").build())
            .operation(Put.of(ContentKey.of("meep"), meta1))
            .commit();
    assertThat(
            getApi()
                .getCommitLog()
                .reference(noSecurityContext)
                .maxRecords(1)
                .get()
                .getLogEntries())
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getCommitter, CommitMeta::getAuthor, CommitMeta::getMessage)
        .containsExactly(tuple("", "", "no security context"));

    securityContextConsumer.accept(securityContext("ThatNessieGuy"));

    Branch withSecurityContext =
        getApi()
            .commitMultipleOperations()
            .branchName(noSecurityContext.getName())
            .hash(noSecurityContext.getHash())
            .commitMeta(CommitMeta.builder().message("with security").build())
            .operation(Put.of(ContentKey.of("meep_meep"), meta2))
            .commit();
    assertThat(
            getApi()
                .getCommitLog()
                .reference(withSecurityContext)
                .maxRecords(2)
                .get()
                .getLogEntries())
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getCommitter, CommitMeta::getAuthor, CommitMeta::getMessage)
        .containsExactly(
            tuple("ThatNessieGuy", "ThatNessieGuy", "with security"),
            tuple("", "", "no security context"));

    securityContextConsumer.accept(securityContext("NessieHerself"));

    // Merge

    getApi().mergeRefIntoBranch().fromRef(withSecurityContext).branch(merge).merge();

    assertThat(getApi().getCommitLog().reference(merge).maxRecords(2).get().getLogEntries())
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getCommitter, CommitMeta::getAuthor, CommitMeta::getMessage)
        .containsExactly(
            tuple("NessieHerself", "ThatNessieGuy", "with security"),
            tuple("NessieHerself", "", "no security context"));

    // Transplant

    getApi()
        .transplantCommitsIntoBranch()
        .fromRefName(withSecurityContext.getName())
        .hashesToTransplant(
            Arrays.asList(noSecurityContext.getHash(), withSecurityContext.getHash()))
        .branch(transplant)
        .transplant();

    assertThat(getApi().getCommitLog().reference(transplant).maxRecords(2).get().getLogEntries())
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getCommitter, CommitMeta::getAuthor, CommitMeta::getMessage)
        .containsExactly(
            tuple("NessieHerself", "ThatNessieGuy", "with security"),
            tuple("NessieHerself", "", "no security context"));
  }
}
