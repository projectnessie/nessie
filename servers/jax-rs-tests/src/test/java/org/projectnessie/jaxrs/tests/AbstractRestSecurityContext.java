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
package org.projectnessie.jaxrs.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.ws.rs.core.SecurityContext;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.jaxrs.ext.NessieSecurityContext;
import org.projectnessie.jaxrs.ext.PrincipalSecurityContext;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation.Put;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestSecurityContext extends AbstractRestAccessChecks {

  private Branch makeCommits(Branch head, Consumer<SecurityContext> securityContextConsumer)
      throws NessieConflictException, NessieNotFoundException {
    IcebergTable meta1 = IcebergTable.of("meep", 42, 42, 42, 42);
    IcebergTable meta2 = IcebergTable.of("meep_meep", 42, 42, 42, 42);

    head =
        getApi()
            .commitMultipleOperations()
            .branchName(head.getName())
            .hash(head.getHash())
            .commitMeta(CommitMeta.builder().message("no security context").build())
            .operation(Put.of(ContentKey.of("meep"), meta1))
            .commit();
    assertThat(getApi().getCommitLog().reference(head).maxRecords(1).get().getLogEntries())
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getCommitter, CommitMeta::getAuthor, CommitMeta::getMessage)
        .containsExactly(tuple("", "", "no security context"));

    securityContextConsumer.accept(PrincipalSecurityContext.forName("ThatNessieGuy"));

    head =
        getApi()
            .commitMultipleOperations()
            .branchName(head.getName())
            .hash(head.getHash())
            .commitMeta(CommitMeta.builder().message("with security").build())
            .operation(Put.of(ContentKey.of("meep_meep"), meta2))
            .commit();
    assertThat(getApi().getCommitLog().reference(head).maxRecords(2).get().getLogEntries())
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getCommitter, CommitMeta::getAuthor, CommitMeta::getMessage)
        .containsExactly(
            tuple("ThatNessieGuy", "ThatNessieGuy", "with security"),
            tuple("", "", "no security context"));

    return head;
  }

  @Test
  public void committerAndAuthorMerge(@NessieSecurityContext Consumer<SecurityContext> secContext)
      throws Exception {
    Branch main = makeCommits(createBranch("committerAndAuthorMerge_main"), secContext);
    Branch merge = createBranch("committerAndAuthorMerge_target");

    secContext.accept(PrincipalSecurityContext.forName("NessieHerself"));

    getApi().mergeRefIntoBranch().fromRef(main).branch(merge).merge();

    merge = (Branch) getApi().getReference().refName(merge.getName()).get();

    assertThat(getApi().getCommitLog().reference(merge).maxRecords(1).get().getLogEntries())
        .extracting(LogEntry::getCommitMeta)
        .allSatisfy(
            commitMeta -> {
              assertThat(commitMeta.getCommitter()).isEqualTo("NessieHerself");
              assertThat(commitMeta.getAuthor()).isEqualTo("NessieHerself");
              assertThat(commitMeta.getMessage())
                  .contains("with security")
                  .contains("no security context");
            });
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V1)
  public void committerAndAuthorMergeUnsquashed(
      @NessieSecurityContext Consumer<SecurityContext> secContext) throws Exception {
    Branch main = makeCommits(createBranch("committerAndAuthorMergeUnsquashed_main"), secContext);
    Branch merge = createBranch("committerAndAuthorMergeUnsquashed_target");

    secContext.accept(PrincipalSecurityContext.forName("NessieHerself"));

    getApi().mergeRefIntoBranch().fromRef(main).branch(merge).keepIndividualCommits(true).merge();

    merge = (Branch) getApi().getReference().refName(merge.getName()).get();

    assertThat(getApi().getCommitLog().reference(merge).maxRecords(2).get().getLogEntries())
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getCommitter, CommitMeta::getAuthor, CommitMeta::getMessage)
        .containsExactly(
            tuple("NessieHerself", "ThatNessieGuy", "with security"),
            tuple("NessieHerself", "", "no security context"));
  }

  @Test
  public void committerAndAuthorTransplant(
      @NessieSecurityContext Consumer<SecurityContext> secContext) throws Exception {
    Branch main = makeCommits(createBranch("committerAndAuthorTransplant_main"), secContext);
    Branch transplant = createBranch("committerAndAuthorTransplant_target");

    List<String> hashesToTransplant =
        getApi().getCommitLog().reference(main).maxRecords(2).get().getLogEntries().stream()
            .map(logEntry -> logEntry.getCommitMeta().getHash())
            .collect(Collectors.toList());
    Collections.reverse(hashesToTransplant); // transplant in the original commit order

    secContext.accept(PrincipalSecurityContext.forName("NessieHerself"));

    getApi()
        .transplantCommitsIntoBranch()
        .fromRefName(main.getName())
        .hashesToTransplant(hashesToTransplant)
        .branch(transplant)
        .keepIndividualCommits(true)
        .transplant();

    transplant = (Branch) getApi().getReference().refName(transplant.getName()).get();

    assertThat(getApi().getCommitLog().reference(transplant).maxRecords(2).get().getLogEntries())
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getCommitter, CommitMeta::getAuthor, CommitMeta::getMessage)
        .containsExactly(
            tuple("NessieHerself", "ThatNessieGuy", "with security"),
            tuple("NessieHerself", "", "no security context"));
  }
}
