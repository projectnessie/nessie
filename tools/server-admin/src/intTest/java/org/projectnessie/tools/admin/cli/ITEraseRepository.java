/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.tools.admin.cli;

import static org.assertj.core.groups.Tuple.tuple;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.logic.ReferencesQuery.referencesQuery;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;
import org.projectnessie.versioned.storage.common.logic.PagedResult;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

@QuarkusMainTest
@TestProfile(BaseConfigProfile.class)
@ExtendWith({NessieServerAdminTestExtension.class, SoftAssertionsExtension.class})
class ITEraseRepository {

  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  public void testErase(QuarkusMainLauncher launcher, Persist persist) {
    String confirmationCode = EraseRepository.getConfirmationCode(persist);
    soft.assertThat(
            launcher.launch("erase-repository", "--confirmation-code", confirmationCode).exitCode())
        .isEqualTo(0);

    soft.assertThat(repositoryLogic(persist).fetchRepositoryDescription()).isNull();

    ReferenceLogic referenceLogic = referenceLogic(persist);
    PagedResult<Reference, String> iter = referenceLogic.queryReferences(referencesQuery());
    soft.assertThat(iter).isExhausted();
  }

  @Test
  public void testReInit(QuarkusMainLauncher launcher, Persist persist) {
    String confirmationCode = EraseRepository.getConfirmationCode(persist);
    soft.assertThat(
            launcher
                .launch(
                    "erase-repository",
                    "--re-initialize",
                    "test123",
                    "--confirmation-code",
                    confirmationCode)
                .exitCode())
        .isEqualTo(0);

    RepositoryDescription repoDesc = repositoryLogic(persist).fetchRepositoryDescription();
    soft.assertThat(repoDesc).isNotNull();

    ReferenceLogic referenceLogic = referenceLogic(persist);
    PagedResult<Reference, String> iter = referenceLogic.queryReferences(referencesQuery());
    soft.assertThat(Lists.newArrayList(iter))
        .extracting(Reference::name, Reference::pointer, Reference::deleted)
        .containsExactly(tuple("refs/heads/" + repoDesc.defaultBranchName(), EMPTY_OBJ_ID, false));
  }
}
