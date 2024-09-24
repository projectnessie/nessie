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
package org.projectnessie.services.impl;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.model.FetchOption.MINIMAL;
import static org.projectnessie.versioned.RequestMeta.API_READ;

import org.junit.jupiter.api.Test;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;

public abstract class AbstractTestInvalidRefs extends BaseTestServiceImpl {

  @Test
  public void testUnknownHashesOnValidNamedRefs() throws BaseNessieClientServerException {
    Branch branch = createBranch("testUnknownHashesOnValidNamedRefs");
    String invalidHash = "1234567890123456";

    int commits = 10;

    String currentHash = branch.getHash();
    createCommits(branch, 1, commits, currentHash);
    assertThatThrownBy(() -> commitLog(branch.getName(), MINIMAL, null, invalidHash, null))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(String.format("Commit '%s' not found", invalidHash));

    assertThatThrownBy(() -> entries(branch.getName(), invalidHash))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(String.format("Commit '%s' not found", invalidHash));

    assertThatThrownBy(() -> contents(branch.getName(), invalidHash, ContentKey.of("table0")))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(String.format("Commit '%s' not found", invalidHash));

    assertThatThrownBy(
            () ->
                contentApi()
                    .getContent(
                        ContentKey.of("table0"), branch.getName(), invalidHash, false, API_READ))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessageContaining(String.format("Commit '%s' not found", invalidHash));
  }
}
