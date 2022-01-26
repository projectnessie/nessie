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
package org.projectnessie.jaxrs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

public abstract class AbstractRestAssign extends AbstractRest {
  protected AbstractRestAssign(Supplier<NessieApiV1> api) {
    super(api);
  }

  /** Assigning a branch/tag to a fresh main without any commits didn't work in 0.9.2 */
  @Test
  public void testAssignRefToFreshMain() throws BaseNessieClientServerException {
    Reference main = getApi().getReference().refName("main").get();
    // make sure main doesn't have any commits
    LogResponse log = getApi().getCommitLog().refName(main.getName()).get();
    assertThat(log.getLogEntries()).isEmpty();

    Branch testBranch = createBranch("testBranch");
    getApi().assignBranch().branch(testBranch).assignTo(main).assign();
    Reference testBranchRef = getApi().getReference().refName(testBranch.getName()).get();
    assertThat(testBranchRef.getHash()).isEqualTo(main.getHash());

    String testTag = "testTag";
    Reference testTagRef =
        getApi()
            .createReference()
            .sourceRefName(main.getName())
            .reference(Tag.of(testTag, main.getHash()))
            .create();
    assertThat(testTagRef.getHash()).isNotNull();
    getApi().assignTag().hash(testTagRef.getHash()).tagName(testTag).assignTo(main).assign();
    testTagRef = getApi().getReference().refName(testTag).get();
    assertThat(testTagRef.getHash()).isEqualTo(main.getHash());
  }
}
