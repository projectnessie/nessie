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
package org.projectnessie.quarkus.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;

@QuarkusMainTest
@TestProfile(QuarkusCliTestProfileMongo.class)
@ExtendWith(NessieCliTestExtension.class)
class ITEraseRepository {
  private static final String CONFIRMATION_CODE = "vfvm68"; // stable for a given adapter config

  @Test
  public void testErase(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws ReferenceNotFoundException {
    assertThat(
            launcher
                .launch("erase-repository", "--confirmation-code", CONFIRMATION_CODE)
                .exitCode())
        .isEqualTo(0);
    try (Stream<ReferenceInfo<ByteString>> refs = adapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      assertThat(refs).isEmpty();
    }
  }

  @Test
  public void testReInit(QuarkusMainLauncher launcher, DatabaseAdapter adapter)
      throws ReferenceNotFoundException {
    assertThat(
            launcher
                .launch(
                    "erase-repository",
                    "--re-initialize",
                    "test123",
                    "--confirmation-code",
                    CONFIRMATION_CODE)
                .exitCode())
        .isEqualTo(0);
    ReferenceInfo<ByteString> ref = adapter.namedRef("test123", GetNamedRefsParams.DEFAULT);
    assertThat(ref.getHash()).isNotNull();
  }
}
