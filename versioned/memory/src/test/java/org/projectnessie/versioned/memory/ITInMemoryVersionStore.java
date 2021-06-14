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
package org.projectnessie.versioned.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ImmutablePut;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.tests.AbstractITVersionStore;

public class ITInMemoryVersionStore extends AbstractITVersionStore {
  private static final InMemoryVersionStore.Builder<String, String, StringSerializer.TestEnum>
      BUILDER =
          InMemoryVersionStore.<String, String, StringSerializer.TestEnum>builder()
              .valueSerializer(StringSerializer.getInstance())
              .metadataSerializer(StringSerializer.getInstance());

  private VersionStore<String, String, StringSerializer.TestEnum> store;

  @Override
  protected VersionStore<String, String, StringSerializer.TestEnum> store() {
    return store;
  }

  @Override
  @Disabled("NYI")
  protected void checkDiff() throws VersionStoreException {
    super.checkDiff();
  }

  @Test
  void clearUnsafe() throws Exception {
    InMemoryVersionStore<String, String, StringSerializer.TestEnum> inMemoryVersionStore =
        (InMemoryVersionStore<String, String, StringSerializer.TestEnum>) store;

    BranchName fooBranch = BranchName.of("foo");

    inMemoryVersionStore.create(fooBranch, Optional.empty());
    assertNotNull(inMemoryVersionStore.toRef("foo"));
    inMemoryVersionStore.commit(
        fooBranch,
        Optional.empty(),
        "foo",
        Collections.singletonList(
            ImmutablePut.<String>builder().key(Key.of("bar")).value("baz").build()));
    assertEquals(1L, inMemoryVersionStore.getCommits(fooBranch).count());

    inMemoryVersionStore.clearUnsafe();
    assertThrows(
        ReferenceNotFoundException.class, () -> assertNull(inMemoryVersionStore.toRef("foo")));
    assertThrows(
        ReferenceNotFoundException.class,
        () -> assertNull(inMemoryVersionStore.getCommits(fooBranch)));
  }

  @BeforeEach
  protected void beforeEach() {
    this.store = BUILDER.build();
  }

  @AfterEach
  protected void afterEach() {
    this.store = null;
  }
}
