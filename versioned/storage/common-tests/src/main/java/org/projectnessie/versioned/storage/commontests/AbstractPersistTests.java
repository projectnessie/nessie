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
package org.projectnessie.versioned.storage.commontests;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInfo;

/** Groups base and logic tests. */
public abstract class AbstractPersistTests {
  private static Class<?> surroundingTestClass;

  @BeforeAll
  static void gatherTestClass(TestInfo testInfo) {
    surroundingTestClass = testInfo.getTestClass().orElseThrow();
  }

  @Nested
  public class BaseTests extends AbstractBasePersistTests {}

  @Nested
  public class CommitLogicTests extends AbstractCommitLogicTests {}

  @Nested
  public class ConsistencyLogicTests extends AbstractConsistencyLogicTests {}

  @Nested
  public class IndexesLogicTests extends AbstractIndexesLogicTests {}

  @Nested
  public class ReferencesLogicTests extends AbstractReferenceLogicTests {
    ReferencesLogicTests() {
      super(surroundingTestClass);
    }
  }

  @Nested
  public class RepositoryLogicTests extends AbstractRepositoryLogicTests {}

  @Nested
  public class BackendRepositoryTests extends AbstractBackendRepositoryTests {}
}
