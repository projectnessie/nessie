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

import org.junit.jupiter.api.Nested;

/** Groups base and logic tests. */
public abstract class AbstractPersistTests {
  @Nested
  @SuppressWarnings("ClassCanBeStatic")
  public class BaseTests extends AbstractBasePersistTests {}

  @Nested
  @SuppressWarnings("ClassCanBeStatic")
  public class CommitLogicTests extends AbstractCommitLogicTests {}

  @Nested
  @SuppressWarnings("ClassCanBeStatic")
  public class IndexesLogicTests extends AbstractIndexesLogicTests {}

  @Nested
  @SuppressWarnings("ClassCanBeStatic")
  public class ReferencesLogicTests extends AbstractReferenceLogicTests {}

  @Nested
  @SuppressWarnings("ClassCanBeStatic")
  public class RepositoryLogicTests extends AbstractRepositoryLogicTests {}

  @Nested
  @SuppressWarnings("ClassCanBeStatic")
  public class BackendRepositoryTests extends AbstractBackendRepositoryTests {}
}
