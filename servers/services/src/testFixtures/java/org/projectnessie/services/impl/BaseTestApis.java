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

import org.junit.jupiter.api.Nested;

public abstract class BaseTestApis {

  @Nested
  public class AssignTests extends AbstractTestAssign {}

  @Nested
  public class CommitsTests extends AbstractTestCommits {}

  @Nested
  public class CommitLogTests extends AbstractTestCommitLog {}

  @Nested
  public class ContentsTests extends AbstractTestContents {}

  @Nested
  public class DiffTests extends AbstractTestDiff {}

  @Nested
  public class EntriesTests extends AbstractTestEntries {}

  @Nested
  public class InvalidRefsTests extends AbstractTestInvalidRefs {}

  @Nested
  public class MergeTransplantTests extends AbstractTestMergeTransplant {}

  @Nested
  public class MiscTests extends AbstractTestMisc {}

  @Nested
  public class NamespaceTests extends AbstractTestNamespace {}

  @Nested
  public class ReferencesTests extends AbstractTestReferences {}

  @Nested
  public class AccessChecks extends AbstractTestAccessChecks {}

  @Nested
  public class Principals extends AbstractTestPrincipals {}
}
