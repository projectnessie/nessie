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
package org.projectnessie.versioned.mongodb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * A test class that contains MongoDB specific tests for unsharded MongoDB.
 */
@ExtendWith(LocalMongoD.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMongoDBStore extends TestMongoDBStoreBase {
  @Override
  protected long getRandomSeed() {
    return 8612341233543L;
  }

  @Test
  @Disabled
  public void putWithConditionValue() {
  }

  @Test
  @Disabled
  void putWithConditionBranch() {
  }

  @Test
  @Disabled
  void putWithConditionTag() {
  }

  @Test
  @Disabled
  void putWithConditionCommitMetadata() {
  }

  @Test
  @Disabled
  void putWithConditionKeyFragment() {
  }

  @Test
  @Disabled
  void putWithConditionL1() {
  }

  @Test
  @Disabled
  void putWithConditionL2() {
  }

  @Test
  @Disabled
  void putWithConditionL3() {
  }

  @Test
  @Disabled
  void deleteWithConditionValue() {
  }

  @Test
  @Disabled
  void deleteWithConditionBranch() {
  }

  @Test
  @Disabled
  void deleteWithConditionTag() {
  }

  @Test
  @Disabled
  void deleteWithConditionCommitMetadata() {
  }

  @Test
  @Disabled
  void deleteConditionMismatchAttributeValue() {
  }

  @Test
  @Disabled
  void deleteConditionMismatchAttributeBranch() {
  }

  @Test
  @Disabled
  void deleteBranchSizeFail() {
  }

  @Test
  @Disabled
  void deleteBranchSizeSucceed() {
  }

  @Test
  @Disabled
  void deleteWithConditionKeyFragment() {
  }

  @Test
  @Disabled
  void deleteWithConditionL1() {
  }

  @Test
  @Disabled
  void deleteWithConditionL2() {
  }

  @Test
  @Disabled
  void deleteWithConditionL3() {
  }

  @Test
  @Disabled
  void deleteValue() {
  }

  @Test
  @Disabled
  void deleteBranch() {
  }

  @Test
  @Disabled
  void deleteTag() {
  }

  @Test
  @Disabled
  void deleteCommitMetadata() {
  }

  @Test
  @Disabled
  void deleteKeyFragment() {
  }

  @Test
  @Disabled
  void deleteL1() {
  }

  @Test
  @Disabled
  void deleteL2() {
  }

  @Test
  @Disabled
  void deleteL3() {
  }
}
