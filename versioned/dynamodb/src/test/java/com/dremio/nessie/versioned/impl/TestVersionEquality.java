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
package com.dremio.nessie.versioned.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.dremio.nessie.versioned.impl.InternalBranch.UpdateState;

class TestVersionEquality {

  /**
   * Make sure that a new branch has the L1.EMPTY_ID identifier.
   */
  @Test
  void internalBranchL1IdEqualsEmpty() {
    InternalBranch b = new InternalBranch("n/a");
    DynamoStore store = Mockito.mock(DynamoStore.class);
    Mockito.when(store.loadSingle(Mockito.any(), Mockito.any())).thenReturn(L1.EMPTY);
    UpdateState us = b.getUpdateState(store);
    us.ensureAvailable(null, null, 1, true);
    assertEquals(L1.EMPTY_ID, us.getL1().getId());
  }

  @Test
  void correctSize() {
    assertEquals(0, L1.EMPTY.size());
    assertEquals(0, L2.EMPTY.size());
    assertEquals(0, L3.EMPTY.size());
  }
}
