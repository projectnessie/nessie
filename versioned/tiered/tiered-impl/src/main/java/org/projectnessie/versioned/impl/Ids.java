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
package org.projectnessie.versioned.impl;

import org.projectnessie.versioned.store.Id;

public final class Ids {

  private Ids() {
  }

  public static Id getEmptyL1() {
    return InternalL1.EMPTY_ID;
  }

  public static Id getEmptyL2() {
    return InternalL2.EMPTY_ID;
  }

  public static Id getEmptyL3() {
    return InternalL3.EMPTY_ID;
  }
}
