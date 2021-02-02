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
package com.dremio.nessie.versioned.store.rocksdb;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * A set of conditions that are asserted against an Entity.
 */
class Condition {

  private List<Function> functionList;

  Condition() {
    this.functionList = new ArrayList<>();
  }

  Condition(List<Function> functions) {
    this.functionList = functions;
  }

  void add(Function holder) {
    functionList.add(holder);
  }

  ImmutableList<Function> getFunctionList() {
    return ImmutableList.copyOf(functionList);
  }
}
