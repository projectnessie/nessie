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
package com.dremio.nessie.versioned.store.mongodb;

import java.util.List;

/**
 * specifies the attributes of an index for a collection.
 */
public class Index {
  private final List<String> keys;
  private final boolean isUnique;
  private final boolean ascending;

  /**
   * A POJO representing the attributes of an index for a collection.
   * @param keys A list of keys on which indices will be applied.
   * @param isUnique if the key is has to be unique.
   * @param order true is order is ascending.
   */
  public Index(
      List<String> keys,
      boolean isUnique,
      boolean order) {
    this.keys = keys;
    this.isUnique = isUnique;
    this.ascending = order;
  }

  // todo:sdave change it to immutable list
  public List<String> getKeys() {
    return keys;
  }

  public boolean isUnique() {
    return isUnique;
  }

  public boolean isAscending() {
    return ascending;
  }
}
