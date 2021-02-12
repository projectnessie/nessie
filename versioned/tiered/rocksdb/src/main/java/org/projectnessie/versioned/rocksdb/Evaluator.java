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
package org.projectnessie.versioned.rocksdb;

import java.util.List;

/**
 * Provides evaluation of a collection of {@link org.projectnessie.versioned.rocksdb.Function} against the
 * implementing class.
 */
interface Evaluator {
  /**
   * Checks that each Function in the collection is met by the implementing class.
   * @param functions the functions to check
   * @return true if the functions are met
   */
  boolean evaluate(List<Function> functions);
}
