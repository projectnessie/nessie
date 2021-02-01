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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public interface Evaluator {
  /**
   * Checks that each Function in the Condition is met by the implementing class.
   * @param condition the condition to check
   * @return true if the condition is met
   */
  boolean evaluate(Condition condition);

  /**
   * Split a string path to it's separate parts.
   * @param path the input string path.
   * @return the split separate parts of the path.
   */
  static List<String> splitPath(String path) {
    return Arrays.asList(path.split(Pattern.quote(".")));
  }
}
