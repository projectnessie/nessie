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

import java.util.List;
import java.util.Optional;

import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.store.Id;

import com.google.common.collect.ImmutableList;

/**
 * A description of a specific value that was different from what was expected.
 */
public class InconsistentValue {

  private final Key key;
  private final Optional<Id> expected;
  private final Optional<Id> actual;

  InconsistentValue(Key key, Optional<Id> expected, Optional<Id> actual) {
    super();
    this.key = key;
    this.expected = expected;
    this.actual = actual;
  }

  /**
   * Returned when a conflict is detected and the specific conflicts are known (not always the case).
   */
  public static class InconsistentValueException extends ReferenceConflictException {
    private static final long serialVersionUID = -1983826821815886489L;

    private final List<InconsistentValue> inconsistentValues;

    public InconsistentValueException(List<InconsistentValue> inconsistentValues) {
      super(String.format("Unable to complete operation. Found %d inconsistent value(s).", inconsistentValues.size()));
      this.inconsistentValues = ImmutableList.copyOf(inconsistentValues);
    }

    public List<InconsistentValue> getInconsistentValues() {
      return inconsistentValues;
    }

  }

  public Key getKey() {
    return key;
  }

  public Optional<Id> getExpected() {
    return expected;
  }

  public Optional<Id> getActual() {
    return actual;
  }

}
