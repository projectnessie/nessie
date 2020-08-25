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

import java.util.List;
import java.util.Optional;

import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.ReferenceConflictException;

public class InconsistentValue {

  private final Key key;
  private final Optional<Id> expected;
  private final Optional<Id> actual;

  public InconsistentValue(Key key, Optional<Id> expected, Optional<Id> actual) {
    super();
    this.key = key;
    this.expected = expected;
    this.actual = actual;
  }

  public static class InconsistentValueException extends ReferenceConflictException {
    private final List<InconsistentValue> inconsistentValues;

    public InconsistentValueException(List<InconsistentValue> inconsistentValues) {
      super(String.format("Unable to complete operation. Found %d inconsistent value(s).", inconsistentValues.size()));
      this.inconsistentValues = inconsistentValues;
    }

    public List<InconsistentValue> getInconsistentValues() {
      return inconsistentValues;
    }

  }


}
