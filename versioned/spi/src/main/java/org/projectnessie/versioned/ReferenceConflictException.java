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
package org.projectnessie.versioned;

import static java.util.Collections.singletonList;
import static org.projectnessie.error.ReferenceConflicts.referenceConflicts;

import java.util.Collection;
import java.util.stream.Collectors;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Conflict;

/**
 * Exception thrown when the hash associated with a named reference does not match with the hash
 * provided by the caller.
 */
public class ReferenceConflictException extends VersionStoreException {
  private static final long serialVersionUID = 4381980193289615523L;

  private final ReferenceConflicts referenceConflicts;

  public ReferenceConflictException(String message) {
    this(null, message);
  }

  public ReferenceConflictException(Conflict conflict) {
    this(singletonList(conflict));
  }

  public ReferenceConflictException(Collection<Conflict> conflicts) {
    this(referenceConflicts(conflicts), buildMessage(conflicts));
  }

  private static String buildMessage(Collection<Conflict> conflicts) {
    if (conflicts.size() == 1) {
      String msg = conflicts.iterator().next().message();
      return Character.toUpperCase(msg.charAt(0)) + msg.substring(1) + '.';
    }
    return "There are multiple conflicts that prevent committing the provided operations: "
        + conflicts.stream().map(Conflict::message).collect(Collectors.joining(", "))
        + ".";
  }

  public ReferenceConflictException(ReferenceConflicts referenceConflicts, String message) {
    super(message);
    this.referenceConflicts = referenceConflicts;
  }

  public ReferenceConflictException(String message, Throwable t) {
    super(message, t);
    this.referenceConflicts = null;
  }

  public ReferenceConflicts getReferenceConflicts() {
    return referenceConflicts;
  }
}
