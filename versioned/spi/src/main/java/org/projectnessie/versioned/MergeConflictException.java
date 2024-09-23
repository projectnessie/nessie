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

import static org.projectnessie.error.ReferenceConflicts.referenceConflicts;

import java.util.List;
import java.util.stream.Collectors;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.versioned.MergeTransplantResultBase.KeyDetails;

/**
 * Special case of a {@link ReferenceConflictException} indicating that a non-dry-run merge or
 * transplant operation could not be completed due to conflicts.
 */
public class MergeConflictException extends ReferenceConflictException {

  private final MergeTransplantResultBase mergeResult;

  public MergeConflictException(String message, MergeTransplantResultBase mergeResult) {
    super(referenceConflicts(asReferenceConflicts(mergeResult)), message);
    this.mergeResult = mergeResult;
  }

  private static List<Conflict> asReferenceConflicts(MergeTransplantResultBase mergeResult) {
    return mergeResult.getDetails().entrySet().stream()
        .map(
            e -> {
              KeyDetails keyDetails = e.getValue();
              Conflict conflict = keyDetails.getConflict();
              return conflict != null
                  ? conflict
                  : Conflict.conflict(ConflictType.KEY_CONFLICT, e.getKey(), "UNKNOWN");
            })
        .collect(Collectors.toList());
  }

  public MergeTransplantResultBase getMergeResult() {
    return mergeResult;
  }
}
