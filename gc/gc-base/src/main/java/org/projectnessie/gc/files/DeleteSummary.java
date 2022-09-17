/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.files;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.immutables.value.Value;

@Value.Immutable
public interface DeleteSummary {

  DeleteSummary EMPTY = DeleteSummary.of(0, 0);

  static DeleteSummary of(long deleted, long failures) {
    return ImmutableDeleteSummary.of(deleted, failures);
  }

  @CanIgnoreReturnValue
  default DeleteSummary add(DeleteResult deleteResult) {
    switch (deleteResult) {
      case SUCCESS:
        return of(deleted() + 1L, failures());
      case FAILURE:
        return of(deleted(), failures() + 1L);
      default:
        throw new IllegalArgumentException("" + deleteResult);
    }
  }

  @CanIgnoreReturnValue
  default DeleteSummary add(DeleteSummary b) {
    return of(deleted() + b.deleted(), failures() + b.failures());
  }

  /** Number of successful deletes. */
  @Value.Parameter(order = 1)
  long deleted();

  /** Number of deletes that failed. */
  @Value.Parameter(order = 2)
  long failures();
}
