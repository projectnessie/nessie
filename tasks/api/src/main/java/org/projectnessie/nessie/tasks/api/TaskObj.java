/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.tasks.api;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.versioned.storage.common.persist.ObjType.CACHE_UNLIMITED;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Instant;
import org.projectnessie.versioned.storage.common.objtypes.CustomObjType.CacheExpireCalculation;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.UpdateableObj;

/** Base interface for objects that represent task state and value. */
public interface TaskObj extends UpdateableObj {
  /** Retrieve the task-state of this object. */
  TaskState taskState();

  interface Builder {

    @CanIgnoreReturnValue
    Builder from(TaskObj base);

    @CanIgnoreReturnValue
    Builder versionToken(String versionToken);

    @CanIgnoreReturnValue
    Builder id(ObjId id);

    @CanIgnoreReturnValue
    Builder type(ObjType type);

    @CanIgnoreReturnValue
    Builder taskState(TaskState taskState);

    TaskObj build();
  }

  CacheExpireCalculation<? extends TaskObj> _TASK_DEFAULT_CACHE_EXPIRE =
      (obj, currentTimeMicros) -> {
        TaskState state = obj.taskState();
        if (state.status().isFinal()) {
          return CACHE_UNLIMITED;
        }

        Instant retryNotBefore = state.retryNotBefore();
        if (retryNotBefore != null) {
          return MILLISECONDS.toMicros(retryNotBefore.toEpochMilli());
        }

        return CACHE_UNLIMITED;
      };

  @SuppressWarnings("unchecked")
  static <T extends TaskObj> CacheExpireCalculation<T> taskDefaultCacheExpire() {
    return (CacheExpireCalculation<T>) _TASK_DEFAULT_CACHE_EXPIRE;
  }
}
