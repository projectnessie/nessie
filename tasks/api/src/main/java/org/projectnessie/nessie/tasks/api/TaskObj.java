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

import static org.projectnessie.nessie.tasks.api.TaskObjUtil.TASK_DEFAULT_CACHE_EXPIRE;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.projectnessie.versioned.storage.common.objtypes.CustomObjType.CacheExpireCalculation;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

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

  @SuppressWarnings("unchecked")
  static <T extends TaskObj> CacheExpireCalculation<T> taskDefaultCacheExpire() {
    return (CacheExpireCalculation<T>) TASK_DEFAULT_CACHE_EXPIRE;
  }
}
