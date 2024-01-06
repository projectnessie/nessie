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
package org.projectnessie.nessie.tasks.service.impl;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.projectnessie.nessie.tasks.api.TaskType;
import org.projectnessie.nessie.tasks.service.spi.TaskTypeController;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public class TypeControllerRegistry {
  public static TaskTypeController controllerForTaskType(TaskType taskType) {
    return controllerForObjType(taskType.objType());
  }

  public static TaskTypeController controllerForObjType(ObjType type) {
    return requireNonNull(TaskDefinitionsInternal.TYPE_DEFINITION_MAP.get(type));
  }

  private static final class TaskDefinitionsInternal {
    static final Map<ObjType, TaskTypeController> TYPE_DEFINITION_MAP;

    static {
      Map<ObjType, TaskTypeController> map = new HashMap<>();
      for (TaskTypeController taskTypeController : ServiceLoader.load(TaskTypeController.class)) {
        ObjType objType = taskTypeController.taskType().objType();
        TaskTypeController existing = map.putIfAbsent(objType, taskTypeController);
        checkState(
            existing == null,
            "Object type %s used for task type %s and %s",
            objType,
            taskTypeController.taskType(),
            existing != null ? existing.taskType() : null);
      }
      TYPE_DEFINITION_MAP = map;
    }
  }
}
