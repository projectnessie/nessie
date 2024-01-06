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
package org.projectnessie.nessie.tasks.service.tasktypes;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.hash.Hashing;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.immutables.value.Value;
import org.projectnessie.nessie.tasks.api.TaskObj;
import org.projectnessie.nessie.tasks.api.TaskRequest;
import org.projectnessie.nessie.tasks.api.TaskType;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@Value.Immutable
public interface BasicTaskRequest extends TaskRequest {
  @Override
  @Value.NonAttribute
  default TaskType taskType() {
    return TASK_TYPE;
  }

  @Value.Parameter(order = 1)
  String taskParameter();

  @Value.Parameter(order = 2)
  ObjId objId();

  @Value.Parameter(order = 3)
  Supplier<CompletionStage<TaskObj.Builder>> taskCompletionStageSupplier();

  TaskType TASK_TYPE = () -> BasicTaskObj.TYPE;

  static BasicTaskRequest basicTaskRequest(
      String taskParameter,
      Supplier<CompletionStage<TaskObj.Builder>> taskCompletionStageSupplier) {
    ObjId objId =
        ObjId.objIdFromByteArray(
            Hashing.sha256()
                .newHasher()
                .putString(TASK_TYPE.name(), UTF_8)
                .putString(taskParameter, UTF_8)
                .hash()
                .asBytes());
    return ImmutableBasicTaskRequest.of(taskParameter, objId, taskCompletionStageSupplier);
  }
}
