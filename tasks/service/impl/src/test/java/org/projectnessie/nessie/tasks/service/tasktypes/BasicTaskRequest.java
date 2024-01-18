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

import static org.projectnessie.nessie.tasks.service.tasktypes.BasicTaskObj.TYPE;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.immutables.value.Value;
import org.projectnessie.nessie.tasks.api.TaskBehavior;
import org.projectnessie.nessie.tasks.api.TaskRequest;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjIdHasher;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@Value.Immutable
public interface BasicTaskRequest extends TaskRequest<BasicTaskObj, BasicTaskObj.Builder> {
  @Override
  @Value.NonAttribute
  default ObjType objType() {
    return TYPE;
  }

  @Value.Parameter(order = 1)
  String taskParameter();

  @Value.Parameter(order = 2)
  ObjId objId();

  @Value.Parameter(order = 3)
  Supplier<CompletionStage<BasicTaskObj.Builder>> taskCompletionStageSupplier();

  @Override
  @Value.Parameter(order = 4)
  TaskBehavior<BasicTaskObj, BasicTaskObj.Builder> behavior();

  @Override
  @Value.NonAttribute
  default CompletionStage<BasicTaskObj.Builder> submitExecution() {
    return taskCompletionStageSupplier().get();
  }

  @Override
  default BasicTaskObj.Builder applyRequestToObjBuilder(BasicTaskObj.Builder builder) {
    return builder.taskParameter(taskParameter());
  }

  static BasicTaskRequest basicTaskRequest(
      String taskParameter,
      Supplier<CompletionStage<BasicTaskObj.Builder>> taskCompletionStageSupplier) {
    ObjId objId = ObjIdHasher.objIdHasher(TYPE.name()).hash(taskParameter).generate();
    return ImmutableBasicTaskRequest.of(
        taskParameter, objId, taskCompletionStageSupplier, BasicTaskBehavior.INSTANCE);
  }
}
