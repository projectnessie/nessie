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

import static org.projectnessie.versioned.storage.common.objtypes.CustomObjType.dynamicCaching;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.nessie.tasks.api.TaskObj;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@Value.Immutable
@JsonSerialize(as = ImmutableBasicTaskObj.class)
@JsonDeserialize(as = ImmutableBasicTaskObj.class)
public interface BasicTaskObj extends TaskObj {
  ObjType TYPE =
      dynamicCaching(
          "basic",
          "basic",
          BasicTaskObj.class,
          TaskObj.taskDefaultCacheExpire(),
          c -> ObjType.NOT_CACHED);

  @Override
  @Value.Default
  default ObjType type() {
    return TYPE;
  }

  String taskParameter();

  @Nullable
  String taskResult();

  interface Builder extends TaskObj.Builder {

    @Override
    Builder id(ObjId id);

    @CanIgnoreReturnValue
    Builder taskParameter(String taskParameter);

    @CanIgnoreReturnValue
    Builder taskResult(String taskResult);

    @Override
    @CanIgnoreReturnValue
    Builder taskState(TaskState taskState);

    BasicTaskObj build();
  }

  static Builder builder() {
    return ImmutableBasicTaskObj.builder();
  }
}
