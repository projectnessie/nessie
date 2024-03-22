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
package org.projectnessie.objectstoragemock.adlsgen2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableDataLakeStorageErrorObj.class)
@JsonDeserialize(as = ImmutableDataLakeStorageErrorObj.class)
@Value.Immutable
@Value.Style(
    defaults = @Value.Immutable(lazyhash = true),
    allParameters = true,
    forceJacksonPropertyNames = false,
    clearBuilder = true,
    depluralize = true,
    jdkOnly = true,
    get = {"get*", "is*"})
public interface DataLakeStorageErrorObj {

  @JsonProperty("Code")
  @Nullable
  String code();

  @JsonProperty("Message")
  @Nullable
  String message();
}
