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
package org.projectnessie.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Schema(
    type = SchemaType.OBJECT,
    title = "NessieUserInfo",
    description = "Information about the current user.")
@Value.Immutable
@JsonSerialize(as = ImmutableNessieUserInfo.class)
@JsonDeserialize(as = ImmutableNessieUserInfo.class)
public interface NessieUserInfo {
  @Value.Parameter(order = 1)
  boolean anonymous();

  @Value.Parameter(order = 2)
  @Nullable
  @jakarta.annotation.Nullable
  String name();

  @Value.Parameter(order = 3)
  List<String> roles();

  static NessieUserInfo nessieUserInfo(boolean anonymous, String name, List<String> roles) {
    return null; // ImmutableNessieUserInfo.of(anonymous, name, roles);
  }

  static Builder builder() {
    return ImmutableNessieUserInfo.builder();
  }

  interface Builder {
    Builder anonymous(boolean anonymous);

    Builder name(@Nullable String name);

    Builder roles(Iterable<String> roles);

    Builder addRoles(String role);

    Builder addRoles(String... role);

    Builder addAllRoles(Iterable<String> role);

    NessieUserInfo build();
  }
}
