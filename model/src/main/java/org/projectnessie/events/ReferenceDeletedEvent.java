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
package org.projectnessie.events;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.projectnessie.model.Reference;

/** This event is fired every time a reference (Branch or Tag) is deleted. */
@Value.Immutable
@JsonSerialize(as = ImmutableReferenceDeletedEvent.class)
@JsonDeserialize(as = ImmutableReferenceDeletedEvent.class)
@JsonTypeName("REFERENCE_DELETED")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface ReferenceDeletedEvent extends Event {
  /** The recently deleted reference type. */
  Reference.ReferenceType getReferenceType();

  /** The recently deleted reference name. */
  String getReferenceName();

  static ImmutableReferenceDeletedEvent.Builder builder() {
    return ImmutableReferenceDeletedEvent.builder();
  }
}
