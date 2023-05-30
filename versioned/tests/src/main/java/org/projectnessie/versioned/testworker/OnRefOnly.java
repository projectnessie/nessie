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
package org.projectnessie.versioned.testworker;

import com.fasterxml.jackson.annotation.JsonTypeName;
import org.immutables.value.Value;
import org.projectnessie.model.Content;
import org.projectnessie.model.types.ContentTypes;
import org.projectnessie.nessie.relocated.protobuf.ByteString;

/** Content with on-reference state only. */
@Value.Immutable
@JsonTypeName("ON_REF_ONLY")
public abstract class OnRefOnly extends Content {

  public static final Content.Type ON_REF_ONLY = ContentTypes.forName("ON_REF_ONLY");

  public static OnRefOnly onRef(String onRef, String contentId) {
    return ImmutableOnRefOnly.builder().onRef(onRef).id(contentId).build();
  }

  public static OnRefOnly newOnRef(String onRef) {
    return ImmutableOnRefOnly.builder().onRef(onRef).build();
  }

  public abstract String getOnRef();

  @Override
  public Content.Type getType() {
    return ON_REF_ONLY;
  }

  public ByteString serialized() {
    String id = getId();
    if (id == null) {
      id = "";
    }
    return ByteString.copyFromUtf8(getType().name() + ":" + id + ":" + getOnRef());
  }

  @Override
  public abstract OnRefOnly withId(String id);
}
