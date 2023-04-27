/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.model.types;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.projectnessie.model.Content;

@Value.Immutable
@JsonSerialize(as = ImmutableCustomTestContent.class)
@JsonDeserialize(as = ImmutableCustomTestContent.class)
@JsonTypeName(CustomTestContent.TYPE)
public abstract class CustomTestContent extends Content {
  static final String TYPE = "TEST_CUSTOM_CONTENT_TYPE";

  public abstract long getSomeLong();

  public abstract String getSomeString();

  @Override
  public Type getType() {
    return ContentTypes.forName(TYPE);
  }
}
