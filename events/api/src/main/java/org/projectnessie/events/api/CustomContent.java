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
package org.projectnessie.events.api;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.projectnessie.events.api.json.CustomContentDeserializer;
import org.projectnessie.events.api.json.CustomContentSerializer;

/**
 * A custom {@link Content} whose runtime type is not known.
 *
 * <p>This special content subtype is only used when deserializing unknown content types. Since
 * contents are pluggable in Nessie, this situation can happen when a custom content other than the
 * built-in ones is registered server-side, but the client deserializing the content does not have
 * the concrete class available on its classpath.
 *
 * <p>All properties of the original content object will be deserialized in the {@link
 * #getProperties()} map.
 */
@Value.Immutable
@JsonTypeName("CUSTOM")
@JsonSerialize(using = CustomContentSerializer.class)
@JsonDeserialize(using = CustomContentDeserializer.class)
public interface CustomContent extends Content {

  @Override
  @Value.Default
  default ContentType getType() {
    return ContentType.CUSTOM;
  }

  /** The actual runtime type name of this content object. */
  String getCustomType();
}
