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
package org.projectnessie.versioned.store;

import org.projectnessie.model.Content;
import org.projectnessie.nessie.relocated.protobuf.ByteString;

/**
 * Content serializers provide persistence layer (de)serialization functionality for a specific
 * content type.
 */
public interface ContentSerializer<C extends Content> {

  /**
   * Content type names are assigned via the projectnessie project, see the <a
   * href="https://projectnessie.org/develop/content-types">project website</a>.
   */
  Content.Type contentType();

  /**
   * Payload IDs are assigned via the projectnessie project, see the <a
   * href="https://projectnessie.org/develop/content-types">project website</a>.
   */
  int payload();

  ByteString toStoreOnReferenceState(C content);

  C valueFromStore(ByteString onReferenceValue);
}
