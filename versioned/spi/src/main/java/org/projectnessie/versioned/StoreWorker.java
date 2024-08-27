/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned;

import org.projectnessie.model.Content;
import org.projectnessie.nessie.relocated.protobuf.ByteString;

/** A set of helpers that users of a VersionStore must implement. */
public interface StoreWorker {

  /** Returns the serialized representation of the on-reference part of the given content-object. */
  ByteString toStoreOnReferenceState(Content content);

  Content valueFromStore(int payload, ByteString onReferenceValue);
}
