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

import com.google.protobuf.ByteString;
import java.util.Optional;

/**
 * A set of helpers that users of a VersionStore must implement.
 *
 * @param <CONTENTS> The value type saved in the VersionStore.
 * @param <COMMIT_METADATA> The commit metadata type saved in the VersionStore.
 */
public interface StoreWorker<CONTENTS, COMMIT_METADATA, CONTENTS_TYPE extends Enum<CONTENTS_TYPE>> {

  /**
   * Returns the serialized representation of the on-reference part of the given contents-object.
   */
  ByteString toStoreOnReferenceState(CONTENTS contents);

  ByteString toStoreGlobalState(CONTENTS contents);

  CONTENTS valueFromStore(ByteString onReferenceValue, Optional<ByteString> globalState);

  String getId(CONTENTS contents);

  Byte getPayload(CONTENTS contents);

  boolean requiresGlobalState(CONTENTS contents);

  CONTENTS_TYPE getType(Byte payload);

  default CONTENTS_TYPE getType(CONTENTS contents) {
    return getType(getPayload(contents));
  }

  Serializer<COMMIT_METADATA> getMetadataSerializer();
}
