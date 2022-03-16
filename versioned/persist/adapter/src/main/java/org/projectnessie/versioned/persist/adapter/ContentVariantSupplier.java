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
package org.projectnessie.versioned.persist.adapter;

import com.google.protobuf.ByteString;

public interface ContentVariantSupplier {
  ContentVariant getContentVariant(byte type);

  /**
   * Returns true if the given type is a NAMESPACE, false otherwise.
   *
   * @param type A potential type within a {@link ByteString}
   * @return true if the given type is a NAMESPACE, false otherwise.
   */
  default boolean isNamespace(ByteString type) {
    // this be seen as a temporary workaround to determine whether something is a namespace or
    // not from within the DatabaseAdapter (as currently such knowledge only lives within a
    // store worker)
    return false;
  }
}
