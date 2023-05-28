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
package org.projectnessie.versioned.store.types;

import org.projectnessie.model.Content;
import org.projectnessie.model.types.ContentTypes;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.store.ContentSerializer;

public class CustomTestContentSerializer implements ContentSerializer<CustomTestContent> {

  static final int PAYLOAD = 42;

  @Override
  public Content.Type contentType() {
    return ContentTypes.forName(CustomTestContent.TYPE);
  }

  @Override
  public int payload() {
    return PAYLOAD;
  }

  @Override
  public ByteString toStoreOnReferenceState(CustomTestContent content) {
    String id = content.getId();
    return ByteString.copyFromUtf8(
        content.getSomeString() + '|' + content.getSomeLong() + '|' + (id != null ? id : ""));
  }

  @Override
  @SuppressWarnings("StringSplitter")
  public CustomTestContent valueFromStore(ByteString onReferenceValue) {
    String[] arr = onReferenceValue.toStringUtf8().split("\\|");
    ImmutableCustomTestContent.Builder b =
        ImmutableCustomTestContent.builder().someString(arr[0]).someLong(Long.parseLong(arr[1]));
    if (arr.length >= 3 && !arr[2].isEmpty()) {
      b.id(arr[2]);
    }
    return b.build();
  }
}
