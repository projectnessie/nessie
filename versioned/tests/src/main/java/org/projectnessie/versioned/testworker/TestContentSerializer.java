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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Supplier;
import org.projectnessie.model.Content;
import org.projectnessie.model.types.ContentTypes;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.store.ContentSerializer;

abstract class TestContentSerializer<C extends Content> implements ContentSerializer<C> {

  @Override
  public Content.Type getType(ByteString onRefContent) {
    String serialized = onRefContent.toStringUtf8();
    int i = serialized.indexOf(':');
    if (i == -1) {
      return OnRefOnly.ON_REF_ONLY;
    }
    String typeString = serialized.substring(0, i);
    return ContentTypes.forName(typeString);
  }

  @Override
  public C valueFromStore(
      byte payload, ByteString onReferenceValue, Supplier<ByteString> globalState) {
    String serialized = onReferenceValue.toStringUtf8();

    int i = serialized.indexOf(':');
    String typeString = serialized.substring(0, i);
    serialized = serialized.substring(i + 1);
    assertThat(typeString).isEqualTo(contentType().name());

    i = serialized.indexOf(':');
    String contentId = serialized.substring(0, i);
    if (contentId.isEmpty()) {
      contentId = null;
    }
    i = serialized.indexOf(':');
    String onRef = serialized.substring(i + 1);

    ByteString global = globalState.get();
    return valueFromStore(contentId, onRef, global);
  }

  protected abstract C valueFromStore(String contentId, String onRef, ByteString global);
}
