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
package org.projectnessie.server.store;

import com.google.protobuf.ByteString;
import java.util.function.Supplier;
import org.projectnessie.model.Content;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.model.Namespace;
import org.projectnessie.server.store.proto.ObjectTypes;

public final class NamespaceSerializer extends BaseSerializer<Namespace> {

  @Override
  public String contentType() {
    return Content.Type.NAMESPACE.name();
  }

  @Override
  public Content.Type getType(byte payload, ByteString onReferenceValue) {
    return Content.Type.NAMESPACE;
  }

  @Override
  protected void toStoreOnRefState(Namespace content, ObjectTypes.Content.Builder builder) {
    builder.setNamespace(
        ObjectTypes.Namespace.newBuilder()
            .addAllElements(content.getElements())
            .putAllProperties(content.getProperties())
            .build());
  }

  @Override
  public Namespace applyId(Namespace content, String id) {
    return ImmutableNamespace.builder().from(content).id(id).build();
  }

  @Override
  protected Namespace valueFromStore(
      ObjectTypes.Content content, Supplier<ByteString> globalState) {
    return valueFromStoreNamespace(content);
  }
}
