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

import org.projectnessie.versioned.store.ContentSerializerBundle;
import org.projectnessie.versioned.store.ContentSerializerRegistry;

/** Serializer bundle for Iceberg tables+views, Delta Lake tables + namespaces. */
public class MainSerializerBundle implements ContentSerializerBundle {

  @Override
  public void register(ContentSerializerRegistry registry) {
    registry.register(new UnknownSerializer());
    registry.register(new IcebergTableSerializer());
    registry.register(new DeltaLakeTableSerializer());
    registry.register(new IcebergViewSerializer());
    registry.register(new NamespaceSerializer());
    registry.register(new UDFSerializer());
  }
}
