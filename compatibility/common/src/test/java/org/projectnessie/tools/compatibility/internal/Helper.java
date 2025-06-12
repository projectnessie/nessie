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
package org.projectnessie.tools.compatibility.internal;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.engine.support.store.Namespace;
import org.junit.platform.engine.support.store.NamespacedHierarchicalStore;

final class Helper {
  @SuppressWarnings("deprecation")
  static final NamespacedHierarchicalStore.CloseAction<Namespace> CLOSE_RESOURCES =
      (namespace, key, value) -> {
        if (value instanceof ExtensionContext.Store.CloseableResource) {
          ((ExtensionContext.Store.CloseableResource) value).close();
        }
        if (value instanceof AutoCloseable) {
          ((AutoCloseable) value).close();
        }
      };
}
