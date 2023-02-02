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
package org.projectnessie.tools.compatibility.internal;

import com.google.common.base.Throwables;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.platform.engine.UniqueId;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.api.NessieApiV2;

final class Util {

  static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(Util.class);

  private Util() {}

  static Store extensionStore(ExtensionContext context) {
    return context.getStore(NAMESPACE);
  }

  static RuntimeException throwUnchecked(Throwable e) {
    Throwables.throwIfUnchecked(e);
    return new RuntimeException(e);
  }

  static ExtensionContext classContext(ExtensionContext context) {
    for (ExtensionContext c = Objects.requireNonNull(context, "context must not be null"); ; ) {
      if ("class".equals(UniqueId.parse(c.getUniqueId()).getLastSegment().getType())) {
        return c;
      }
      Optional<ExtensionContext> parent = c.getParent();
      if (!parent.isPresent()) {
        throw new IllegalArgumentException(
            String.format("Context %s has no class part", context.getUniqueId()));
      }
      c = parent.get();
    }
  }

  static <T> T withClassLoader(ClassLoader classLoader, Callable<T> callable) throws Exception {
    ClassLoader appClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);
    try {
      return callable.call();
    } finally {
      Thread.currentThread().setContextClassLoader(appClassLoader);
    }
  }

  static URI resolve(URI base, Class<? extends NessieApi> apiType) {
    String suffix = NessieApiV2.class.isAssignableFrom(apiType) ? "v2" : "v1";
    return base.resolve(suffix);
  }
}
