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

import static org.projectnessie.tools.compatibility.internal.Util.classContext;
import static org.projectnessie.tools.compatibility.internal.Util.extensionStore;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.engine.UniqueId;

/**
 * Maintains properties for a test class name in the root {@link ExtensionContext}, which is needed
 * to maintain state across multiple runs of the same test class.
 */
final class GlobalForClass {

  private final Map<String, Object> properties = new HashMap<>();

  private GlobalForClass() {}

  static GlobalForClass globalForClass(ExtensionContext context) {
    ExtensionContext root = context.getRoot();
    String forClass =
        UniqueId.parse(classContext(context).getUniqueId()).getLastSegment().getValue();

    String key = String.format("%s <-- %s", forClass, GlobalForClass.class.getName());

    return extensionStore(root)
        .getOrComputeIfAbsent(key, x -> new GlobalForClass(), GlobalForClass.class);
  }

  @SuppressWarnings({"unchecked", "unused"})
  <T> T getOrCompute(String key, Function<String, T> compute, Class<T> type) {
    return (T) properties.computeIfAbsent(key, compute);
  }
}
