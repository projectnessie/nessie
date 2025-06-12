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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.projectnessie.tools.compatibility.internal.Helper.CLOSE_RESOURCES;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.engine.execution.NamespaceAwareStore;
import org.junit.platform.engine.support.store.Namespace;
import org.junit.platform.engine.support.store.NamespacedHierarchicalStore;

@SuppressWarnings({"Convert2Lambda", "unchecked", "rawtypes"})
@ExtendWith(SoftAssertionsExtension.class)
class TestGlobalForClass {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void globalForClass() {
    try (NamespacedHierarchicalStore<Namespace> valuesStore =
        new NamespacedHierarchicalStore<>(null, CLOSE_RESOURCES)) {
      Store store = new NamespaceAwareStore(valuesStore, Util.NAMESPACE);

      ExtensionContext ctx = mock(ExtensionContext.class);
      when(ctx.getRoot()).thenReturn(ctx);
      when(ctx.getStore(any(ExtensionContext.Namespace.class))).thenReturn(store);
      when(ctx.getUniqueId()).thenReturn("[engine:meep]/[class:hello.world.MyClass]");

      GlobalForClass first = GlobalForClass.globalForClass(ctx);

      Function<String, List> listCreator =
          spy(
              new Function<String, List>() {
                @Override
                public List apply(String s) {
                  return new ArrayList<>();
                }
              });

      soft.assertThat(first.getOrCompute("my-key", listCreator, List.class))
          .isInstanceOf(List.class);
      verify(listCreator).apply("my-key");

      when(ctx.getRoot()).thenReturn(ctx);
      when(ctx.getStore(any(ExtensionContext.Namespace.class))).thenReturn(store);
      when(ctx.getUniqueId()).thenReturn("[engine:meep]/[class:hello.world.MyClass]");

      GlobalForClass second = GlobalForClass.globalForClass(ctx);

      soft.assertThat(second).isSameAs(first);

      listCreator =
          spy(
              new Function<String, List>() {
                @Override
                public List apply(String s) {
                  return new ArrayList<>();
                }
              });
      soft.assertThat(second.getOrCompute("my-key", listCreator, List.class))
          .isInstanceOf(List.class);
      verifyNoInteractions(listCreator);

      when(ctx.getRoot()).thenReturn(ctx);
      when(ctx.getStore(any(ExtensionContext.Namespace.class))).thenReturn(store);
      when(ctx.getUniqueId()).thenReturn("[engine:meep]/[class:hello.world.AnotherClass]");

      GlobalForClass forOtherClass = GlobalForClass.globalForClass(ctx);

      soft.assertThat(forOtherClass).isNotSameAs(first);
    }
  }
}
