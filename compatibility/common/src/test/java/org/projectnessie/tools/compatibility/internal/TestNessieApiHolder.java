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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.tools.compatibility.api.Version.NEW_STORAGE_MODEL_WITH_COMPAT_TESTING;
import static org.projectnessie.tools.compatibility.internal.Helper.CLOSE_RESOURCES;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Proxy;
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
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.tools.compatibility.api.Version;

@ExtendWith(SoftAssertionsExtension.class)
class TestNessieApiHolder {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void currentVersionServer() {
    try (NamespacedHierarchicalStore<Namespace> valuesStore =
        new NamespacedHierarchicalStore<>(null, CLOSE_RESOURCES)) {
      Store store = new NamespaceAwareStore(valuesStore, Util.NAMESPACE);

      ExtensionContext ctx = mock(ExtensionContext.class);
      when(ctx.getRoot()).thenReturn(ctx);
      when(ctx.getStore(any(ExtensionContext.Namespace.class))).thenReturn(store);

      try (CurrentNessieApiHolder apiHolder =
          new CurrentNessieApiHolder(
              new ClientKey(
                  Version.CURRENT,
                  "org.projectnessie.client.http.NessieHttpClientBuilderImpl",
                  NessieApiV1.class,
                  ImmutableMap.of(
                      "nessie.uri", "http://127.42.42.42:19120",
                      "nessie.enable-api-compatibility-check", "false")))) {
        soft.assertThat(apiHolder)
            .extracting(AbstractNessieApiHolder::getApiInstance)
            .extracting(Object::getClass)
            .extracting(Class::getClassLoader)
            .isSameAs(Thread.currentThread().getContextClassLoader());
      }
    }
  }

  @Test
  void oldVersionServer() {
    try (NamespacedHierarchicalStore<Namespace> valuesStore =
        new NamespacedHierarchicalStore<>(null, CLOSE_RESOURCES)) {
      Store store = new NamespaceAwareStore(valuesStore, Util.NAMESPACE);

      ExtensionContext ctx = mock(ExtensionContext.class);
      when(ctx.getRoot()).thenReturn(ctx);
      when(ctx.getStore(any(ExtensionContext.Namespace.class))).thenReturn(store);

      OldNessieApiHolder apiHolder =
          new OldNessieApiHolder(
              ctx,
              new ClientKey(
                  NEW_STORAGE_MODEL_WITH_COMPAT_TESTING,
                  "org.projectnessie.client.http.HttpClientBuilder",
                  NessieApiV1.class,
                  ImmutableMap.of(
                      "nessie.uri", "http://127.42.42.42:19120",
                      "nessie.enable-api-compatibility-check", "false")));
      try {
        soft.assertThat(apiHolder)
            .satisfies(
                api -> assertThat(api.getApiInstance().getClass()).matches(Proxy::isProxyClass))
            .extracting(OldNessieApiHolder::getTranslatingApiInstance)
            .extracting(TranslatingVersionNessieApi::getOldVersionApiInstance)
            .extracting(Object::getClass)
            .extracting(Class::getClassLoader)
            .isNotSameAs(Thread.currentThread().getContextClassLoader());
      } finally {
        apiHolder.close();
      }
    }
  }
}
