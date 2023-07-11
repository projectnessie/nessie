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
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.engine.execution.ExtensionValuesStore;
import org.junit.jupiter.engine.execution.NamespaceAwareStore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.tools.compatibility.api.Version;

@ExtendWith(SoftAssertionsExtension.class)
class TestNessieServer {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void currentVersionServer() {
    ExtensionValuesStore valuesStore = new ExtensionValuesStore(null);
    NessieServer server;
    try {
      Store store = new NamespaceAwareStore(valuesStore, Util.NAMESPACE);

      ExtensionContext ctx = mock(ExtensionContext.class);
      when(ctx.getStore(any(Namespace.class))).thenReturn(store);

      ServerKey key = new ServerKey(Version.CURRENT, "In-Memory", Collections.emptyMap());

      when(ctx.getStore(any(Namespace.class))).thenReturn(store);
      soft.assertThatThrownBy(() -> NessieServer.nessieServerExisting(ctx, key))
          .isInstanceOf(NullPointerException.class)
          .hasMessageStartingWith("No Nessie server for ");

      server = NessieServer.nessieServer(ctx, key, () -> true, c -> {});
      soft.assertThat(server)
          .isInstanceOf(CurrentNessieServer.class)
          .extracting(s -> s.getUri(NessieApiV1.class))
          .isNotNull();

      when(ctx.getStore(any(Namespace.class))).thenReturn(store);
      soft.assertThat(NessieServer.nessieServerExisting(ctx, key)).isSameAs(server);
    } finally {
      valuesStore.closeAllStoredCloseableValues();
    }

    soft.assertThatThrownBy(server::close)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("was already shut down");
  }

  @ParameterizedTest
  @ValueSource(strings = {"0.55.0"})
  void oldNessieVersionServer(String nessieVersion) {
    ExtensionValuesStore valuesStore = new ExtensionValuesStore(null);
    NessieServer server;
    try {
      Store rootStore = new NamespaceAwareStore(valuesStore, Util.NAMESPACE);
      ExtensionContext rootCtx = mock(ExtensionContext.class);
      when(rootCtx.getStore(any(Namespace.class))).thenReturn(rootStore);

      Store store = new NamespaceAwareStore(valuesStore, Util.NAMESPACE);
      ExtensionContext ctx = mock(ExtensionContext.class);
      when(ctx.getRoot()).thenReturn(rootCtx);
      when(ctx.getStore(any(Namespace.class))).thenReturn(store);

      ServerKey key =
          new ServerKey(Version.parseVersion(nessieVersion), "In-Memory", Collections.emptyMap());

      when(ctx.getStore(any(Namespace.class))).thenReturn(store);
      soft.assertThatThrownBy(() -> NessieServer.nessieServerExisting(ctx, key))
          .isInstanceOf(NullPointerException.class)
          .hasMessageStartingWith("No Nessie server for ");

      server = NessieServer.nessieServer(ctx, key, () -> true, c -> {});
      soft.assertThat(server)
          .isInstanceOf(OldNessieServer.class)
          .extracting(s -> s.getUri(NessieApiV1.class))
          .isNotNull();

      when(ctx.getStore(any(Namespace.class))).thenReturn(store);
      soft.assertThat(NessieServer.nessieServerExisting(ctx, key)).isSameAs(server);
    } finally {
      valuesStore.closeAllStoredCloseableValues();
    }

    soft.assertThatThrownBy(server::close)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("was already shut down");
  }
}
