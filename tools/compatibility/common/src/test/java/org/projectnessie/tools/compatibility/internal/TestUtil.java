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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;

class TestUtil {
  @Test
  void throwUnchecked() {
    assertThatThrownBy(
            () -> {
              throw Util.throwUnchecked(new IllegalStateException("foo"));
            })
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("foo");
    assertThatThrownBy(
            () -> {
              throw Util.throwUnchecked(new IOException("foo"));
            })
        .isInstanceOf(RuntimeException.class)
        .extracting(Throwable::getCause)
        .isInstanceOf(IOException.class)
        .extracting(Throwable::getMessage)
        .isEqualTo("foo");
  }

  @Test
  void classContextFail() {
    ExtensionContext ctxEngine = mock(ExtensionContext.class);

    when(ctxEngine.getParent()).thenReturn(Optional.empty());
    when(ctxEngine.getRoot()).thenReturn(ctxEngine);
    when(ctxEngine.getUniqueId()).thenReturn("[engine:my-engine]");
    when(ctxEngine.getUniqueId()).thenReturn("[engine:my-engine]");

    assertThatThrownBy(() -> Util.classContext(ctxEngine))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no class part");
  }

  @Test
  void classContext() {
    ExtensionContext ctxEngine = mock(ExtensionContext.class);
    ExtensionContext ctxClass = mock(ExtensionContext.class);
    ExtensionContext ctxMethod = mock(ExtensionContext.class);

    when(ctxEngine.getParent()).thenReturn(Optional.empty());
    when(ctxEngine.getRoot()).thenReturn(ctxEngine);
    when(ctxEngine.getUniqueId()).thenReturn("[engine:my-engine]");

    when(ctxClass.getParent()).thenReturn(Optional.of(ctxEngine));
    when(ctxClass.getRoot()).thenReturn(ctxEngine);
    when(ctxClass.getUniqueId()).thenReturn("[engine:my-engine]/[class:some.package.ClassName]");

    when(ctxMethod.getParent()).thenReturn(Optional.of(ctxClass));
    when(ctxMethod.getRoot()).thenReturn(ctxEngine);
    when(ctxMethod.getUniqueId())
        .thenReturn("[engine:my-engine]/[class:some.package.ClassName]/[test:fooBar]");

    assertThat(Util.classContext(ctxMethod)).isSameAs(ctxClass);
  }

  @Test
  void withClassLoaderThrowing() throws Exception {
    ClassLoader classLoader =
        new URLClassLoader(new URL[] {new File("some-non-existing.jar").toURI().toURL()});
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

    assertThatThrownBy(
            () ->
                Util.withClassLoader(
                    classLoader,
                    () -> {
                      throw new IllegalArgumentException("blah");
                    }))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("blah");
    assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(contextClassLoader);
  }

  @Test
  void withClassLoader() throws Exception {
    ClassLoader classLoader =
        new URLClassLoader(new URL[] {new File("some-non-existing.jar").toURI().toURL()});
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

    assertThat(
            Util.withClassLoader(classLoader, () -> Thread.currentThread().getContextClassLoader()))
        .isSameAs(classLoader);
    assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(contextClassLoader);
  }
}
