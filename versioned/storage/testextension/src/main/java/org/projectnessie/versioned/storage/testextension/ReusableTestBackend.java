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
package org.projectnessie.versioned.storage.testextension;

import java.util.Objects;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.PersistLoader;

/** Allows a test class to reuse the test container used by the previous test class. */
final class ReusableTestBackend implements AutoCloseable {

  private Class<? extends BackendTestFactory> backendType;
  private String backendName;
  private BackendTestFactory backendTestFactory;
  private Backend backend;

  BackendTestFactory backendTestFactory(ExtensionContext context) {
    BackendTestFactory f = backendTestFactory;
    if (f == null) {
      NessieBackend nessieBackend =
          PersistExtension.annotationInstance(context, NessieBackend.class);
      if (nessieBackend != null) {
        try {
          backendType = nessieBackend.value();
          f = backendTestFactory = backendType.getDeclaredConstructor().newInstance();
          f.start();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    return f;
  }

  Backend backend(ExtensionContext context) {
    Backend reusable = maybeReusable(context);
    if (reusable != null) {
      return reusable;
    }

    return createNewBackend(context);
  }

  private Backend createNewBackend(ExtensionContext context) {
    backendName = null;
    backendType = null;

    NessieBackend nessieBackend = PersistExtension.annotationInstance(context, NessieBackend.class);
    if (nessieBackend != null) {
      try {
        backendType = nessieBackend.value();

        BackendTestFactory f = backendTestFactory(context);

        backend = f.createNewBackend();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      NessieBackendName nessieBackendName =
          PersistExtension.annotationInstance(context, NessieBackendName.class);

      backendName = nessieBackendName != null ? nessieBackendName.value() : null;

      BackendFactory<Object> backendFactory;
      if (backendName != null) {
        backendFactory = PersistLoader.findFactoryByName(backendName);
      } else {
        backendFactory = PersistLoader.findAny();
      }
      backend = backendFactory.buildBackend(backendFactory.newConfigInstance());
    }
    return backend;
  }

  private Backend maybeReusable(ExtensionContext context) {
    if (backend == null) {
      return null;
    }

    NessieBackend nessieBackend = PersistExtension.annotationInstance(context, NessieBackend.class);
    Class<? extends BackendTestFactory> nessieBackendType =
        nessieBackend != null ? nessieBackend.value() : null;
    NessieBackendName nessieBackendName =
        PersistExtension.annotationInstance(context, NessieBackendName.class);
    String name = nessieBackendName != null ? nessieBackendName.value() : null;

    if (!Objects.equals(backendType, nessieBackendType) || !Objects.equals(name, backendName)) {
      try {
        close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    }

    return backend;
  }

  @Override
  public void close() throws Exception {
    backendName = null;
    backendType = null;

    try {
      if (backend != null) {
        backend.close();
      }
    } finally {
      backend = null;

      try {
        if (backendTestFactory != null) {
          backendTestFactory.stop();
        }
      } finally {
        backendTestFactory = null;
      }
    }
  }
}
