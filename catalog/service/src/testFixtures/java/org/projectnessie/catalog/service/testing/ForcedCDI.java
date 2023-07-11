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
package org.projectnessie.catalog.service.testing;

import static com.google.common.base.Preconditions.checkState;

import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;
import javax.enterprise.inject.spi.CDI;
import javax.enterprise.inject.spi.CDIProvider;
import org.jboss.weld.environment.se.WeldContainer;

final class ForcedCDI {

  private static final CDIProvider weldCdiProvider;
  private static final AtomicReference<CDI<Object>> forcedCDI = new AtomicReference<>();

  static {
    ServiceLoader<CDIProvider> providerLoader =
        ServiceLoader.load(CDIProvider.class, CDI.class.getClassLoader());
    weldCdiProvider = providerLoader.iterator().next();

    // This is an ugly hack to allow TWO Weld containers. If there's already a running Weld
    // container, ignore it and use the one created above.
    // Otherwise, the "old" Weld container would be used, effectively preventing the Iceberg REST
    // catalog tests to work.
    // Alternative would be to start a Nessie Quarkus instance, but that makes debugging/development
    // a lot harder. Once development is mostly done, and we run into issues with this hack,
    // reverting to the integration-test approach using a Nessie Quarkus instance is fine.
    CDI<Object> currentCDI = null;
    try {
      currentCDI = CDI.current();
    } catch (Exception ignore) {
      // ignore
    }
    if (currentCDI != null) {
      CDI.setCDIProvider(
          () -> {
            CDI<Object> cdi = forcedCDI.get();
            return cdi != null ? cdi : weldCdiProvider.getCDI();
          });
    }
  }

  static void clear() {
    forcedCDI.set(null);
  }

  static void setCDI(WeldContainer weldContainer) {
    checkState(forcedCDI.compareAndSet(null, weldContainer), "Forced CDI instance already set");
  }
}
