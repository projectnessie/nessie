/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.test.server;

import static org.projectnessie.test.compatibility.Util.FIELD_NESSIE_VERSION;
import static org.projectnessie.test.compatibility.Util.METHOD_UPDATE_TEST_SERVER_URI;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.test.compatibility.NessieServerHelper;
import org.projectnessie.test.compatibility.NessieServerHelper.NessieServerInstance;

public class JerseyServerLifetimeExtensions implements BeforeAllCallback, AfterAllCallback {

  @Override
  public void afterAll(ExtensionContext context) {
    context.getTestClass().ifPresent(this::stopJersey);
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    context.getTestClass().ifPresent(this::startJersey);
  }

  private NessieServerInstance serverInstance;

  private synchronized void startJersey(Class<?> testClass) {
    try {
      if (serverInstance == null) {
        String nessieVersion =
            Objects.requireNonNull(
                (String) testClass.getDeclaredField(FIELD_NESSIE_VERSION).get(null),
                String.format("Field '%s' must not be null.", FIELD_NESSIE_VERSION));
        serverInstance = NessieServerHelper.startIsolated(nessieVersion);
      }
      testClass
          .getMethod(METHOD_UPDATE_TEST_SERVER_URI, URI.class)
          .invoke(null, serverInstance.getURI());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private synchronized void stopJersey(Class<?> testClass) {
    if (serverInstance == null) {
      return;
    }
    try {
      serverInstance.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      serverInstance = null;
    }
  }
}
