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
package org.projectnessie.server;

import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.projectnessie.client.NessieClient;

/** Base class for client-base authentication and authorization tests. */
public abstract class BaseClientAuthTest {

  private NessieClient client;
  private Consumer<NessieClient.Builder> customizer;

  @AfterEach
  void closeClient() {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  protected void withClientCustomizer(Consumer<NessieClient.Builder> customizer) {
    this.customizer = customizer;
  }

  protected NessieClient client() {
    if (client != null) {
      return client;
    }

    NessieClient.Builder builder = NessieClient.builder().withUri("http://localhost:19121/api/v1");

    if (customizer != null) {
      customizer.accept(builder);
    }

    client = builder.build();

    return client;
  }
}
