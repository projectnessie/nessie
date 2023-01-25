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
package org.projectnessie.restcatalog.server;

import io.quarkus.runtime.Startup;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;

@ApplicationScoped
public class NessieApiProvider {
  @Inject NessieIcebergRestConfig config;

  @Produces
  @Singleton
  @Startup
  public NessieApiV2 produceNessieApi() {
    NessieClientBuilder<?> clientBuilder;
    try {
      clientBuilder =
          (NessieClientBuilder<?>)
              Class.forName(config.nessieClientBuilder())
                  .asSubclass(NessieClientBuilder.class)
                  .getDeclaredMethod("builder")
                  .invoke(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return clientBuilder
        .fromSystemProperties()
        .fromConfig(cfg -> config.nessieClientConfig().get(cfg))
        .build(NessieApiV2.class);
  }

  public void disposeFileIO(@Disposes NessieApiV2 api) {
    api.close();
  }
}
