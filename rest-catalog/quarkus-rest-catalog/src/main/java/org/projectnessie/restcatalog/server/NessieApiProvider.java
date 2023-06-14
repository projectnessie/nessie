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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TYPE;

import io.quarkus.runtime.Startup;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.restcatalog.server.auth.BearerTokenPropagatingAuthentication;
import org.projectnessie.restcatalog.server.auth.BearerTokenPropagator;

@ApplicationScoped
public class NessieApiProvider {

  private static final String PROPAGATE_AUTH_TYPE = "PROPAGATE";

  @Inject NessieIcebergRestConfig config;

  @Inject BearerTokenPropagator tokenPropagator;

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

    Map<String, String> clientConfig = new HashMap<>(config.nessieClientConfig());

    String authType = getAuthType(clientConfig);
    HttpAuthentication authentication = null;
    if (Objects.equals(authType, PROPAGATE_AUTH_TYPE)) {
      authentication = new BearerTokenPropagatingAuthentication(tokenPropagator);
      System.clearProperty(CONF_NESSIE_AUTH_TYPE);
      clientConfig.remove(CONF_NESSIE_AUTH_TYPE);
    }

    clientBuilder.fromSystemProperties().fromConfig(clientConfig::get);

    // must be set after fromSystemProperties and fromConfig
    if (authentication != null) {
      clientBuilder.withAuthentication(authentication);
    }

    return clientBuilder.build(NessieApiV2.class);
  }

  private static String getAuthType(Map<String, String> clientConfig) {
    return System.getProperty(CONF_NESSIE_AUTH_TYPE, clientConfig.get(CONF_NESSIE_AUTH_TYPE));
  }

  public void disposeNessieApi(@Disposes NessieApiV2 api) {
    api.close();
  }
}
