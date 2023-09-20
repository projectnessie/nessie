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
package org.projectnessie.jaxrs.ext.customclient;

import java.lang.reflect.Proxy;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.api.NessieApiV2;

public class IncompleteClientBuilderForTesting
    extends NessieClientBuilder.AbstractNessieClientBuilder {
  @Override
  public String name() {
    return "IncompleteForTesting";
  }

  @Override
  public int priority() {
    return 0;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <API extends NessieApi> API build(Class<API> apiContract) {
    return (API)
        Proxy.newProxyInstance(
            IncompleteClientBuilderForTesting.class.getClassLoader(),
            new Class[] {NessieApiV2.class},
            (proxy, method, args) -> {
              throw new UnsupportedOperationException(
                  "This Nessie client instance is not real, sorry.");
            });
  }
}
