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
package org.projectnessie.server;

import java.net.URI;
import java.util.Objects;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class QuarkusNessieUriResolver implements ParameterResolver {

  private static Integer getQuarkusTestPort() {
    return Objects.requireNonNull(
        Integer.getInteger("quarkus.http.test-port"),
        "System property not set correctly: quarkus.http.test-port");
  }

  private static String getNessieApiV1Url() {
    return String.format("http://localhost:%d/api/v1", getQuarkusTestPort());
  }

  private boolean isQuarkusNessieUriParam(ParameterContext paramCtx) {
    return paramCtx.getParameter().getName().equals("quarkusNessieUri")
        && paramCtx.getParameter().getType().equals(URI.class);
  }

  @Override
  public boolean supportsParameter(ParameterContext paramCtx, ExtensionContext extensionCtx)
      throws ParameterResolutionException {
    return isQuarkusNessieUriParam(paramCtx);
  }

  @Override
  public Object resolveParameter(ParameterContext paramCtx, ExtensionContext extensionCtx)
      throws ParameterResolutionException {
    if (isQuarkusNessieUriParam(paramCtx)) {
      return URI.create(getNessieApiV1Url());
    }
    throw new ParameterResolutionException(
        "Unsupported parameter " + paramCtx.getParameter() + " on " + paramCtx.getTarget());
  }
}
