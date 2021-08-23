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
package org.projectnessie.client.auth;

import java.util.function.Function;

/**
 * This authentication-"provider" just maps the {@value #AUTH_TYPE_VALUE} to a {@link
 * NessieAuthenticationProvider} implementation, which "produces" a {@code null} {@link
 * NessieAuthentication} instance, which means "no authentication".
 */
public class NoneAuthenticationProvider implements NessieAuthenticationProvider {

  public static final String AUTH_TYPE_VALUE = "NONE";

  public static NessieAuthenticationProvider getInstance() {
    return new NoneAuthenticationProvider();
  }

  @Override
  public String getAuthTypeValue() {
    return AUTH_TYPE_VALUE;
  }

  @Override
  public NessieAuthentication build(Function<String, String> configSupplier) {
    return null;
  }
}
