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
package com.dremio.nessie.client;

/**
 * Configuration constants for Nessie.
 */
public final class NessieConfigConstants {
  /**
   * Config property name ({@value #CONF_NESSIE_URL}) for the Nessie service URL.
   */
  public static final String CONF_NESSIE_URL = "nessie.url";
  /**
   * Config property name ({@value #CONF_NESSIE_USERNAME}) for the user name used for (basic) authentication.
   */
  public static final String CONF_NESSIE_USERNAME = "nessie.username";
  /**
   * Config property name ({@value #CONF_NESSIE_PASSWORD}) for the password used for (basic) authentication.
   */
  public static final String CONF_NESSIE_PASSWORD = "nessie.password";
  /**
   * Config property name ({@value #CONF_NESSIE_AUTH_TYPE}) for the authentication type,
   * see {@link com.dremio.nessie.client.NessieClient.AuthType}.
   * <p>
   * If no {@value #CONF_NESSIE_AUTH_TYPE} option is specified, the implementation will default
   * to {@link com.dremio.nessie.client.NessieClient.AuthType#BASIC}, if both
   * {@value #CONF_NESSIE_USERNAME} and {@value #CONF_NESSIE_PASSWORD} are specified, otherwise
   * the default will be {@link com.dremio.nessie.client.NessieClient.AuthType#NONE}. Note that
   * "basic" HTTP authentication is not considered secure.
   * </p>
   */
  public static final String CONF_NESSIE_AUTH_TYPE = "nessie.auth_type";
  /**
   * Config property name ({@value #CONF_NESSIE_REF}) for the nessie reference used by clients.
   */
  public static final String CONF_NESSIE_REF = "nessie.ref";

  private NessieConfigConstants() {
    // empty
  }
}
