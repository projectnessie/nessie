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
package org.projectnessie.client;

/** Configuration constants for Nessie. */
public final class NessieConfigConstants {
  /** Config property name ({@value #CONF_NESSIE_URI}) for the Nessie service URL. */
  public static final String CONF_NESSIE_URI = "nessie.uri";
  /**
   * Config property name ({@value #CONF_NESSIE_USERNAME}) for the user name used for (basic)
   * authentication.
   *
   * @deprecated "basic" HTTP authentication is not considered secure. Use {@link
   *     #CONF_NESSIE_AUTH_TOKEN} instead.
   */
  @Deprecated public static final String CONF_NESSIE_USERNAME = "nessie.authentication.username";
  /**
   * Config property name ({@value #CONF_NESSIE_PASSWORD}) for the password used for (basic)
   * authentication.
   *
   * @deprecated "basic" HTTP authentication is not considered secure. Use {@link
   *     #CONF_NESSIE_AUTH_TOKEN} instead.
   */
  @Deprecated public static final String CONF_NESSIE_PASSWORD = "nessie.authentication.password";
  /**
   * Config property name ({@value #CONF_NESSIE_AUTH_TOKEN}) for the token used for (bearer)
   * authentication.
   */
  public static final String CONF_NESSIE_AUTH_TOKEN = "nessie.authentication.token";
  /**
   * Config property name ({@value #CONF_NESSIE_AWS_REGION}) for the region used for AWS
   * authentication.
   */
  public static final String CONF_NESSIE_AWS_REGION = "nessie.authentication.aws.region";
  /**
   * Config property name ({@value #CONF_NESSIE_AWS_PROFILE}) for the profile name used for AWS
   * authentication (optional).
   */
  public static final String CONF_NESSIE_AWS_PROFILE = "nessie.authentication.aws.profile";
  /**
   * Config property name ({@value #CONF_NESSIE_AUTH_TYPE}) for the authentication provider ID.
   * Valid values are {@code BASIC}, {@code BEARER} and {@code AWS}.
   *
   * <p>Note that "basic" HTTP authentication is not considered secure, use {@code BEARER} instead.
   */
  public static final String CONF_NESSIE_AUTH_TYPE = "nessie.authentication.type";
  /**
   * Config property name ({@value #CONF_NESSIE_REF}) for the nessie reference name used by clients.
   */
  public static final String CONF_NESSIE_REF = "nessie.ref";
  /**
   * Config property name ({@value #CONF_NESSIE_REF_HASH}) for the nessie reference hash used by
   * clients.
   */
  public static final String CONF_NESSIE_REF_HASH = "nessie.ref.hash";
  /**
   * Config property name ({@value #CONF_NESSIE_TRACING}) to enable adding the HTTP headers of an
   * active OpenTracing span to all Nessie requests. Valid values are {@code true} and {@code
   * false}.
   */
  public static final String CONF_NESSIE_TRACING = "nessie.tracing";
  /**
   * Config property name ("{@value #CONF_READ_TIMEOUT}") for the network transport read-timeout.
   */
  public static final String CONF_READ_TIMEOUT = "nessie.transport.read-timeout";
  /**
   * Config property name ("{@value #CONF_CONNECT_TIMEOUT}") for the network transport connect
   * timeout.
   */
  public static final String CONF_CONNECT_TIMEOUT = "nessie.transport.connect-timeout";
  /**
   * Config property name ("{@value #CONF_NESSIE_DISABLE_COMPRESSION}") whether to disable
   * compression on the network layer.
   */
  public static final String CONF_NESSIE_DISABLE_COMPRESSION =
      "nessie.transport.disable-compression";
  /**
   * Config property name ({@value #CONF_NESSIE_CLIENT_BUILDER_IMPL}) for custom client builder
   * class name.
   */
  public static final String CONF_NESSIE_CLIENT_BUILDER_IMPL = "nessie.client-builder-impl";

  private NessieConfigConstants() {
    // empty
  }
}
