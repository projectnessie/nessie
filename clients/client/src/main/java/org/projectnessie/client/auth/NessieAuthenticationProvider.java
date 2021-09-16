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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TYPE;

import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.function.Function;

/**
 * Base interface for different authentication methods like "basic" (username + plain password),
 * bearer token, etc.
 */
public interface NessieAuthenticationProvider {

  /**
   * Configure a {@link NessieAuthentication} instance using the configuration supplied by {@code
   * configuration}.
   *
   * <p>Checks all instances returned by Java's {@link ServiceLoader} for the {@link
   * NessieAuthenticationProvider} interface.
   *
   * <p>Uses the config option {@link
   * org.projectnessie.client.NessieConfigConstants#CONF_NESSIE_AUTH_TYPE} to determine the
   * authentication type by comparing the config value with the value returned by implementations'
   * {@link #getAuthTypeValue()}. If {@link
   * org.projectnessie.client.NessieConfigConstants#CONF_NESSIE_AUTH_TYPE} is not configured, {@code
   * null} will be returned. If no implementation could be found, an {@link
   * IllegalArgumentException} is thrown.
   *
   * <p>If a {@link NessieAuthenticationProvider} instance was found, the implementation's builder
   * {@link #build(Function)} will be called.
   */
  static NessieAuthentication fromConfig(Function<String, String> configuration) {
    String authType = configuration.apply(CONF_NESSIE_AUTH_TYPE);
    if (authType == null) {
      authType = configuration.apply("nessie.auth-type"); // legacy property name
    }

    if (authType != null) {
      for (NessieAuthenticationProvider ap :
          ServiceLoader.load(NessieAuthenticationProvider.class)) {
        if (ap.getAuthTypeValue().equalsIgnoreCase(authType)) {
          return ap.build(configuration);
        }
      }

      // Fallback to reflection, just in case the files in META-INF/services required by
      // ServiceLoader got lost by broken shaded jars.

      String reflectionClassName = NessieAuthentication.class.getName();
      reflectionClassName =
          reflectionClassName.substring(0, reflectionClassName.lastIndexOf('.') + 1)
              + authType.substring(0, 1).toUpperCase(Locale.ROOT)
              + authType.substring(1).toLowerCase(Locale.ROOT)
              + "AuthenticationProvider";

      NessieAuthenticationProvider ap;
      try {
        @SuppressWarnings("unchecked")
        Class<NessieAuthenticationProvider> provider =
            (Class<NessieAuthenticationProvider>) Class.forName(reflectionClassName);
        ap = provider.getDeclaredConstructor().newInstance();
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(
            String.format(
                "No authentication provider for '%s' found using class name '%s'.",
                authType, reflectionClassName));
      } catch (InvocationTargetException
          | InstantiationException
          | IllegalAccessException
          | NoSuchMethodException e) {
        throw new IllegalArgumentException(
            String.format("Could not initialize '%s': %s", reflectionClassName, e), e);
      }

      return ap.build(configuration);
    }
    return null;
  }

  /**
   * The authentication type discriminator. If authentication is configured via properties, like
   * {@link org.projectnessie.client.NessieClientBuilder#fromConfig(Function)}/{@link
   * org.projectnessie.client.NessieClientBuilder#withAuthenticationFromConfig(Function)}, set the a
   * {@link NessieAuthenticationProvider} instance is used, if {@link
   * org.projectnessie.client.NessieConfigConstants#CONF_NESSIE_AUTH_TYPE} equals the value returned
   * by this method.
   */
  String getAuthTypeValue();

  /**
   * Build the implementation that provides authentication credentials using the given parameters.
   *
   * <p>The implementation must throw appropriate exceptions, like {@link NullPointerException} or
   * {@link IllegalArgumentException} with proper and readable explanations, if mandatory
   * configuration options are missing or have wrong/incompatible/invalid values.
   */
  NessieAuthentication build(Function<String, String> configSupplier);
}
