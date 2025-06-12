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
package org.projectnessie.tools.compatibility.internal;

import static org.junit.platform.commons.support.AnnotationSupport.findRepeatableAnnotations;
import static org.projectnessie.tools.compatibility.internal.TranslatingVersionNessieApi.unsupportedApiInterfaceProxy;
import static org.projectnessie.tools.compatibility.internal.Util.extensionStore;
import static org.projectnessie.tools.compatibility.internal.Util.throwUnchecked;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieApiBuilderProperty;
import org.projectnessie.tools.compatibility.api.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractNessieApiHolder implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNessieApiHolder.class);

  protected final ClientKey clientKey;

  /** Called for fields that have the {@link NessieAPI} annotation. */
  static Object apiInstanceForField(
      ExtensionContext context,
      Field field,
      Version version,
      Function<ExtensionContext, NessieServer> nessieServerSupplier) {
    ClientKey clientKey = createClientKey(context, field, version, nessieServerSupplier);

    if (version.equals(Version.CURRENT)) {
      return extensionStore(context)
          .getOrComputeIfAbsent(
              clientKey, CurrentNessieApiHolder::new, CurrentNessieApiHolder.class)
          .getApiInstance();
    } else {
      return context
          .getStore(Util.EXTENSION_CONTEXT_NAMESPACE)
          .getOrComputeIfAbsent(
              clientKey, k -> new OldNessieApiHolder(context, k), OldNessieApiHolder.class)
          .getApiInstance();
    }
  }

  private static ClientKey createClientKey(
      ExtensionContext context,
      Field field,
      Version version,
      Function<ExtensionContext, NessieServer> nessieServerSupplier) {
    // This method is only called for fields that are annotated with NessieAPI.
    NessieAPI nessieAPI = field.getAnnotation(NessieAPI.class);

    @SuppressWarnings("unchecked")
    Class<? extends NessieApi> apiType = (Class<? extends NessieApi>) field.getType();
    Map<String, String> configs =
        buildApiBuilderConfig(context, field, apiType, nessieServerSupplier);
    return new ClientKey(version, nessieAPI.builderClassName(), apiType, configs);
  }

  private static Map<String, String> buildApiBuilderConfig(
      ExtensionContext context,
      Field field,
      Class<? extends NessieApi> apiType,
      Function<ExtensionContext, NessieServer> nessieServerSupplier) {
    Map<String, String> configs = new HashMap<>();
    findRepeatableAnnotations(field, NessieApiBuilderProperty.class)
        .forEach(prop -> configs.put(prop.name(), prop.value()));
    NessieServer nessieServer = nessieServerSupplier.apply(context);
    URI uri = nessieServer.getUri(apiType);
    if (uri != null) {
      configs.put("nessie.uri", uri.toString());
    }
    return configs;
  }

  protected AbstractNessieApiHolder(ClientKey clientKey) {
    this.clientKey = clientKey;
  }

  @Override
  public void close() {
    LOGGER.info("Closing Nessie client for version {}", clientKey.getVersion());
    NessieApi api = getApiInstance();
    if (api != null) {
      api.close();
    }
  }

  public abstract NessieApi getApiInstance();

  /**
   * Used to construct {@link NessieApi} instances, for both current (in-tree) and old Nessie
   * versions.
   *
   * <p>Must use {@link AutoCloseable} instead of {@link NessieApi}, because it loads the class via
   * the given class loader, so instances of {@link NessieApi} for <em>old</em> Nessie versions will
   * return a different class.
   */
  protected static AutoCloseable createNessieClient(ClassLoader classLoader, ClientKey clientKey) {
    try {
      Object builderInstance;

      Class<?> nessieClientBuilderClass =
          classLoader.loadClass("org.projectnessie.client.NessieClientBuilder");
      String builderClass = clientKey.getBuilderClass();
      if (NessieAPI.DEFAULT_BUILDER_CLASS_NAME.equals(builderClass)) {
        builderClass = null;
      }

      try {
        // New functionality using NessieClientBuilder and the service loader mechanism.
        Method createClientBuilderMethod =
            nessieClientBuilderClass.getDeclaredMethod(
                "createClientBuilder", String.class, String.class);
        builderInstance = createClientBuilderMethod.invoke(null, null, builderClass);
      } catch (NoSuchMethodException ignore) {
        if (builderClass == null) {
          // Fall back to legacy (and now removed) HttpClientBuilder. See
          // https://github.com/projectnessie/nessie/pull/7803
          builderClass = "org.projectnessie.client.http.HttpClientBuilder";
        }
        Class<?> builderClazz = classLoader.loadClass(builderClass);
        builderInstance = builderClazz.getMethod("builder").invoke(null);
      }

      Method fromConfigMethod = builderInstance.getClass().getMethod("fromConfig", Function.class);
      Function<String, String> getCfg =
          k -> {
            String v = clientKey.getConfigs().get(k);
            if (v != null) {
              return v;
            }
            return System.getProperty(k);
          };
      builderInstance = fromConfigMethod.invoke(builderInstance, getCfg);

      Class<?> targetClass;
      try {
        targetClass = classLoader.loadClass(clientKey.getType().getName());
      } catch (ClassNotFoundException e) {
        Class<? extends NessieApi> declaredType = clientKey.getType();
        return unsupportedApiInterfaceProxy(declaredType, clientKey.getVersion());
      }

      Method buildMethod = builderInstance.getClass().getMethod("build", Class.class);
      Object apiInstance = null;
      try {
        apiInstance = buildMethod.invoke(builderInstance, targetClass);
      } catch (InvocationTargetException e) {
        // Let the test continue with a null instance, if the Nessie API is not compatible.
        // Test methods must check for nulls and skip the test if necessary.
        if (!isNessieApiCompatibilityException(e)) {
          throw e;
        }
      }

      LOGGER.info(
          "Created Nessie client for version {} for {}",
          clientKey.getVersion(),
          getCfg.apply("nessie.uri"));

      return (AutoCloseable) apiInstance;
    } catch (InvocationTargetException e) {
      throw throwUnchecked(e.getTargetException());
    } catch (Exception e) {
      throw throwUnchecked(e);
    }
  }

  private static boolean isNessieApiCompatibilityException(InvocationTargetException e) {
    return e.getCause() != null
        && e.getCause().getClass().getSimpleName().equals("NessieApiCompatibilityException");
  }
}
