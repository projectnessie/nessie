/*
 * Copyright (C) 2025 Dremio
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
package org.projectnessie.junit.engine;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.engine.config.DefaultJupiterConfiguration;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor;
import org.junit.jupiter.engine.discovery.DiscoverySelectorResolver;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.reporting.OutputDirectoryProvider;
import org.junit.platform.engine.support.discovery.DiscoveryIssueReporter;

@SuppressWarnings({"deprecation", "removal", "JavaReflectionMemberAccess"})
final class JUnitCompat {
  private JUnitCompat() {}

  private static final Constructor<DefaultJupiterConfiguration> CTOR_JUNIT_6;
  private static final Constructor<DefaultJupiterConfiguration> CTOR_JUNIT_5_14_2;
  private static final Constructor<DefaultJupiterConfiguration> CTOR_JUNIT_OLDER;

  private static final Method MTH_RESOLVE_SELECTORS_JUNIT_6;
  private static final Method MTH_RESOLVE_SELECTORS_JUNIT_5;

  static {
    Constructor<DefaultJupiterConfiguration> ctorJunit6;
    Constructor<DefaultJupiterConfiguration> ctorJunit5142;
    Constructor<DefaultJupiterConfiguration> ctorJunitOlder;
    try {
      Class<?> clazzOutputDirectoryCreator =
          Class.forName("org.junit.platform.engine.OutputDirectoryCreator");
      try {
        ctorJunit6 =
            DefaultJupiterConfiguration.class.getDeclaredConstructor(
                ConfigurationParameters.class,
                clazzOutputDirectoryCreator,
                DiscoveryIssueReporter.class);
      } catch (Exception e) {
        ctorJunit6 = null;
      }
      try {
        ctorJunit5142 =
            DefaultJupiterConfiguration.class.getDeclaredConstructor(
                ConfigurationParameters.class, clazzOutputDirectoryCreator);
      } catch (Exception e) {
        ctorJunit5142 = null;
      }
    } catch (ClassNotFoundException e) {
      ctorJunit6 = null;
      ctorJunit5142 = null;
    }
    try {
      ctorJunitOlder =
          DefaultJupiterConfiguration.class.getDeclaredConstructor(
              ConfigurationParameters.class, OutputDirectoryProvider.class);
    } catch (Exception e) {
      ctorJunitOlder = null;
    }
    CTOR_JUNIT_6 = ctorJunit6;
    CTOR_JUNIT_5_14_2 = ctorJunit5142;
    CTOR_JUNIT_OLDER = ctorJunitOlder;

    Method mthResolveSelectorsJunit6;
    Method mthResolveSelectorsJunit5;
    try {
      mthResolveSelectorsJunit6 =
          DiscoverySelectorResolver.class.getDeclaredMethod(
              "resolveSelectors",
              EngineDiscoveryRequest.class,
              JupiterEngineDescriptor.class,
              DiscoveryIssueReporter.class);
    } catch (Exception e) {
      mthResolveSelectorsJunit6 = null;
    }
    try {
      mthResolveSelectorsJunit5 =
          DiscoverySelectorResolver.class.getDeclaredMethod(
              "resolveSelectors", EngineDiscoveryRequest.class, JupiterEngineDescriptor.class);
    } catch (Exception e) {
      mthResolveSelectorsJunit5 = null;
    }
    MTH_RESOLVE_SELECTORS_JUNIT_6 = mthResolveSelectorsJunit6;
    MTH_RESOLVE_SELECTORS_JUNIT_5 = mthResolveSelectorsJunit5;
  }

  static JupiterConfiguration newDefaultJupiterConfiguration(
      EngineDiscoveryRequest request, UniqueId uniqueId) {
    return newDefaultJupiterConfiguration(request.getConfigurationParameters(), request, uniqueId);
  }

  static JupiterConfiguration newDefaultJupiterConfiguration(
      ConfigurationParameters configurationParameters,
      EngineDiscoveryRequest request,
      UniqueId uniqueId) {
    try {
      if (CTOR_JUNIT_6 != null) {
        DiscoveryIssueReporter issueReporter =
            DiscoveryIssueReporter.deduplicating(
                DiscoveryIssueReporter.forwarding(request.getDiscoveryListener(), uniqueId));
        return CTOR_JUNIT_6.newInstance(
            configurationParameters, request.getOutputDirectoryCreator(), issueReporter);
      }
      if (CTOR_JUNIT_5_14_2 != null) {
        return CTOR_JUNIT_5_14_2.newInstance(
            configurationParameters, request.getOutputDirectoryCreator());
      }
      if (CTOR_JUNIT_OLDER != null) {
        return CTOR_JUNIT_OLDER.newInstance(
            configurationParameters, request.getOutputDirectoryProvider());
      }
      throw new IllegalStateException("No JUnit constructor found");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void resolveSelectors(
      EngineDiscoveryRequest request, JupiterEngineDescriptor result, UniqueId uniqueId) {
    try {
      if (MTH_RESOLVE_SELECTORS_JUNIT_6 != null) {
        DiscoveryIssueReporter issueReporter =
            DiscoveryIssueReporter.deduplicating(
                DiscoveryIssueReporter.forwarding(request.getDiscoveryListener(), uniqueId));
        MTH_RESOLVE_SELECTORS_JUNIT_6.invoke(null, request, result, issueReporter);
        return;
      }
      if (MTH_RESOLVE_SELECTORS_JUNIT_5 != null) {
        Object instance =
            (MTH_RESOLVE_SELECTORS_JUNIT_5.getModifiers() & Modifier.STATIC) == 0
                ? DiscoverySelectorResolver.class.getDeclaredConstructor().newInstance()
                : null;
        MTH_RESOLVE_SELECTORS_JUNIT_5.invoke(instance, request, result);
        return;
      }
      throw new IllegalStateException("No JUnit resolveSelectors() found");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
