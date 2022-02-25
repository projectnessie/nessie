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
package org.projectnessie.versioned.persist.tests.extension;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;

import io.opentracing.mock.MockTracer;
import io.opentracing.util.GlobalTracer;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.ExceptionUtils;

/**
 * Extension that allows injecting a {@link MockTracer} on {@link NessieDbTracer} annotated fields
 * and parameters.
 *
 * <p>Note that the {@link MockTracer} instance is constant per class - like a {@code static final}
 * field.
 */
public class NessieMockedTracingExtension
    implements BeforeAllCallback, BeforeEachCallback, ParameterResolver {
  private static final Namespace NAMESPACE = Namespace.create(NessieMockedTracingExtension.class);

  private static final MockTracer TRACER = new MockTracer();
  private static volatile boolean tracerRegistered;

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    if (GlobalTracer.isRegistered() && !tracerRegistered) {
      throw new ExtensionConfigurationException("GlobalTracer already has a registered Tracer");
    }
    tracerRegistered = true;
    GlobalTracer.registerIfAbsent(TRACER);

    injectTrace(context, TRACER);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    TRACER.reset();

    injectTrace(context, TRACER);
  }

  private static void injectTrace(ExtensionContext context, MockTracer tracer) {
    Class<?> testClass = context.getRequiredTestClass();

    findAnnotatedFields(testClass, NessieDbTracer.class, f -> true)
        .forEach(
            field -> {
              if (!Modifier.isStatic(field.getModifiers())
                  && !context.getTestInstance().isPresent()) {
                return;
              }

              if (!field.getType().isAssignableFrom(MockTracer.class)) {
                throw new ExtensionConfigurationException(
                    "@NessieDbTracer annotated fields must be of type "
                        + MockTracer.class.getName());
              }

              try {
                makeAccessible(field).set(context.getTestInstance().orElse(null), tracer);
              } catch (IllegalAccessException e) {
                ExceptionUtils.throwAsUncheckedException(e);
              }
            });
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.isAnnotated(NessieDbTracer.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return extensionContext.getStore(NAMESPACE).get("tracer", MockTracer.class);
  }
}
