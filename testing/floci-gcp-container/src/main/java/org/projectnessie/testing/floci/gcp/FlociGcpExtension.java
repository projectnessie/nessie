/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.testing.floci.gcp;

import static java.lang.String.format;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;

import java.lang.reflect.Field;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;

public class FlociGcpExtension
    implements BeforeAllCallback, BeforeEachCallback, ParameterResolver, ExecutionCondition {
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(FlociGcpExtension.class);

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    if (OS.current() == OS.LINUX) {
      return enabled("Running on Linux");
    }
    if (OS.current() == OS.MAC) {
      return enabled("Running on macOS");
    }
    return disabled(format("Disabled on %s", OS.current().name()));
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();

    findAnnotatedFields(testClass, FlociGcp.class, ReflectionUtils::isStatic)
        .forEach(field -> injectField(context, field));
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();

    findAnnotatedFields(testClass, FlociGcp.class, ReflectionUtils::isNotStatic)
        .forEach(field -> injectField(context, field));
  }

  private void injectField(ExtensionContext context, Field field) {
    try {
      FlociGcp flociGcp =
          AnnotationUtils.findAnnotation(field, FlociGcp.class)
              .orElseThrow(IllegalStateException::new);

      FlociGcpAccess container =
          context
              .getStore(NAMESPACE)
              .computeIfAbsent(
                  field.toString(), x -> createContainer(flociGcp), FlociGcpAccess.class);

      makeAccessible(field).set(context.getTestInstance().orElse(null), container);
    } catch (Throwable t) {
      ExceptionUtils.throwAsUncheckedException(t);
    }
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (parameterContext.findAnnotation(FlociGcp.class).isEmpty()) {
      return false;
    }
    return parameterContext.getParameter().getType().isAssignableFrom(FlociGcpAccess.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return extensionContext
        .getStore(NAMESPACE)
        .computeIfAbsent(
            FlociGcpExtension.class.getName() + '#' + parameterContext.getParameter().getName(),
            k -> {
              FlociGcp flociGcp = parameterContext.findAnnotation(FlociGcp.class).get();
              return createContainer(flociGcp);
            },
            FlociGcpAccess.class);
  }

  private FlociGcpAccess createContainer(FlociGcp flociGcp) {
    String projectId = nonDefault(flociGcp.projectId());
    String oauth2token = nonDefault(flociGcp.oauth2token());
    String bucket = nonDefault(flociGcp.bucket());
    FlociGcpContainer container =
        new FlociGcpContainer(null, bucket, projectId, oauth2token).withStartupAttempts(5);
    container.start();
    return container;
  }

  private static String nonDefault(String s) {
    return s.equals(FlociGcp.DEFAULT) ? null : s;
  }
}
