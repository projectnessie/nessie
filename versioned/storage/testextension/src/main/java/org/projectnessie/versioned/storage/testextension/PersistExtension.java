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
package org.projectnessie.versioned.storage.testextension;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;
import static org.junit.platform.commons.util.AnnotationUtils.findRepeatableAnnotations;
import static org.junit.platform.commons.util.ReflectionUtils.findMethod;
import static org.junit.platform.commons.util.ReflectionUtils.isPrivate;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;
import static org.projectnessie.versioned.storage.testextension.ClassPersistInstances.reinit;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;

/**
 * JUnit extension to supply {@link Persist} to test classes.
 *
 * <p>The test class must be annotated with {@link
 * org.junit.jupiter.api.extension.ExtendWith @ExtendWith}.
 */
public class PersistExtension implements BeforeAllCallback, BeforeEachCallback, ParameterResolver {
  static final Namespace NAMESPACE = Namespace.create(PersistExtension.class);
  static final String KEY_STATICS = "static-nessie-persist";
  static final String KEY_REUSABLE_BACKEND = "reusable-backend";

  static <A extends Annotation> A annotationInstance(
      ExtensionContext context, Class<A> annotation) {
    while (true) {
      Optional<Class<?>> clazz = context.getTestClass();
      if (clazz.isPresent()) {
        Optional<A> ann = findAnnotation(clazz.get(), annotation);
        if (ann.isPresent()) {
          return ann.get();
        }
      }

      Optional<ExtensionContext> p = context.getParent();
      if (p.isEmpty()) {
        return null;
      }
      context = p.get();
    }
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();

    ClassPersistInstances classPersistInstances =
        context
            .getStore(NAMESPACE)
            .getOrComputeIfAbsent(
                KEY_STATICS, k -> new ClassPersistInstances(context), ClassPersistInstances.class);

    findAnnotatedFields(testClass, NessiePersist.class, ReflectionUtils::isStatic)
        .forEach(
            field -> injectField(context, null, field, classPersistInstances::registerPersist));
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    context.getStore(NAMESPACE).get(KEY_STATICS, ClassPersistInstances.class).reinitialize();
    context
        .getRequiredTestInstances()
        .getAllInstances() //
        .forEach(
            instance ->
                findAnnotatedFields(
                        instance.getClass(), NessiePersist.class, ReflectionUtils::isNotStatic)
                    .forEach(
                        field ->
                            injectField(
                                context,
                                instance,
                                field,
                                persist -> {
                                  NessiePersist annotation =
                                      field.getAnnotation(NessiePersist.class);
                                  reinit(persist, annotation.initializeRepo());
                                })));
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.isAnnotated(NessiePersist.class);
  }

  private void injectField(
      ExtensionContext context, Object instance, Field field, Consumer<Persist> newPersist) {
    assertValidFieldCandidate(field);
    try {
      NessiePersist nessiePersist =
          findAnnotation(field, NessiePersist.class).orElseThrow(IllegalStateException::new);

      Object assign = resolve(nessiePersist, field, field.getType(), context, false, newPersist);

      makeAccessible(field).set(instance, assign);
    } catch (Throwable t) {
      ExceptionUtils.throwAsUncheckedException(t);
    }
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    NessiePersist nessiePersist =
        parameterContext
            .findAnnotation(NessiePersist.class)
            .orElseThrow(IllegalStateException::new);

    Parameter parameter = parameterContext.getParameter();
    return resolve(nessiePersist, parameter, parameter.getType(), context, true, persist -> {});
  }

  private Object resolve(
      NessiePersist nessiePersist,
      AnnotatedElement annotatedElement,
      Class<?> type,
      ExtensionContext context,
      boolean canReinit,
      Consumer<Persist> newPersist) {

    boolean wantsPersist = Persist.class.isAssignableFrom(type);
    boolean wantsPersistFactory = PersistFactory.class.isAssignableFrom(type);
    boolean wantsBackend = Backend.class.isAssignableFrom(type);
    boolean wantsBackendTestFactory = BackendTestFactory.class.isAssignableFrom(type);

    checkState(
        wantsPersist || wantsPersistFactory || wantsBackend || wantsBackendTestFactory,
        "Cannot assign to %s",
        annotatedElement);

    if (wantsBackendTestFactory || wantsBackend || wantsPersistFactory) {
      ClassPersistInstances classPersistInstances = classPersistInstances(context);
      if (wantsBackendTestFactory) {
        return classPersistInstances.backendTestFactory();
      }
      if (wantsBackend) {
        return classPersistInstances.backend();
      }
      return classPersistInstances.persistFactory();
    }

    Persist persist = createPersist(nessiePersist, annotatedElement, context);

    if (canReinit) {
      reinit(persist, nessiePersist.initializeRepo());
    }

    newPersist.accept(persist);

    return persist;
  }

  static Map<String, String> extractConfig(
      ExtensionContext context, AnnotatedElement annotatedElement) {

    List<NessieStoreConfig> configs = new ArrayList<>();
    Consumer<AnnotatedElement> collector =
        m -> configs.addAll(findRepeatableAnnotations(m, NessieStoreConfig.class));
    collector.accept(annotatedElement);
    context.getTestMethod().ifPresent(collector);
    context
        .getTestClass()
        .ifPresent(
            cls -> {
              for (; cls != Object.class; cls = cls.getSuperclass()) {
                collector.accept(cls);
              }
            });

    return configs.stream()
        .collect(Collectors.toMap(NessieStoreConfig::name, NessieStoreConfig::value));
  }

  static ClassPersistInstances classPersistInstances(ExtensionContext context) {
    return context.getStore(NAMESPACE).get(KEY_STATICS, ClassPersistInstances.class);
  }

  static Persist createPersist(
      NessiePersist persistAnnotation,
      AnnotatedElement annotatedElement,
      ExtensionContext context) {
    StoreConfig.Adjustable config = StoreConfig.Adjustable.empty();

    config =
        extractCustomConfiguration(persistAnnotation, context)
            .apply(context.getTestInstance().orElse(null), config);

    Map<String, String> configMap = extractConfig(context, annotatedElement);

    config = config.fromFunction(configMap::get);

    config = config.withClock(UniqueMicrosClock.SHARED_INSTANCE);

    return classPersistInstances(context).newPersist(config);
  }

  private static BiFunction<Object, StoreConfig.Adjustable, StoreConfig.Adjustable>
      extractCustomConfiguration(NessiePersist persistAnnotation, ExtensionContext context) {
    BiFunction<Object, StoreConfig.Adjustable, StoreConfig.Adjustable> applyCustomConfig =
        (testInstance, c) -> c;
    if (!persistAnnotation.configMethod().isEmpty()) {
      Method configMethod =
          findMethod(
                  context.getRequiredTestClass(),
                  persistAnnotation.configMethod(),
                  StoreConfig.Adjustable.class)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              "%s.configMethod='%s' does not exist in %s",
                              NessiePersist.class.getSimpleName(),
                              persistAnnotation.configMethod(),
                              context.getRequiredTestClass().getName())));

      makeAccessible(configMethod);

      if (Modifier.isPrivate(configMethod.getModifiers())
          || !StoreConfig.Adjustable.class.isAssignableFrom(configMethod.getReturnType())) {
        throw new IllegalArgumentException(
            String.format(
                "%s.configMethod='%s' must have the signature 'static %s %s(%s)' in %s",
                NessiePersist.class.getSimpleName(),
                persistAnnotation.configMethod(),
                StoreConfig.Adjustable.class.getSimpleName(),
                persistAnnotation.configMethod(),
                StoreConfig.Adjustable.class.getSimpleName(),
                context.getRequiredTestClass().getName()));
      }
      applyCustomConfig =
          (testInstance, c) -> {
            try {
              return (StoreConfig.Adjustable) configMethod.invoke(testInstance, c);
            } catch (InvocationTargetException | IllegalAccessException e) {
              throw new RuntimeException(e);
            }
          };
    }
    return applyCustomConfig;
  }

  private void assertValidFieldCandidate(Field field) {
    if (!field.getType().isAssignableFrom(Persist.class)
        && !field.getType().isAssignableFrom(PersistFactory.class)
        && !field.getType().isAssignableFrom(Backend.class)
        && !field.getType().isAssignableFrom(BackendTestFactory.class)) {
      throw new ExtensionConfigurationException(
          "Can only resolve fields of type "
              + Persist.class.getName()
              + " but was: "
              + field.getType().getName());
    }
    if (isPrivate(field)) {
      throw new ExtensionConfigurationException(
          String.format("field [%s] must not be private.", field));
    }
  }
}
