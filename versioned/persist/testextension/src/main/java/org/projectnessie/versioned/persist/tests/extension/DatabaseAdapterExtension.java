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
package org.projectnessie.versioned.persist.tests.extension;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.AnnotationUtils.findRepeatableAnnotations;
import static org.junit.platform.commons.util.ReflectionUtils.findMethod;
import static org.junit.platform.commons.util.ReflectionUtils.isPrivate;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;
import static org.projectnessie.versioned.persist.tests.SystemPropertiesConfigurer.CONFIG_NAME_PREFIX;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.AdjustableDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionProvider;
import org.projectnessie.versioned.persist.adapter.events.AdapterEventConsumer;
import org.projectnessie.versioned.persist.adapter.spi.TracingDatabaseAdapter;
import org.projectnessie.versioned.persist.store.PersistVersionStore;
import org.projectnessie.versioned.persist.tests.SystemPropertiesConfigurer;

/**
 * JUnit extension to supply {@link DatabaseAdapter} and derived {@link VersionStore} to test
 * classes.
 *
 * <p>The test class must be annotated with {@link
 * org.junit.jupiter.api.extension.ExtendWith @ExtendWith}.
 */
public class DatabaseAdapterExtension
    implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, ParameterResolver {
  private static final Namespace NAMESPACE = Namespace.create(DatabaseAdapterExtension.class);
  private static final String KEY_STATICS = "static-adapters";

  private static class ClassDbAdapters implements CloseableResource {
    final List<DatabaseAdapter> adapters = new ArrayList<>();
    TestConnectionProviderSource<?> connectionProvider;

    ClassDbAdapters(Class<?> testClass) {
      NessieExternalDatabase external =
          AnnotationUtils.findAnnotation(testClass, NessieExternalDatabase.class)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          String.format(
                              "Mandatory @%s missing for test class %s",
                              NessieExternalDatabase.class.getSimpleName(), testClass.getName())));
      TestConnectionProviderSource<?> connectionProvider;
      try {
        connectionProvider = external.value().getDeclaredConstructor().newInstance();
        connectionProvider.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      this.connectionProvider = connectionProvider;
    }

    @Override
    public void close() throws Exception {
      if (connectionProvider != null) {
        try {
          connectionProvider.stop();
        } finally {
          connectionProvider = null;
        }
      }
    }

    void newDatabaseAdapter(DatabaseAdapter adapter) {
      adapters.add(adapter);
    }
  }

  private static void reinit(DatabaseAdapter adapter) {
    adapter.eraseRepo();
    adapter.initializeRepo("main");
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();

    ClassDbAdapters classDbAdapters =
        context
            .getStore(NAMESPACE)
            .getOrComputeIfAbsent(
                KEY_STATICS, k -> new ClassDbAdapters(testClass), ClassDbAdapters.class);

    findAnnotatedFields(testClass, NessieDbAdapter.class, ReflectionUtils::isStatic)
        .forEach(field -> injectField(context, field, classDbAdapters::newDatabaseAdapter));
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    context
        .getStore(NAMESPACE)
        .get(KEY_STATICS, ClassDbAdapters.class)
        .adapters
        .forEach(DatabaseAdapterExtension::reinit);
    context
        .getRequiredTestInstances()
        .getAllInstances() //
        .forEach(
            instance ->
                findAnnotatedFields(
                        instance.getClass(), NessieDbAdapter.class, ReflectionUtils::isNotStatic)
                    .forEach(
                        field ->
                            injectField(
                                context,
                                field,
                                adapter -> {
                                  if (field.getAnnotation(NessieDbAdapter.class).initializeRepo()) {
                                    reinit(adapter);
                                  }
                                })));
  }

  @Override
  public void afterEach(ExtensionContext context) {
    context
        .getStore(NAMESPACE)
        .get(KEY_STATICS, ClassDbAdapters.class)
        .adapters
        .forEach(DatabaseAdapter::assertCleanStateForTests);
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.isAnnotated(NessieDbAdapter.class);
  }

  private void injectField(
      ExtensionContext context, Field field, Consumer<DatabaseAdapter> newAdapter) {
    assertValidFieldCandidate(field);
    try {
      NessieDbAdapter nessieDbAdapter =
          AnnotationUtils.findAnnotation(field, NessieDbAdapter.class)
              .orElseThrow(IllegalStateException::new);

      Object assign =
          resolve(nessieDbAdapter, field, field.getType(), context, null, field, false, newAdapter);

      makeAccessible(field).set(context.getTestInstance().orElse(null), assign);
    } catch (Throwable t) {
      ExceptionUtils.throwAsUncheckedException(t);
    }
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context)
      throws ParameterResolutionException {
    NessieDbAdapter nessieDbAdapter =
        parameterContext
            .findAnnotation(NessieDbAdapter.class)
            .orElseThrow(IllegalStateException::new);

    Parameter parameter = parameterContext.getParameter();
    return resolve(
        nessieDbAdapter,
        parameter,
        parameter.getType(),
        context,
        parameterContext,
        null,
        true,
        adapter -> {});
  }

  private Object resolve(
      NessieDbAdapter nessieDbAdapter,
      AnnotatedElement annotatedElement,
      Class<?> type,
      ExtensionContext context,
      ParameterContext parameterContext,
      Field field,
      boolean canReinit,
      Consumer<DatabaseAdapter> newAdapter) {

    DatabaseAdapter databaseAdapter =
        createAdapterResource(nessieDbAdapter, context, parameterContext, field);

    if (nessieDbAdapter.withTracing()) {
      databaseAdapter = new TracingDatabaseAdapter(databaseAdapter);
    }

    if (canReinit && nessieDbAdapter.initializeRepo()) {
      reinit(databaseAdapter);
    }

    Object assign;
    if (DatabaseAdapter.class.isAssignableFrom(type)) {
      assign = databaseAdapter;
    } else if (VersionStore.class.isAssignableFrom(type)) {
      VersionStore store = new PersistVersionStore(databaseAdapter);
      assign = store;
    } else {
      throw new IllegalStateException("Cannot assign to " + annotatedElement);
    }

    newAdapter.accept(databaseAdapter);

    if (assign != null && !type.isAssignableFrom(assign.getClass())) {
      throw new IllegalStateException(
          String.format("Cannot assign %s to %s", assign.getClass(), annotatedElement));
    }

    return assign;
  }

  static <A extends Annotation> Optional<A> findAnnotation(
      ExtensionContext context,
      Field field,
      ParameterContext parameterContext,
      Class<A> annotation) {
    Optional<A> opt;
    if (parameterContext != null) {
      opt = parameterContext.findAnnotation(annotation);
      if (opt.isPresent()) {
        return opt;
      }
    }
    if (field != null) {
      A fieldAnnotation = field.getAnnotation(annotation);
      if (fieldAnnotation != null) {
        return Optional.of(fieldAnnotation);
      }
    }
    opt = context.getTestMethod().flatMap(m -> AnnotationUtils.findAnnotation(m, annotation));
    if (opt.isPresent()) {
      return opt;
    }

    while (true) {
      Optional<Class<?>> clazz = context.getTestClass();
      if (clazz.isPresent()) {
        Optional<A> ann = AnnotationUtils.findAnnotation(clazz.get(), annotation);
        if (ann.isPresent()) {
          return ann;
        }
      }

      Optional<ExtensionContext> p = context.getParent();
      if (!p.isPresent()) {
        return Optional.empty();
      }
      context = p.get();
    }
  }

  static DatabaseAdapter createAdapterResource(
      NessieDbAdapter adapterAnnotation,
      ExtensionContext context,
      ParameterContext parameterContext,
      Field field) {
    DatabaseAdapterFactory<
            DatabaseAdapter,
            DatabaseAdapterConfig,
            AdjustableDatabaseAdapterConfig,
            DatabaseConnectionProvider<?>>
        factory =
            findAnnotation(context, field, parameterContext, NessieDbAdapterName.class)
                .map(NessieDbAdapterName::value)
                .map(
                    DatabaseAdapterFactory
                        ::<DatabaseAdapter, DatabaseAdapterConfig, AdjustableDatabaseAdapterConfig,
                            DatabaseConnectionProvider<?>>loadFactoryByName)
                .orElseGet(() -> DatabaseAdapterFactory.loadFactory(x -> true));

    Function<AdjustableDatabaseAdapterConfig, DatabaseAdapterConfig> applyCustomConfig =
        extractCustomConfiguration(adapterAnnotation, context);

    DatabaseAdapterFactory.Builder<
            DatabaseAdapter,
            DatabaseAdapterConfig,
            AdjustableDatabaseAdapterConfig,
            DatabaseConnectionProvider<?>>
        builder = factory.newBuilder();

    if (adapterAnnotation.eventConsumer() != AdapterEventConsumer.class) {
      AdapterEventConsumer eventConsumer;
      try {
        Constructor<? extends AdapterEventConsumer> ctor =
            adapterAnnotation.eventConsumer().getDeclaredConstructor();
        makeAccessible(ctor);
        eventConsumer = ctor.newInstance();
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(
                "Failed to instantiate AdapterEventConsumer of type %s",
                adapterAnnotation.eventConsumer()),
            e);
      }
      builder.withEventConsumer(eventConsumer);
    }

    builder
        .configure(
            c ->
                SystemPropertiesConfigurer.configureAdapterFromProperties(
                    c,
                    property -> {
                      List<NessieDbAdapterConfigItem> configs = new ArrayList<>();
                      if (parameterContext != null) {
                        configs.addAll(
                            parameterContext.findRepeatableAnnotations(
                                NessieDbAdapterConfigItem.class));
                      }
                      Consumer<AnnotatedElement> collector =
                          m ->
                              configs.addAll(
                                  findRepeatableAnnotations(m, NessieDbAdapterConfigItem.class));
                      if (field != null) {
                        collector.accept(field);
                      }
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
                          .filter(n -> (CONFIG_NAME_PREFIX + n.name()).equals(property))
                          .findFirst()
                          .map(NessieDbAdapterConfigItem::value)
                          .orElse(null);
                    }))
        .configure(applyCustomConfig)
        .withConnector(getConnectionProvider(context));

    return builder.build();
  }

  private static Function<AdjustableDatabaseAdapterConfig, DatabaseAdapterConfig>
      extractCustomConfiguration(NessieDbAdapter adapterAnnotation, ExtensionContext context) {
    Function<AdjustableDatabaseAdapterConfig, DatabaseAdapterConfig> applyCustomConfig = c -> c;
    if (!adapterAnnotation.configMethod().isEmpty()) {
      Method configMethod =
          findMethod(
                  context.getRequiredTestClass(),
                  adapterAnnotation.configMethod(),
                  AdjustableDatabaseAdapterConfig.class)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              "%s.configMethod='%s' does not exist in %s",
                              NessieDbAdapter.class.getSimpleName(),
                              adapterAnnotation.configMethod(),
                              context.getRequiredTestClass().getName())));

      makeAccessible(configMethod);

      if (!Modifier.isStatic(configMethod.getModifiers())
          || Modifier.isPrivate(configMethod.getModifiers())
          || !DatabaseAdapterConfig.class.isAssignableFrom(configMethod.getReturnType())) {
        throw new IllegalArgumentException(
            String.format(
                "%s.configMethod='%s' must have the signature 'static %s %s(%s)' in %s",
                NessieDbAdapter.class.getSimpleName(),
                adapterAnnotation.configMethod(),
                DatabaseAdapterConfig.class.getSimpleName(),
                adapterAnnotation.configMethod(),
                AdjustableDatabaseAdapterConfig.class.getSimpleName(),
                context.getRequiredTestClass().getName()));
      }
      applyCustomConfig =
          c -> {
            try {
              return (DatabaseAdapterConfig) configMethod.invoke(null, c);
            } catch (InvocationTargetException | IllegalAccessException e) {
              throw new RuntimeException(e);
            }
          };
    }
    return applyCustomConfig;
  }

  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  private static <CONNECTOR extends DatabaseConnectionProvider<?>> CONNECTOR getConnectionProvider(
      ExtensionContext context) {
    TestConnectionProviderSource<?> connectionProvider =
        context.getStore(NAMESPACE).get(KEY_STATICS, ClassDbAdapters.class).connectionProvider;
    if (connectionProvider == null) {
      throw new NullPointerException("connectionProvider not configured");
    }
    return (CONNECTOR) connectionProvider.getConnectionProvider();
  }

  private void assertValidFieldCandidate(Field field) {
    if (!field.getType().isAssignableFrom(DatabaseAdapter.class)
        && !field.getType().isAssignableFrom(VersionStore.class)) {
      throw new ExtensionConfigurationException(
          "Can only resolve fields of type "
              + VersionStore.class.getName()
              + " or "
              + DatabaseAdapter.class.getName()
              + " but was: "
              + field.getType().getName());
    }
    if (isPrivate(field)) {
      throw new ExtensionConfigurationException(
          String.format("field [%s] must not be private.", field));
    }
  }
}
