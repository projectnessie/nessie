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
package org.projectnessie.versioned.persist.tests;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;
import static org.junit.platform.commons.util.ReflectionUtils.isPrivate;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.function.Function;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.platform.commons.JUnitException;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterFactory;

/**
 * JUnit extension to supply {@link DatabaseAdapter} to test classes.
 *
 * <p>The test class must be annotated with {@link
 * org.junit.jupiter.api.extension.ExtendWith @ExtendWith} using a concrete implementation of this
 * extension.
 *
 * <p>{@link DatabaseAdapter} must be injected as a {@code static} field and annotated with {@link
 * Adapter}.
 */
public abstract class DatabaseAdapterExtension implements BeforeAllCallback, BeforeEachCallback {
  private static final Namespace NAMESPACE = Namespace.create(DatabaseAdapterExtension.class);
  private static final String KEY = "database-adapter";

  /**
   * Used to provide this extension an instance of {@link DatabaseAdapter}.
   *
   * @param context JUnit extension context, if needed
   * @param testConfigurer {@link TestConfigurer}, must be passed though to the static {@code
   *     createAdapter()} methods.
   */
  protected abstract DatabaseAdapter createAdapter(
      ExtensionContext context, TestConfigurer testConfigurer);

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Adapter {
    /**
     * Allows per-test-class configuration for of database-adapter configurations using a {@link
     * TestConfigurer}.
     */
    Class<? extends TestConfigurer> configurer() default NoopConfigurer.class;
  }

  /** Used to configure database-adapters per test-class. */
  public interface TestConfigurer {
    <C extends DatabaseAdapterConfig> DatabaseAdapterConfig configure(C config);
  }

  private static class NoopConfigurer implements TestConfigurer {
    @Override
    public <C extends DatabaseAdapterConfig> DatabaseAdapterConfig configure(C config) {
      return config;
    }
  }

  protected static <C extends DatabaseAdapterConfig> DatabaseAdapter createAdapter(
      String adapterName,
      TestConfigurer testConfigurer,
      Function<C, DatabaseAdapterConfig> configurer) {
    return createAdapter(
        testConfigurer,
        DatabaseAdapterFactory.<C>loadFactoryByName(adapterName).newBuilder(),
        configurer);
  }

  protected static <C extends DatabaseAdapterConfig> DatabaseAdapter createAdapter(
      TestConfigurer testConfigurer,
      DatabaseAdapterFactory.Builder<C> adapterBuilder,
      Function<C, DatabaseAdapterConfig> configurer) {
    adapterBuilder =
        adapterBuilder
            .configure(SystemPropertiesConfigurer::configureFromSystemProperties)
            // default to a quite small max-size for the CommitLogEntry.keyList + KeyListEntity
            // This is necessary for AbstractManyKeys to work properly!!
            .configure(c -> c.withMaxKeyListSize(2048))
            .configure(configurer)
            .configure(testConfigurer::configure);

    return adapterBuilder.build();
  }

  static class AdapterResource implements CloseableResource {
    private final DatabaseAdapter databaseAdapter;

    AdapterResource(DatabaseAdapter databaseAdapter) {
      this.databaseAdapter = databaseAdapter;
    }

    @Override
    public void close() throws Throwable {
      databaseAdapter.close();
    }
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    Class<?> testClass = context.getRequiredTestClass();
    findAnnotatedFields(testClass, Adapter.class, ReflectionUtils::isStatic)
        .forEach(
            field -> {
              assertValidStaticFieldCandidate(field);
              Adapter annotation =
                  findAnnotation(field, Adapter.class).orElseThrow(IllegalStateException::new);
              try {
                if (context.getStore(NAMESPACE).get(KEY) != null) {
                  throw new JUnitException(
                      "Only one instance of '@DatabaseAdapterExtension.Adapter static DatabaseAdapter databaseAdapter;'"
                          + "field allowed per test class");
                }

                TestConfigurer configurer =
                    makeAccessible(annotation.configurer().getDeclaredConstructor()).newInstance();
                DatabaseAdapter adapter =
                    context
                        .getStore(NAMESPACE)
                        .getOrComputeIfAbsent(
                            KEY,
                            key -> new AdapterResource(createAdapter(context, configurer)),
                            AdapterResource.class)
                        .databaseAdapter;

                makeAccessible(field).set(null, adapter);
              } catch (Throwable t) {
                ExceptionUtils.throwAsUncheckedException(t);
              }
            });
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    AdapterResource adapter = context.getStore(NAMESPACE).get(KEY, AdapterResource.class);

    if (adapter == null) {
      throw new JUnitException(
          "Test class does not declare mandatory '@DatabaseAdapterExtension.Adapter static DatabaseAdapter databaseAdapter;' field");
    }

    adapter.databaseAdapter.reinitializeRepo("main");
  }

  private void assertValidStaticFieldCandidate(Field field) {
    if (!field.getType().isAssignableFrom(DatabaseAdapter.class)) {
      throw new ExtensionConfigurationException(
          "Can only resolve @DatabaseAdapterExtension.Adapter field of type "
              + DatabaseAdapter.class.getName()
              + " but was: "
              + field.getType().getName());
    }
    assertNotPrivate(field);
  }

  private void assertNotPrivate(Field field) {
    if (isPrivate(field)) {
      throw new ExtensionConfigurationException(
          "@DatabaseAdapterExtension.Adapter field [" + field + "] must not be private.");
    }
  }
}
