/*
 * Copyright (C) 2023 Dremio
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

import static org.projectnessie.junit.engine.JUnitCompat.newDefaultJupiterConfiguration;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestInstantiationAwareExtension;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDirFactory;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.OutputDirectoryCreator;

public class MultiEnvDelegatingJupiterConfiguration implements JupiterConfiguration {

  final JupiterConfiguration delegate;

  public MultiEnvDelegatingJupiterConfiguration(EngineDiscoveryRequest discoveryRequest) {
    this.delegate = newDefaultJupiterConfiguration(discoveryRequest);
  }

  @Override
  public DisplayNameGenerator getDefaultDisplayNameGenerator() {
    return delegate.getDefaultDisplayNameGenerator();
  }

  @Override
  public Predicate<Class<? extends Extension>> getFilterForAutoDetectedExtensions() {
    return delegate.getFilterForAutoDetectedExtensions();
  }

  @Override
  public Optional<String> getRawConfigurationParameter(String key) {
    return delegate.getRawConfigurationParameter(key);
  }

  @Override
  public <T> Optional<T> getRawConfigurationParameter(String key, Function<String, T> transformer) {
    return delegate.getRawConfigurationParameter(key, transformer);
  }

  @Override
  public boolean isParallelExecutionEnabled() {
    return delegate.isParallelExecutionEnabled();
  }

  @Override
  public boolean isExtensionAutoDetectionEnabled() {
    return delegate.isExtensionAutoDetectionEnabled();
  }

  @Override
  public boolean isThreadDumpOnTimeoutEnabled() {
    return delegate.isThreadDumpOnTimeoutEnabled();
  }

  @Override
  public ExecutionMode getDefaultExecutionMode() {
    return delegate.getDefaultExecutionMode();
  }

  @Override
  public ExecutionMode getDefaultClassesExecutionMode() {
    return delegate.getDefaultClassesExecutionMode();
  }

  @Override
  public TestInstance.Lifecycle getDefaultTestInstanceLifecycle() {
    return delegate.getDefaultTestInstanceLifecycle();
  }

  @Override
  public Predicate<ExecutionCondition> getExecutionConditionFilter() {
    return delegate.getExecutionConditionFilter();
  }

  @Override
  public Optional<MethodOrderer> getDefaultTestMethodOrderer() {
    return delegate.getDefaultTestMethodOrderer();
  }

  @Override
  public Optional<ClassOrderer> getDefaultTestClassOrderer() {
    return delegate.getDefaultTestClassOrderer();
  }

  @Override
  public CleanupMode getDefaultTempDirCleanupMode() {
    return delegate.getDefaultTempDirCleanupMode();
  }

  @Override
  public Supplier<TempDirFactory> getDefaultTempDirFactorySupplier() {
    return delegate.getDefaultTempDirFactorySupplier();
  }

  @Override
  public TestInstantiationAwareExtension.ExtensionContextScope
      getDefaultTestInstantiationExtensionContextScope() {
    return delegate.getDefaultTestInstantiationExtensionContextScope();
  }

  @Override
  public OutputDirectoryCreator getOutputDirectoryCreator() {
    return delegate.getOutputDirectoryCreator();
  }

  @Override
  public boolean isClosingStoredAutoCloseablesEnabled() {
    return delegate.isClosingStoredAutoCloseablesEnabled();
  }
}
