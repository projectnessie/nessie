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

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;
import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;
import static org.projectnessie.tools.compatibility.api.Version.parseVersion;
import static org.projectnessie.tools.compatibility.internal.AnnotatedFields.populateAnnotatedFields;

import java.lang.reflect.AnnotatedElement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.UniqueId.Segment;
import org.projectnessie.junit.engine.MultiEnvTestExtension;
import org.projectnessie.tools.compatibility.api.NessieVersion;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.api.VersionCondition;

/**
 * Base class for all Nessie multi-version tests.
 *
 * <p>Implements extension to handle {@link VersionCondition}.
 */
abstract class AbstractMultiVersionExtension
    implements BeforeAllCallback, BeforeEachCallback, ExecutionCondition, MultiEnvTestExtension {

  private static final String NESSIE_VERSION_SEGMENT_TYPE = "nessie-version";

  private static final ConditionEvaluationResult PASS = enabled(null);

  @Override
  public String segmentType() {
    return NESSIE_VERSION_SEGMENT_TYPE;
  }

  @Override
  public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
    try {
      return VersionsToExercise.versionsForEngine(configuration).stream()
          .map(Objects::toString)
          .collect(Collectors.toList());
    } catch (IllegalStateException e) {
      // No versions to test found - return early
      return Collections.emptyList();
    }
  }

  private void checkExtensions(Class<?> testClass) {
    long count = multiVersionExtensionsForTestClass(Stream.of(testClass)).count();
    if (count > 1) {
      // Sanity check, it's illegal to have multiple nessie-compatibility extensions on one test
      // class
      throw new IllegalStateException(
          String.format(
              "Test class %s contains more than one Nessie multi-version extension",
              testClass.getName()));
    }
  }

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    Optional<Version> version = nessieVersionFromContext(context);
    if (version.isEmpty()) {
      if (isNessieMultiVersionTest(context)) {
        return disabled("(not running via multi-nessie-version engine)");
      }

      return enabled("(not running via multi-nessie-versions engine)");
    }

    ConditionEvaluationResult classResult =
        context.getTestClass().map(c -> conditionCheck(version.get(), c)).orElse(PASS);
    if (classResult.isDisabled()) {
      return classResult;
    }

    ConditionEvaluationResult methodResult =
        context.getTestMethod().map(m -> conditionCheck(version.get(), m)).orElse(PASS);
    if (methodResult.isDisabled()) {
      return methodResult;
    }

    return PASS;
  }

  static boolean isNessieMultiVersionTest(ExtensionContext context) {
    return multiVersionExtensionsForTestClass(Stream.of(context.getTestClass().orElse(null)))
        .findAny()
        .isPresent();
  }

  /**
   * Retrieve a stream of JUnit extension classes that are assignable to {@link
   * AbstractMultiVersionExtension}.
   */
  @SuppressWarnings("unchecked")
  static Stream<Class<? extends AbstractMultiVersionExtension>> multiVersionExtensionsForTestClass(
      Stream<Class<?>> classStream) {
    return classStream
        .filter(Objects::nonNull)
        .flatMap(c -> AnnotationUtils.findRepeatableAnnotations(c, ExtendWith.class).stream())
        .flatMap(e -> Arrays.stream(e.value()))
        .filter(AbstractMultiVersionExtension.class::isAssignableFrom)
        .map(c -> (Class<? extends AbstractMultiVersionExtension>) c);
  }

  static ConditionEvaluationResult conditionCheck(Version version, AnnotatedElement annotated) {
    Optional<VersionCondition> cond = findAnnotation(annotated, VersionCondition.class);
    if (cond.isPresent()) {
      VersionCondition versionCondition = cond.get();
      if (!versionCondition.minVersion().isEmpty()
          && parseVersion(versionCondition.minVersion()).isGreaterThan(version)) {
        return disabled(
            String.format(
                "%s requires minimum Nessie version '%s', but current Nessie version is '%s'",
                annotated, versionCondition.minVersion(), version));
      }
      if (!versionCondition.maxVersion().isEmpty()
          && parseVersion(versionCondition.maxVersion()).isLessThan(version)) {
        return disabled(
            String.format(
                "%s requires maximum Nessie version '%s', but current Nessie version is '%s'",
                annotated, versionCondition.maxVersion(), version));
      }
    }
    return PASS;
  }

  Version populateNessieVersionAnnotatedFields(ExtensionContext context, Object instance) {
    checkExtensions(context.getRequiredTestClass());

    Optional<Version> nessieVersion = nessieVersionFromContext(context);
    if (nessieVersion.isEmpty()) {
      return null;
    }
    Version version = nessieVersion.get();

    populateAnnotatedFields(context, instance, NessieVersion.class, a -> true, f -> version);

    return version;
  }

  static Optional<Version> nessieVersionFromContext(ExtensionContext context) {
    return nessieVersionFromContext(UniqueId.parse(context.getUniqueId()));
  }

  static Optional<Version> nessieVersionFromContext(UniqueId uniqueId) {
    return uniqueId.getSegments().stream()
        .filter(s -> NESSIE_VERSION_SEGMENT_TYPE.equals(s.getType()))
        .map(Segment::getValue)
        .map(Version::parseVersion)
        .findFirst();
  }
}
