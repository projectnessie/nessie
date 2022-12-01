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
package org.projectnessie.client.ext;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.UniqueId;
import org.projectnessie.junit.engine.MultiEnvTestExtension;

/**
 * Runs the related suite of tests for several Nessie API versions as specified by the {@link
 * NessieApiVersions} annotation.
 */
public class MultiVersionApiTest implements MultiEnvTestExtension, ExecutionCondition {
  public static final String API_VERSION_SEGMENT_TYPE = "nessie-api";

  // API version to be used for tests not annotated with `@ForNessieApiVersions`
  private static final NessieApiVersion DEFAULT_API_VERSION = NessieApiVersion.V2;

  @Override
  public String segmentType() {
    return API_VERSION_SEGMENT_TYPE;
  }

  @Override
  public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
    return Arrays.stream(NessieApiVersion.values()).map(Enum::name).collect(Collectors.toList());
  }

  @Override
  public boolean accepts(Class<?> testClass) {
    return AnnotationUtils.findAnnotation(testClass, NessieApiVersions.class).isPresent();
  }

  static NessieApiVersion apiVersion(ExtensionContext context) {
    return UniqueId.parse(context.getUniqueId()).getSegments().stream()
        .filter(s -> API_VERSION_SEGMENT_TYPE.equals(s.getType()))
        .map(UniqueId.Segment::getValue)
        .findFirst()
        .map(NessieApiVersion::valueOf)
        .orElse(DEFAULT_API_VERSION);
  }

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
    NessieApiVersion apiVersion = apiVersion(context);
    boolean matches =
        Arrays.asList(
                context
                    .getTestMethod()
                    .flatMap(m -> AnnotationUtils.findAnnotation(m, NessieApiVersions.class))
                    .map(NessieApiVersions::versions)
                    .orElseGet(
                        () ->
                            AnnotationUtils.findAnnotation(
                                    context.getRequiredTestClass(), NessieApiVersions.class)
                                .map(NessieApiVersions::versions)
                                .orElseGet(() -> new NessieApiVersion[] {DEFAULT_API_VERSION})))
            .contains(apiVersion);
    return matches
        ? ConditionEvaluationResult.enabled(null)
        : ConditionEvaluationResult.disabled(
            String.format(
                "API version %s is not applicable to test context %s",
                apiVersion, context.getUniqueId()));
  }
}
