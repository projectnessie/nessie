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
package org.projectnessie.junit.engine;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.util.AnnotationUtils;

public final class MultiEnvAnnotationUtils {
  private MultiEnvAnnotationUtils() {}

  public static <A extends Annotation> Set<A> findNestedRepeatableAnnotationsOn(
      Class<?> classToSearch, Class<A> annotationType) {
    Set<A> annotations = new HashSet<>();
    // Find annotations following the class nesting chain
    for (Class<?> cl = classToSearch; cl != null; cl = cl.getDeclaringClass()) {
      annotations.addAll(AnnotationUtils.findRepeatableAnnotations(cl, annotationType));
    }
    return annotations;
  }

  public static Stream<Class<? extends MultiEnvTestExtension>> findNestedMultiEnvTestExtensionsOn(
      Class<?> classToSearch) {
    //noinspection unchecked
    return findNestedRepeatableAnnotationsOn(classToSearch, ExtendWith.class).stream()
        .flatMap(extendWithAnnotation -> Arrays.stream(extendWithAnnotation.value()))
        .filter(MultiEnvTestExtension.class::isAssignableFrom)
        .map(e -> (Class<? extends MultiEnvTestExtension>) e);
  }

  public static String segmentTypeOf(Class<? extends MultiEnvTestExtension> extensionClass) {
    MultiEnvSegmentType segmentTypeAnnotation =
        extensionClass.getAnnotation(MultiEnvSegmentType.class);

    if (segmentTypeAnnotation == null) {
      throw new IllegalStateException(
          String.format(
              "%s is missing a MultiEnvSegmentType annotation.", extensionClass.getName()));
    }

    return segmentTypeAnnotation.value();
  }

  public static String segmentTypeOf(MultiEnvTestExtension extension) {
    return segmentTypeOf(extension.getClass());
  }
}
