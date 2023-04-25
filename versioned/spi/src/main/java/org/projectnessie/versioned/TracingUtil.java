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
package org.projectnessie.versioned;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import java.util.Collection;

/** Utility methods for tracing. */
public final class TracingUtil {

  private TracingUtil() {
    // empty
  }

  public static String safeToString(Object o) {
    return o != null ? o.toString() : "<null>";
  }

  public static int safeSize(Collection<?> collection) {
    return collection != null ? collection.size() : -1;
  }

  /**
   * Set {@link StatusCode#ERROR} using {@link Span#recordException(Throwable)}.
   *
   * @param span trace-span
   * @param e exception to trace
   * @return returns {@code e}
   */
  public static RuntimeException traceError(Span span, RuntimeException e) {
    span.setStatus(StatusCode.ERROR).recordException(e);
    return e;
  }
}
