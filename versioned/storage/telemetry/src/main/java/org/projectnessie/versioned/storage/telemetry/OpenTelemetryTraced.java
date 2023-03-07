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
package org.projectnessie.versioned.storage.telemetry;

import static io.opentelemetry.api.trace.StatusCode.ERROR;
import static org.projectnessie.versioned.storage.telemetry.Traced.tagName;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

final class OpenTelemetryTraced implements Traced {

  private final Span span;
  private final Scope scope;

  @SuppressWarnings("MustBeClosedChecker")
  OpenTelemetryTraced(Tracer tracer, String spanName) {
    span = tracer.spanBuilder(spanName).startSpan();
    scope = span.makeCurrent();
  }

  @Override
  public void close() {
    try {
      scope.close();
    } finally {
      span.end();
    }
  }

  @Override
  public void event(String eventName) {
    span.addEvent(eventName);
  }

  @Override
  public Traced attribute(String tag, String value) {
    span.setAttribute(tagName(tag), value);
    return this;
  }

  @Override
  public Traced attribute(String tag, boolean value) {
    span.setAttribute(tagName(tag), value);
    return this;
  }

  @Override
  public Traced attribute(String tag, int value) {
    span.setAttribute(tagName(tag), value);
    return this;
  }

  @Override
  public Traced attribute(String tag, long value) {
    span.setAttribute(tagName(tag), value);
    return this;
  }

  @Override
  public RuntimeException unhandledError(RuntimeException e) {
    span.setStatus(ERROR).recordException(e);
    return e;
  }
}
