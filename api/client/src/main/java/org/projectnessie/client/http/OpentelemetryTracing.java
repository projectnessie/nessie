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
package org.projectnessie.client.http;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import org.projectnessie.api.NessieVersion;

final class OpentelemetryTracing {
  static final AttributeKey<String> HTTP_URL = AttributeKey.stringKey("http.url");
  static final AttributeKey<String> HTTP_METHOD = AttributeKey.stringKey("http.method");
  static final AttributeKey<String> NESSIE_VERSION = AttributeKey.stringKey("nessie.version");
  static final AttributeKey<Long> HTTP_STATUS_CODE = AttributeKey.longKey("http.status_code");

  private OpentelemetryTracing() {}

  static void addTracing(HttpClient.Builder httpClient) {
    // It's safe to reference `GlobalTracer` here even without the required dependencies available
    // at runtime, as long as tracing is not enabled. I.e. as long as tracing is not enabled, this
    // method will not be called and the JVM won't try to load + initialize `GlobalTracer`.
    OpenTelemetry otel = GlobalOpenTelemetry.get();
    Tracer tracer = otel.getTracer("Nessie");
    if (tracer != null) {
      httpClient.addRequestFilter(
          context -> {
            Span span =
                tracer
                    .spanBuilder("Nessie-HTTP")
                    .setSpanKind(SpanKind.CLIENT)
                    .startSpan()
                    .setAttribute(HTTP_URL, context.getUri().toString())
                    .setAttribute(HTTP_METHOD, context.getMethod().name())
                    .setAttribute(NESSIE_VERSION, NessieVersion.NESSIE_VERSION);

            //noinspection DataFlowIssue
            W3CTraceContextPropagator.getInstance()
                .inject(Context.current().with(span), context, RequestContext::putHeader);

            context.addResponseCallback(
                (responseContext, exception) -> {
                  if (responseContext != null) {
                    span.setAttribute(HTTP_STATUS_CODE, responseContext.getStatus().getCode());
                  }
                  if (exception != null) {
                    span.setStatus(StatusCode.ERROR).recordException(exception);
                  } else {
                    span.setStatus(StatusCode.OK);
                  }
                  span.end();
                });
          });
    }
  }
}
