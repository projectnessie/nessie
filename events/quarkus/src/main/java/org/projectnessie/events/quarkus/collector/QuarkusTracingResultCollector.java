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
package org.projectnessie.events.quarkus.collector;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.util.function.Consumer;
import org.projectnessie.versioned.Result;

public class QuarkusTracingResultCollector implements Consumer<Result> {

  public static final String NESSIE_RESULTS_SPAN_NAME = "nessie.results";
  public static final AttributeKey<String> NESSIE_RESULT_TYPE_ATTRIBUTE_KEY =
      AttributeKey.stringKey("nessie.results.type");
  public static final AttributeKey<String> ENDUSER_ID = AttributeKey.stringKey("enduser.id");
  public static final AttributeKey<String> PEER_SERVICE = AttributeKey.stringKey("peer.service");
  public static final AttributeKey<String> SERVICE_NAME = AttributeKey.stringKey("service.name");

  private final Consumer<Result> delegate;
  private final String userName;
  private final Tracer tracer;

  public QuarkusTracingResultCollector(Consumer<Result> delegate, String userName, Tracer tracer) {
    this.delegate = delegate;
    this.userName = userName;
    this.tracer = tracer;
  }

  @Override
  public void accept(Result result) {
    Span span =
        tracer
            .spanBuilder(NESSIE_RESULTS_SPAN_NAME)
            .setAttribute(NESSIE_RESULT_TYPE_ATTRIBUTE_KEY, result.getResultType().name())
            .setAttribute(ENDUSER_ID, userName)
            .setAttribute(SERVICE_NAME, "Nessie")
            .setAttribute(PEER_SERVICE, "Nessie")
            .startSpan();
    try (Scope ignored = span.makeCurrent()) {
      delegate.accept(result);
    } finally {
      span.end();
    }
  }
}
