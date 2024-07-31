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

import static io.opentelemetry.semconv.ServiceAttributes.SERVICE_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.projectnessie.events.quarkus.collector.QuarkusTracingResultCollector.NESSIE_RESULTS_SPAN_NAME;
import static org.projectnessie.events.quarkus.collector.QuarkusTracingResultCollector.NESSIE_RESULT_TYPE_ATTRIBUTE_KEY;
import static org.projectnessie.versioned.ResultType.MERGE;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import java.util.function.Consumer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.versioned.Result;

@ExtendWith({SoftAssertionsExtension.class, MockitoExtension.class})
class TestQuarkusTracingResultCollector {
  @InjectSoftAssertions SoftAssertions soft;
  @Mock Consumer<Result> delegate;
  @Mock Result result;
  @Mock SpanProcessor processor;

  public static final AttributeKey<String> ENDUSER_ID = AttributeKey.stringKey("enduser.id");

  @Test
  void accept() {
    when(result.getResultType()).thenReturn(MERGE);
    when(processor.isStartRequired()).thenReturn(true);
    when(processor.isEndRequired()).thenReturn(true);
    @SuppressWarnings("resource")
    TracerProvider tracerProvider = SdkTracerProvider.builder().addSpanProcessor(processor).build();
    Tracer tracer = tracerProvider.get("test");
    QuarkusTracingResultCollector collector =
        new QuarkusTracingResultCollector(delegate, "alice", tracer);
    collector.accept(result);
    verify(delegate).accept(result);
    verify(processor).onStart(any(), any());
    ArgumentCaptor<ReadableSpan> spanEndCaptor = ArgumentCaptor.forClass(ReadableSpan.class);
    verify(processor).onEnd(spanEndCaptor.capture());
    ReadableSpan end = spanEndCaptor.getValue();
    soft.assertThat(end.getName()).isEqualTo(NESSIE_RESULTS_SPAN_NAME);
    soft.assertThat(end.getAttribute(NESSIE_RESULT_TYPE_ATTRIBUTE_KEY)).isEqualTo(MERGE.name());
    soft.assertThat(end.getAttribute(ENDUSER_ID)).isEqualTo("alice");
    soft.assertThat(end.getAttribute(SERVICE_NAME)).isEqualTo("Nessie");
    soft.assertThat(end.getSpanContext().isValid()).isTrue();
    verifyNoMoreInteractions(delegate, processor);
  }
}
