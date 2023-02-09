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
package org.projectnessie.events.quarkus.fixtures;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.test.Mock;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Mock
@Singleton
@IfBuildProperty(name = "quarkus.otel.traces.sampler", stringValue = "always_on")
public class MockSpanExporter implements SpanExporter {

  private List<SpanData> spans = new ArrayList<>();

  public synchronized List<SpanData> getSpans() {
    return spans;
  }

  @Override
  public synchronized CompletableResultCode export(@Nullable Collection<SpanData> spans) {
    if (spans != null && !spans.isEmpty()) {
      this.spans.addAll(spans);
    }
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public CompletableResultCode flush() {
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public CompletableResultCode shutdown() {
    return CompletableResultCode.ofSuccess();
  }

  public synchronized void reset() {
    spans = new ArrayList<>();
  }
}
