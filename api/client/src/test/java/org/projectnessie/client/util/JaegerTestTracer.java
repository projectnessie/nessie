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
package org.projectnessie.client.util;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class JaegerTestTracer {
  private static final AtomicBoolean REGISTERED = new AtomicBoolean();

  public static final AttributeKey<String> SERVICE_NAME = AttributeKey.stringKey("service.name");

  public static void register() {
    if (!REGISTERED.compareAndSet(false, true)) {
      return;
    }

    OtlpGrpcSpanExporter jaegerOtlpExporter =
        OtlpGrpcSpanExporter.builder()
            .setEndpoint("http://localhost:4317")
            .setTimeout(30, TimeUnit.SECONDS)
            .build();

    Resource serviceNameResource =
        Resource.create(Attributes.of(SERVICE_NAME, "TestNessieHttpClient"));

    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(BatchSpanProcessor.builder(jaegerOtlpExporter).build())
            .setResource(Resource.getDefault().merge(serviceNameResource))
            .build();
    OpenTelemetry openTelemetry =
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();

    Runtime.getRuntime().addShutdownHook(new Thread(tracerProvider::close));

    GlobalOpenTelemetry.set(openTelemetry);
  }
}
