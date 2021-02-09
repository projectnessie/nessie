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
/*
 * Copyright 2017-2019 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.projectnessie.versioned.dynamodb.metrics;

import java.util.HashMap;
import java.util.Map;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.log.Fields;
import io.opentracing.tag.Tags;
import software.amazon.awssdk.core.interceptor.Context.AfterExecution;
import software.amazon.awssdk.core.interceptor.Context.AfterMarshalling;
import software.amazon.awssdk.core.interceptor.Context.BeforeExecution;
import software.amazon.awssdk.core.interceptor.Context.FailedExecution;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;
import software.amazon.awssdk.http.SdkHttpRequest;

/**
 * Taken from https://github.com/opentracing-contrib/java-aws-sdk as its version of opentracing and Quarkus' collide.
 *
 * <p>has been modified to support Quarkus' version of opentracing
 */
public class TracingExecutionInterceptor implements ExecutionInterceptor {

  private static final String COMPONENT_NAME = "java-aws-sdk";
  private static final ExecutionAttribute<Span> SPAN_ATTRIBUTE = new ExecutionAttribute<>("ot-span");
  private final Tracer tracer;

  public TracingExecutionInterceptor(Tracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public void beforeExecution(BeforeExecution context, ExecutionAttributes executionAttributes) {
    final Span span = tracer.buildSpan(context.request().getClass().getSimpleName())
                            .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
                            .withTag(Tags.PEER_SERVICE.getKey(),
                                     executionAttributes.getAttribute(SdkExecutionAttribute.SERVICE_NAME))
                            .withTag(Tags.COMPONENT.getKey(), COMPONENT_NAME).start();

    executionAttributes.putAttribute(SPAN_ATTRIBUTE, span);
  }

  @Override
  public void afterMarshalling(final AfterMarshalling context,
                               final ExecutionAttributes executionAttributes) {
    final Span span = executionAttributes.getAttribute(SPAN_ATTRIBUTE);
    final SdkHttpRequest httpRequest = context.httpRequest();

    Tags.HTTP_METHOD.set(span, httpRequest.method().name());
    Tags.HTTP_URL.set(span, httpRequest.getUri().toString());
    Tags.PEER_HOSTNAME.set(span, httpRequest.host());
    if (httpRequest.port() > 0) {
      Tags.PEER_PORT.set(span, httpRequest.port());
    }
  }

  @Override
  public void afterExecution(final AfterExecution context,
                             final ExecutionAttributes executionAttributes) {
    final Span span = executionAttributes.getAttribute(SPAN_ATTRIBUTE);
    if (span == null) {
      return;
    }

    executionAttributes.putAttribute(SPAN_ATTRIBUTE, null);
    span.setTag(Tags.HTTP_STATUS.getKey(), context.httpResponse().statusCode());
    span.finish();
  }

  @Override
  public void onExecutionFailure(final FailedExecution context,
                                 final ExecutionAttributes executionAttributes) {
    final Span span = executionAttributes.getAttribute(SPAN_ATTRIBUTE);
    if (span == null) {
      return;
    }

    executionAttributes.putAttribute(SPAN_ATTRIBUTE, null);
    Tags.ERROR.set(span, Boolean.TRUE);
    span.log(errorLogs(context.exception()));
    span.finish();
  }

  private static Map<String, Object> errorLogs(final Throwable ex) {
    Map<String, Object> errorLogs = new HashMap<>(2);
    errorLogs.put(Fields.EVENT, Tags.ERROR.getKey());
    errorLogs.put(Fields.ERROR_OBJECT, ex);
    return errorLogs;
  }
}
