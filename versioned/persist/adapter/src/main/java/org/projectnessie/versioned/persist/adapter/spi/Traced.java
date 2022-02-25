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
package org.projectnessie.versioned.persist.adapter.spi;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import java.io.Closeable;

public final class Traced implements Closeable {

  private final Span span;
  private final Scope scope;

  public static Traced trace(String opName) {
    return new Traced(opName);
  }

  private Traced(String opName) {
    Tracer t = GlobalTracer.get();
    String spanName = "DatabaseAdapter." + opName;
    span =
        t.buildSpan(spanName)
            .asChildOf(t.activeSpan())
            .withTag(tagName("operation"), opName)
            .start();
    scope = t.activateSpan(span);
  }

  @Override
  public void close() {
    scope.close();
    span.finish();
  }

  public Traced tag(String tag, Number number) {
    span.setTag(tagName(tag), number);
    return this;
  }

  public Traced tag(String tag, String value) {
    span.setTag(tagName(tag), value);
    return this;
  }

  private static String tagName(String tag) {
    return "nessie.database-adapter." + tag;
  }
}
