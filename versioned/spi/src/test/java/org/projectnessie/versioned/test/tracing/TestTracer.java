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
package org.projectnessie.versioned.test.tracing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tag;
import io.opentracing.util.GlobalTracer;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tracer implementation used in tests of {@code TracingVersionStore} and {@code TracingStore} to
 * collect traced information.
 */
public class TestTracer implements Tracer {

  private static Tracer tracerPerTest;

  private TestSpan activeSpan;
  private boolean closed;
  private boolean parentSet;
  private String opName;

  /** Registers a "mocked" {@link Tracer} via {@link GlobalTracer}. */
  public static void registerGlobal() {
    // GlobalTracer doesn't allow resetting or changing an already registered "global tracer",
    // so this code delegates to some Tracer-per-test
    assertFalse(GlobalTracer.isRegistered());
    GlobalTracer.registerIfAbsent(
        (Tracer)
            Proxy.newProxyInstance(
                Thread.currentThread().getContextClassLoader(),
                new Class[] {Tracer.class},
                (proxy, method, args) -> method.invoke(tracerPerTest, args)));
  }

  public void registerForCurrentTest() {
    tracerPerTest = this;
  }

  public TestSpan getActiveSpan() {
    return activeSpan;
  }

  public boolean isClosed() {
    return closed;
  }

  public boolean isParentSet() {
    return parentSet;
  }

  public String getOpName() {
    return opName;
  }

  @Override
  public ScopeManager scopeManager() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Span activeSpan() {
    return activeSpan;
  }

  @Override
  public Scope activateSpan(Span span) {
    activeSpan = (TestSpan) span;
    return () -> {
      assertFalse(closed);
      closed = true;
    };
  }

  @Override
  public SpanBuilder buildSpan(String operationName) {
    opName = operationName;
    return new SpanBuilder() {

      final Map<String, Object> tags = new HashMap<>();

      @Override
      public SpanBuilder withTag(String key, String value) {
        tags.put(key, value);
        return this;
      }

      @Override
      public SpanBuilder withTag(String key, boolean value) {
        tags.put(key, value);
        return this;
      }

      @Override
      public SpanBuilder withTag(String key, Number value) {
        tags.put(key, value);
        return this;
      }

      @Override
      public <T> SpanBuilder withTag(Tag<T> tag, T t) {
        throw new UnsupportedOperationException();
      }

      @Override
      public SpanBuilder asChildOf(SpanContext parent) {
        throw new UnsupportedOperationException();
      }

      @Override
      public SpanBuilder asChildOf(Span parent) {
        assertFalse(parentSet);
        assertNull(parent);
        parentSet = true;
        return this;
      }

      @Override
      public SpanBuilder addReference(String referenceType, SpanContext referencedContext) {
        throw new UnsupportedOperationException();
      }

      @Override
      public SpanBuilder ignoreActiveSpan() {
        throw new UnsupportedOperationException();
      }

      @Override
      public SpanBuilder withStartTimestamp(long microseconds) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Span start() {
        activeSpan = new TestSpan(tags);
        return activeSpan;
      }
    };
  }

  @Override
  public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <C> SpanContext extract(Format<C> format, C carrier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {}

  public class TestSpan implements Span {

    private final Map<String, Object> tags = new HashMap<>();
    private final List<Map<String, ?>> logs = new ArrayList<>();

    TestSpan(Map<String, Object> tags) {
      this.tags.putAll(tags);
    }

    public Map<String, Object> getTags() {
      return tags;
    }

    public List<Map<String, ?>> getLogs() {
      return logs;
    }

    @Override
    public SpanContext context() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Span setTag(String key, String value) {
      tags.put(key, value);
      return this;
    }

    @Override
    public Span setTag(String key, boolean value) {
      tags.put(key, value);
      return this;
    }

    @Override
    public Span setTag(String key, Number value) {
      tags.put(key, value);
      return this;
    }

    @Override
    public <T> Span setTag(Tag<T> tag, T t) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Span log(Map<String, ?> fields) {
      logs.add(fields);
      return this;
    }

    @Override
    public Span log(long timestampMicroseconds, Map<String, ?> fields) {
      return log(fields);
    }

    @Override
    public Span log(String event) {
      return log(Collections.singletonMap("event", event));
    }

    @Override
    public Span log(long timestampMicroseconds, String event) {
      return log(event);
    }

    @Override
    public Span setBaggageItem(String key, String value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getBaggageItem(String key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Span setOperationName(String operationName) {
      opName = operationName;
      return this;
    }

    @Override
    public void finish() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void finish(long finishMicros) {
      throw new UnsupportedOperationException();
    }
  }
}
