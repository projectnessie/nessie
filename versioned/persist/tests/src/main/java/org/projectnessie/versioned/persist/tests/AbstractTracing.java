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
package org.projectnessie.versioned.persist.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Strings;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockSpan.MockContext;
import io.opentracing.mock.MockTracer;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.tests.extension.NessieDbTracer;
import org.projectnessie.versioned.tests.AbstractNestedVersionStore;
import org.projectnessie.versioned.testworker.BaseContent;
import org.projectnessie.versioned.testworker.CommitMessage;

public abstract class AbstractTracing extends AbstractNestedVersionStore {
  @NessieDbTracer MockTracer tracer;

  protected AbstractTracing(VersionStore<BaseContent, CommitMessage, BaseContent.Type> store) {
    super(store);
  }

  @Test
  void createBranch() throws Exception {
    store().create(BranchName.of("traceCreateBranch"), Optional.empty());
    assertThat(tracer.activeSpan()).isNull();
    List<MockSpanHierarchy> hierarchy = spanHierarchy();
    assertThat(hierarchy).hasSize(1);
    MockSpanHierarchy root = hierarchy.get(0);
    /*
    11 (39000µs)      VersionStore.create  {nessie.version-store.operation=Create, nessie.version-store.target-hash=Optional.empty, nessie.version-store.ref=BranchName{name=traceCreateBranch}}
    12 (39000µs)          DatabaseAdapter.create {nessie.database-adapter.operation=create, nessie.database-adapter.hash=2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d, nessie.database-adapter.ref=traceCreateBranch}
    13 (36000µs)              DatabaseAdapter.try-loop.createRef {nessie.database-adapter.operation=try-loop.createRef, nessie.database-adapter.try-loop.attempt=0, nessie.database-adapter.try-loop.retries=0}
    14 (    0µs)                  DatabaseAdapter.fetchGlobalPointer {nessie.database-adapter.operation=fetchGlobalPointer}
    18 (    0µs)                  DatabaseAdapter.writeRefLog {nessie.database-adapter.operation=writeRefLog}
    19 (    0µs)                  DatabaseAdapter.globalPointerCas {nessie.database-adapter.operation=globalPointerCas}
    */
    assertThat(root.asOperationHierarchy(null))
        .satisfiesAnyOf(
            // 1st "satisfies()" - for non-transactional database adapters
            h ->
                assertThat(h)
                    .isEqualTo(
                        OperationHierarchy.create()
                            .child(
                                "VersionStore.create",
                                vsCreate ->
                                    vsCreate.child(
                                        "DatabaseAdapter.create",
                                        dbCreate ->
                                            dbCreate.child(
                                                "DatabaseAdapter.try-loop.createRef",
                                                tryLoop ->
                                                    tryLoop
                                                        .add("DatabaseAdapter.fetchGlobalPointer")
                                                        .add("DatabaseAdapter.writeRefLog")
                                                        .add(
                                                            "DatabaseAdapter.globalPointerCas"))))),
            // 2nd "satisfies()" - for transactional database adapters
            h ->
                assertThat(h)
                    .isEqualTo(
                        OperationHierarchy.create()
                            .child(
                                "VersionStore.create",
                                vsCreate ->
                                    vsCreate.child(
                                        "DatabaseAdapter.create",
                                        dbCreate ->
                                            dbCreate.child(
                                                "DatabaseAdapter.try-loop.createRef",
                                                tryLoop ->
                                                    tryLoop
                                                        .add(
                                                            "DatabaseAdapter.checkNamedRefExistence")
                                                        .add("DatabaseAdapter.insertNewReference")
                                                        .add("DatabaseAdapter.getRefLogHead")
                                                        .add(
                                                            "DatabaseAdapter.updateRefLogHead"))))));
  }

  static class OperationHierarchy {
    private final OperationHierarchy parent;
    private final String name;
    private final List<OperationHierarchy> children = new ArrayList<>();

    OperationHierarchy(OperationHierarchy parent, String name) {
      this.parent = parent;
      this.name = name;
    }

    public static OperationHierarchy create() {
      return new OperationHierarchy(null, "<root>");
    }

    public OperationHierarchy child(String name, Consumer<OperationHierarchy> childConsumer) {
      OperationHierarchy ch = new OperationHierarchy(this, name);
      children.add(ch);
      childConsumer.accept(ch);
      return this;
    }

    public OperationHierarchy add(String name) {
      child(name, c -> {});
      return this;
    }

    public OperationHierarchy parent() {
      return parent;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof OperationHierarchy)) {
        return false;
      }
      OperationHierarchy that = (OperationHierarchy) o;
      return Objects.equals(name, that.name) && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, children);
    }

    @Override
    public String toString() {
      try (StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw)) {
        toString(pw, 0);
        return sw.toString();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @SuppressWarnings("InlineMeInliner") // asks to use Java11's String.repeat()
    void toString(PrintWriter w, int indent) {
      w.printf("%s %s%n", Strings.repeat("    ", indent), name);
      for (OperationHierarchy child : children) {
        child.toString(w, indent + 1);
      }
    }
  }

  static class MockSpanHierarchy {
    private MockSpanHierarchy parent;
    private MockSpan span;
    private final List<MockSpan> childSpans = new ArrayList<>();
    private final List<MockSpanHierarchy> children = new ArrayList<>();

    private MockSpanHierarchy() {}

    public OperationHierarchy asOperationHierarchy(OperationHierarchy parent) {
      OperationHierarchy op =
          new OperationHierarchy(parent, span != null ? span.operationName() : "<root>");
      for (MockSpanHierarchy child : children) {
        op.children.add(child.asOperationHierarchy(op));
      }
      return op;
    }

    @Override
    public String toString() {
      try (StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw)) {
        toString(pw, 0);
        return sw.toString();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @SuppressWarnings("InlineMeInliner") // asks to use Java11's String.repeat()
    void toString(PrintWriter w, int indent) {
      if (span != null) {
        w.printf(
            "%5d (%5dµs) %s %-20s %s%n",
            span.context().spanId(),
            span.finishMicros() - span.startMicros(),
            Strings.repeat("    ", indent),
            span.operationName(),
            span.tags());
      }
      for (MockSpanHierarchy child : children) {
        child.toString(w, indent + 1);
      }
    }
  }

  private List<MockSpanHierarchy> spanHierarchy() {
    Map<Long, MockSpanHierarchy> byId = new HashMap<>();

    for (MockSpan span : tracer.finishedSpans()) {
      byId.computeIfAbsent(span.context().spanId(), id -> new MockSpanHierarchy()).span = span;
      byId.computeIfAbsent(span.parentId(), id -> new MockSpanHierarchy()).childSpans.add(span);
    }

    for (MockSpanHierarchy span : byId.values()) {
      if (span.span != null) {
        span.parent = byId.get(span.span.parentId());
      }
      span.childSpans.stream()
          .map(MockSpan::context)
          .mapToLong(MockContext::spanId)
          .mapToObj(byId::get)
          .forEach(span.children::add);
    }

    return byId.values().stream().filter(h -> h.parent == null).collect(Collectors.toList());
  }
}
