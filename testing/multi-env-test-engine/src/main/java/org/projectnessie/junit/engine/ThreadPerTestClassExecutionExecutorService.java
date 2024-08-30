/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.junit.engine;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.hierarchical.HierarchicalTestExecutorService;

/**
 * Implements a JUnit test executor that provides thread-per-test-class behavior.
 *
 * <p>"Thread-per-test-class behavior" is needed to prevent the class/class-loader leak via {@link
 * ThreadLocal}s as described in <a
 * href="https://github.com/projectnessie/nessie/issues/9441">#9441</a>.
 */
public class ThreadPerTestClassExecutionExecutorService implements HierarchicalTestExecutorService {

  private static final Class<?> CLASS_NODE_TEST_TASK;
  private static final Field FIELD_TEST_DESCRIPTOR;

  static {
    try {
      CLASS_NODE_TEST_TASK =
          Class.forName("org.junit.platform.engine.support.hierarchical.NodeTestTask");
      FIELD_TEST_DESCRIPTOR = CLASS_NODE_TEST_TASK.getDeclaredField("testDescriptor");
      FIELD_TEST_DESCRIPTOR.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(
          "ThreadPerExecutionExecutorService is probably not compatible with the current JUnit version",
          e);
    }
  }

  protected TestDescriptor getTestDescriptor(TestTask testTask) {
    if (!CLASS_NODE_TEST_TASK.isAssignableFrom(testTask.getClass())) {
      throw new IllegalArgumentException(
          testTask.getClass().getName() + " is not of type " + CLASS_NODE_TEST_TASK.getName());
    }
    try {
      return (TestDescriptor) FIELD_TEST_DESCRIPTOR.get(testTask);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public ThreadPerTestClassExecutionExecutorService() {}

  @Override
  public Future<Void> submit(TestTask testTask) {
    executeTask(testTask);
    return completedFuture(null);
  }

  @Override
  public void invokeAll(List<? extends TestTask> tasks) {
    tasks.forEach(this::executeTask);
  }

  protected void executeTask(TestTask testTask) {
    TestDescriptor testDescriptor = getTestDescriptor(testTask);
    UniqueId.Segment lastSegment = testDescriptor.getUniqueId().getLastSegment();
    String type = lastSegment.getType();
    if ("class".equals(type)) {
      AtomicReference<Exception> failure = new AtomicReference<>();
      Thread threadPerClass =
          new Thread(
              () -> {
                try {
                  testTask.execute();
                } catch (Exception e) {
                  failure.set(e);
                }
              },
              "TEST THREAD FOR " + lastSegment.getValue());
      threadPerClass.setDaemon(true);
      threadPerClass.start();
      try {
        threadPerClass.join();
      } catch (InterruptedException e) {
        // delegate a thread-interrupt
        threadPerClass.interrupt();
      }
      Exception ex = failure.get();
      if (ex instanceof RuntimeException) {
        throw (RuntimeException) ex;
      } else if (ex != null) {
        throw new RuntimeException(ex);
      }
    } else {
      testTask.execute();
    }
  }

  @Override
  public void close() {
    // nothing to do
  }
}
